'use strict';

/**
 * Ray Trading Hub — database layer.
 *
 * Two responsibilities, both in one file because they are two halves of the
 * same concern:
 *
 *   1. Connection lifecycle
 *      - connect with retry/backoff
 *      - reflect state onto the event bus for the system status panel
 *      - never crash the process on a transient DB outage
 *      - graceful shutdown
 *
 *   2. Persistence dispatcher
 *      - subscribe to the Event Bus
 *      - write ONLY the events whitelisted in config.persistence.persistedEvents
 *      - route each event to the correct model
 *      - upsert by Deriv-side identity so replay is idempotent
 *
 * Everything else in the application talks to MongoDB through the models,
 * never through this file. This file exists so that the write policy is in
 * exactly one place, reviewable in one screen.
 */

const mongoose = require('mongoose');
const config = require('./config');
const bus = require('./eventBus');
const { EVENTS } = require('./eventBus');
const { Logger } = require('./logger');
const {
  Account,
  Instrument,
  Candle,
  Order,
  Contract,
  Trade,
  Transaction,
  AuditLog,
  enums,
} = require('./models');

const log = new Logger('database');

/* ------------------------------------------------------------------ */
/* constants                                                           */
/* ------------------------------------------------------------------ */

const CONNECT_INITIAL_DELAY_MS = 1_000;
const CONNECT_MAX_DELAY_MS = 30_000;
const CONNECT_MAX_ATTEMPTS = Infinity; // keep retrying; a DB outage is transient
const SHUTDOWN_TIMEOUT_MS = 8_000;

const STATES = Object.freeze({
  DISCONNECTED: 'disconnected',
  CONNECTING: 'connecting',
  CONNECTED: 'connected',
  RECONNECTING: 'reconnecting',
  ERROR: 'error',
  SHUTTING_DOWN: 'shutting_down',
});

/* ------------------------------------------------------------------ */
/* connection                                                          */
/* ------------------------------------------------------------------ */

class Database {
  constructor() {
    this._state = STATES.DISCONNECTED;
    this._attempt = 0;
    this._retryTimer = null;
    this._shuttingDown = false;
    this._listenersAttached = false;
  }

  get state() {
    return this._state;
  }

  /** Snapshot for the system status panel. Never contains the URI. */
  status() {
    const readyState = mongoose.connection.readyState;
    return {
      state: this._state,
      readyState,
      readyStateLabel: readyStateLabel(readyState),
      attempt: this._attempt,
      lastErrorAt: this._lastErrorAt || null,
      lastErrorCode: this._lastErrorCode || null,
    };
  }

  /* -------------------------------------------------------------- */

  /**
   * Connect. Resolves once the first connection succeeds. Never rejects —
   * a failure schedules a retry and returns. Callers should not gate boot
   * on the DB being available.
   */
  async connect() {
    if (this._shuttingDown) return;
    if (this._state === STATES.CONNECTED || this._state === STATES.CONNECTING) return;

    this._setState(STATES.CONNECTING);
    this._attachListeners();

    // Mongoose global config — set once, before any connect attempt.
    mongoose.set('strictQuery', true);
    mongoose.set('bufferCommands', true);  // queue writes until first connect
    mongoose.set('bufferTimeoutMS', 15_000);
    mongoose.set('sanitizeFilter', true);  // reject $-prefixed keys in filters by default
    mongoose.set('autoIndex', !config.isProduction); // never build indexes in prod at runtime

    const opts = {
      serverSelectionTimeoutMS: 10_000,
      socketTimeoutMS: 45_000,
      connectTimeoutMS: 10_000,
      heartbeatFrequencyMS: 10_000,
      maxPoolSize: 10,
      minPoolSize: 1,
      retryWrites: true,
      retryReads: true,
      // No appName collision in shared Atlas dashboards
      appName: `ray-trading-hub-${config.env}`,
    };

    try {
      await mongoose.connect(config.database.uri, opts);
      // The 'connected' listener fires the actual state transition and bus
      // emit, so we do not emit here — avoid double emits.
      this._attempt = 0;
    } catch (err) {
      this._lastErrorAt = new Date();
      this._lastErrorCode = err && err.code ? String(err.code) : 'CONNECT_FAILED';
      log.error('database.connect_failed', `MongoDB connection attempt failed: ${err.message}`, {
        code: this._lastErrorCode,
        attempt: this._attempt + 1,
      });
      this._setState(STATES.ERROR);
      this._scheduleRetry();
    }
  }

  /**
   * Disconnect and stop retrying. Idempotent.
   */
  async disconnect({ reason = 'shutdown' } = {}) {
    this._shuttingDown = true;
    this._setState(STATES.SHUTTING_DOWN);
    if (this._retryTimer) {
      clearTimeout(this._retryTimer);
      this._retryTimer = null;
    }
    try {
      await Promise.race([
        mongoose.connection.close(false),
        new Promise((_, rej) => setTimeout(() => rej(new Error('disconnect timeout')), SHUTDOWN_TIMEOUT_MS)),
      ]);
      log.info('database.disconnected', `MongoDB closed (${reason})`);
    } catch (err) {
      log.warn('database.disconnect_error', `MongoDB close failed during shutdown: ${err.message}`);
    } finally {
      this._setState(STATES.DISCONNECTED);
    }
  }

  /* -------------------------------------------------------------- */

  _attachListeners() {
    if (this._listenersAttached) return;
    this._listenersAttached = true;

    const conn = mongoose.connection;

    conn.on('connected', () => {
      this._attempt = 0;
      this._setState(STATES.CONNECTED);
      log.info('database.connected', 'MongoDB connected', {
        host: safeHost(conn.host),
        name: conn.name,
      });
    });

    conn.on('disconnected', () => {
      if (this._shuttingDown) return;
      this._setState(STATES.DISCONNECTED);
      log.warn('database.disconnected', 'MongoDB disconnected');
      this._scheduleRetry();
    });

    conn.on('reconnected', () => {
      this._attempt = 0;
      this._setState(STATES.CONNECTED);
      log.info('database.reconnected', 'MongoDB reconnected');
    });

    conn.on('error', (err) => {
      this._lastErrorAt = new Date();
      this._lastErrorCode = err && err.code ? String(err.code) : 'RUNTIME_ERROR';
      // Mongoose emits 'error' for many transient issues. Log, do not crash.
      log.error('database.error', `MongoDB error: ${err.message}`, { code: this._lastErrorCode });
    });

    conn.on('close', () => {
      if (this._shuttingDown) return;
      this._setState(STATES.DISCONNECTED);
    });
  }

  _scheduleRetry() {
    if (this._shuttingDown) return;
    if (this._retryTimer) return;
    this._attempt += 1;

    const delay = Math.min(
      CONNECT_INITIAL_DELAY_MS * Math.pow(1.7, this._attempt - 1),
      CONNECT_MAX_DELAY_MS
    ) + Math.floor(Math.random() * 300);

    log.warn('database.retry_scheduled', `Reconnecting to MongoDB in ${Math.round(delay / 1000)}s`, {
      attempt: this._attempt,
    });

    this._retryTimer = setTimeout(() => {
      this._retryTimer = null;
      this._setState(STATES.RECONNECTING);
      this.connect().catch(() => { /* connect() never rejects */ });
    }, delay);
  }

  _setState(next) {
    if (this._state === next) return;
    this._state = next;
    bus.emit(EVENTS.SYSTEM_STATUS, {
      database: { state: next, at: Date.now() },
    });
  }
}

function readyStateLabel(n) {
  return ({ 0: 'disconnected', 1: 'connected', 2: 'connecting', 3: 'disconnecting' })[n] || 'unknown';
}

function safeHost(host) {
  // Never log a connection string. Host only, and only the domain part.
  if (!host) return undefined;
  return String(host).replace(/:[0-9]+$/, '');
}

/* ------------------------------------------------------------------ */
/* persistence dispatcher                                              */
/* ------------------------------------------------------------------ */

/**
 * Subscribes to the Event Bus and writes whitelisted events to MongoDB.
 *
 * Whitelist source of truth: config.persistence.persistedEvents.
 * Unknown events are ignored. The dispatcher never throws upstream —
 * every write is caught and reported as a system.error so the bus stays
 * healthy even if MongoDB is down.
 */
class PersistenceDispatcher {
  constructor() {
    this._unsubscribers = [];
    this._started = false;
    this._writeCount = 0;
    this._errorCount = 0;
    this._pending = new Map(); // eventName -> in-flight promise (last one)
  }

  start() {
    if (this._started) return;
    this._started = true;

    const whitelist = new Set(config.persistence.persistedEvents);
    log.info('persistence.started', `Persistence dispatcher active`, {
      events: [...whitelist],
    });

    // Route table: event name -> handler. Only events in the whitelist get
    // handlers registered, so filtering is structural, not runtime.
    const routes = {
      [EVENTS.ORDER_CREATED]:  (d, m) => this._upsertOrder(d, m, 'DRAFT'),
      [EVENTS.ORDER_SUBMITTED]: (d, m) => this._upsertOrder(d, m, 'SUBMITTED'),
      [EVENTS.ORDER_ACCEPTED]:  (d, m) => this._upsertOrder(d, m, 'ACCEPTED'),
      [EVENTS.ORDER_REJECTED]:  (d, m) => this._upsertOrder(d, m, 'REJECTED'),
      [EVENTS.ORDER_FAILED]:    (d, m) => this._upsertOrder(d, m, 'FAILED'),

      [EVENTS.CONTRACT_CREATED]: (d) => this._upsertContract(d, 'OPEN'),
      [EVENTS.CONTRACT_OPENED]:  (d) => this._upsertContract(d, 'OPEN'),
      [EVENTS.CONTRACT_CLOSED]:  (d) => this._upsertContract(d, 'CLOSED'),

      [EVENTS.TRADE_COMPLETED]:  (d) => this._createTrade(d),

      [EVENTS.ACCOUNT_UPDATED]:         (d) => this._upsertAccount(d),
      [EVENTS.ACCOUNT_BALANCE_UPDATED]: (d) => this._updateBalance(d),
      [EVENTS.ACCOUNT_TRANSACTION]:     (d) => this._createTransaction(d),

      [EVENTS.DERIV_ERROR]:  (d) => this._auditFromDeriv('DERIV_CONNECTED', d),
      [EVENTS.SYSTEM_ERROR]: (d) => this._auditError(d),
    };

    for (const [eventName, handler] of Object.entries(routes)) {
      if (!whitelist.has(eventName)) continue;
      const unsub = bus.on(eventName, (data, meta) => {
        // Fire and forget; the dispatcher never blocks the emitter.
        this._run(eventName, handler, data, meta);
      }, { label: `persistence:${eventName}`, priority: -10 });
      this._unsubscribers.push(unsub);
    }
  }

  stop() {
    if (!this._started) return;
    for (const unsub of this._unsubscribers) {
      try { unsub(); } catch (_) {}
    }
    this._unsubscribers = [];
    this._started = false;
    log.info('persistence.stopped', 'Persistence dispatcher stopped');
  }

  stats() {
    return {
      started: this._started,
      writes: this._writeCount,
      errors: this._errorCount,
    };
  }

  /* -------------------------------------------------------------- */
  /* internal                                                       */
  /* -------------------------------------------------------------- */

  async _run(eventName, handler, data, meta) {
    // If Mongo is not connected, mongoose buffers writes up to
    // bufferTimeoutMS. That is preferable to dropping them. If the timeout
    // is hit, the promise rejects and we count the error.
    try {
      await handler(data, meta);
      this._writeCount += 1;
    } catch (err) {
      this._errorCount += 1;
      log.error('persistence.write_failed', `Failed to persist ${eventName}: ${err.message}`, {
        event: eventName,
        code: err && err.code ? err.code : undefined,
      });
      // Do NOT re-emit system.error for persistence failures caused by
      // system.error events themselves — avoids feedback loops.
      if (eventName !== EVENTS.SYSTEM_ERROR) {
        bus.emit(EVENTS.SYSTEM_ERROR, {
          source: 'persistence',
          event: eventName,
          message: `Persistence write failed: ${err.message}`,
          code: 'PERSISTENCE_WRITE_FAILED',
        });
      }
    }
  }

  /* --- order ------------------------------------------------------ */

  async _upsertOrder(data, meta, state) {
    if (!data || !data.clientOrderId) return;

    const now = new Date();
    const $set = {
      state,
      derivRequestId: data.derivRequestId || undefined,
      submittedAt: state === 'SUBMITTED' ? now : undefined,
      acceptedAt: state === 'ACCEPTED' ? now : undefined,
      closedAt: state === 'CLOSED' ? now : undefined,
      contractId: data.contractId || undefined,
    };
    // Strip undefined so we do not clobber existing values.
    for (const k of Object.keys($set)) if ($set[k] === undefined) delete $set[k];

    if (state === 'REJECTED' || state === 'FAILED') {
      $set.failure = {
        code: data.code || data.errorCode || 'DERIV_REJECTED',
        message: data.message || data.errorMessage || 'Deriv rejected the request.',
        requestId: data.derivRequestId || undefined,
        at: now,
      };
    }

    if (data.derivResponse !== undefined) $set.derivResponse = data.derivResponse;

    const update = {
      $set,
      $setOnInsert: {
        clientOrderId: data.clientOrderId,
        userId: data.userId || 'operator',
        symbol: data.symbol || 'UNKNOWN',
        contractType: data.contractType || 'UNKNOWN',
        stake: Number(data.stake) || 0,
        currency: (data.currency || 'USD').toUpperCase(),
        multiplier: data.multiplier,
        duration: data.duration,
        durationUnit: data.durationUnit,
        barrier: data.barrier,
        parameters: data.parameters || {},
        idempotencyKey: data.idempotencyKey || data.clientOrderId,
      },
      $push: {
        stateHistory: {
          state,
          at: now,
          note: meta && meta.note ? String(meta.note).slice(0, 200) : undefined,
        },
      },
    };

    await Order.updateOne({ clientOrderId: data.clientOrderId }, update, { upsert: true });
  }

  /* --- contract --------------------------------------------------- */

  async _upsertContract(data, forcedStatus) {
    if (!data || !data.contractId) return;

    const now = new Date();
    const $set = {
      clientOrderId: data.clientOrderId || undefined,
      accountId: data.accountId || undefined,
      symbol: data.symbol || undefined,
      contractType: data.contractType || undefined,
      currency: data.currency ? String(data.currency).toUpperCase() : undefined,
      underlying: data.underlying || undefined,
      buyPrice: pickNumber(data, ['buyPrice', 'buy_price']),
      stake: pickNumber(data, ['stake']),
      payout: pickNumber(data, ['payout']),
      entrySpot: pickNumber(data, ['entrySpot', 'entry_spot']),
      currentSpot: pickNumber(data, ['currentSpot', 'current_spot']),
      exitSpot: pickNumber(data, ['exitSpot', 'exit_spot']),
      entryTickTime: pickNumber(data, ['entryTickTime', 'entry_tick_time']),
      exitTickTime: pickNumber(data, ['exitTickTime', 'exit_tick_time']),
      profit: pickNumber(data, ['profit']),
      profitCurrency: data.profitCurrency || data.profit_currency || undefined,
      multiplier: pickNumber(data, ['multiplier']),
      dateStart: pickNumber(data, ['dateStart', 'date_start']),
      dateExpiry: pickNumber(data, ['dateExpiry', 'date_expiry']),
      duration: pickNumber(data, ['duration']),
      durationUnit: data.durationUnit || data.duration_unit || undefined,
      tickCount: pickNumber(data, ['tickCount', 'tick_count']),
      barrier: data.barrier ? String(data.barrier) : undefined,
      status: forcedStatus,
      isSold: typeof data.isSold === 'boolean' ? data.isSold : (typeof data.is_sold === 'boolean' ? data.is_sold : undefined),
      isExpired: typeof data.isExpired === 'boolean' ? data.isExpired : (typeof data.is_expired === 'boolean' ? data.is_expired : undefined),
      isSettled: typeof data.isSettled === 'boolean' ? data.isSettled : (typeof data.is_settled === 'boolean' ? data.is_settled : undefined),
      transactionId: pickNumber(data, ['transactionId', 'transaction_id']),
      sellTime: pickNumber(data, ['sellTime', 'sell_time']),
      lastDerivPayload: data.raw || undefined,
      lastUpdatedAt: now,
    };
    for (const k of Object.keys($set)) if ($set[k] === undefined) delete $set[k];

    const update = {
      $set,
      $setOnInsert: { contractId: data.contractId, updateCount: 0 },
      $inc: { updateCount: 1 },
    };

    await Contract.updateOne({ contractId: data.contractId }, update, { upsert: true });
  }

  /* --- trade ------------------------------------------------------ */

  async _createTrade(data) {
    if (!data || !data.contractId) return;

    const profit = pickNumber(data, ['profit']);
    const result = data.result || deriveResult(profit);

    const tradeId = data.tradeId || `TRD-${data.contractId}`;

    const doc = {
      tradeId,
      contractId: data.contractId,
      clientOrderId: data.clientOrderId || undefined,
      accountId: data.accountId || undefined,
      userId: data.userId || 'operator',
      symbol: data.symbol || 'UNKNOWN',
      contractType: data.contractType || 'UNKNOWN',
      currency: data.currency ? String(data.currency).toUpperCase() : 'USD',
      stake: pickNumber(data, ['stake', 'buyPrice']) || 0,
      payout: pickNumber(data, ['payout']),
      entrySpot: pickNumber(data, ['entrySpot', 'entry_spot']),
      exitSpot: pickNumber(data, ['exitSpot', 'exit_spot']),
      multiplier: pickNumber(data, ['multiplier']),
      openTime: pickNumber(data, ['openTime', 'dateStart', 'date_start']),
      closeTime: pickNumber(data, ['closeTime', 'sellTime', 'sell_time', 'dateExpiry', 'date_expiry']),
      durationSeconds: pickNumber(data, ['durationSeconds']),
      profit: profit != null ? profit : 0,
      profitPercent: pickNumber(data, ['profitPercent']),
      result,
      status: 'CLOSED',
    };

    // Trade is immutable: use upsert with $setOnInsert only.
    await Trade.updateOne(
      { tradeId },
      { $setOnInsert: doc },
      { upsert: true }
    );
  }

  /* --- account ---------------------------------------------------- */

  async _upsertAccount(data) {
    if (!data || !data.accountId) return;

    const $set = {
      mode: (data.mode || config.deriv.accountMode).toLowerCase(),
      currency: (data.currency || 'USD').toUpperCase(),
      balance: pickNumber(data, ['balance']) ?? undefined,
      loginid: data.loginid || undefined,
      email: data.email || undefined,
      country: data.country || undefined,
      isVirtual: typeof data.isVirtual === 'boolean' ? data.isVirtual : undefined,
      landingCompany: data.landingCompany || data.landing_company || undefined,
      lastSyncedAt: new Date(),
      lastSyncedAtEpoch: data.epoch || undefined,
    };
    for (const k of Object.keys($set)) if ($set[k] === undefined) delete $set[k];

    const update = {
      $set,
      $setOnInsert: { accountId: data.accountId },
    };
    if (data.rawAccount) update.$set.lastAccountResponse = stripSecrets(data.rawAccount);
    if (data.rawBalance) update.$set.lastBalanceResponse = stripSecrets(data.rawBalance);

    await Account.updateOne({ accountId: data.accountId }, update, { upsert: true });
  }

  async _updateBalance(data) {
    if (!data || !data.accountId) return;
    const balance = pickNumber(data, ['balance']);
    if (balance == null) return;

    await Account.updateOne(
      { accountId: data.accountId },
      {
        $set: {
          balance,
          lastSyncedAt: new Date(),
          lastSyncedAtEpoch: data.epoch || undefined,
        },
        $setOnInsert: {
          accountId: data.accountId,
          mode: (data.mode || config.deriv.accountMode).toLowerCase(),
          currency: (data.currency || 'USD').toUpperCase(),
        },
      },
      { upsert: true }
    );
  }

  /* --- transaction ------------------------------------------------ */

  async _createTransaction(data) {
    if (!data || data.transactionId == null) return;

    const doc = {
      transactionId: Number(data.transactionId),
      accountId: data.accountId || 'unknown',
      actionType: data.actionType || data.action_type || 'unknown',
      amount: pickNumber(data, ['amount']) ?? 0,
      balanceAfter: pickNumber(data, ['balanceAfter', 'balance_after']),
      currency: data.currency ? String(data.currency).toUpperCase() : undefined,
      contractId: data.contractId || undefined,
      reference: data.reference || undefined,
      transactionTime: pickNumber(data, ['transactionTime', 'transaction_time']),
      raw: stripSecrets(data.raw || data),
    };
    for (const k of Object.keys(doc)) if (doc[k] === undefined) delete doc[k];

    await Transaction.updateOne(
      { transactionId: doc.transactionId },
      { $setOnInsert: doc },
      { upsert: true }
    );
  }

  /* --- audit ------------------------------------------------------ */

  async _auditFromDeriv(action, data) {
    await AuditLog.create({
      action: 'DERIV_DISCONNECTED', // spec action closest; detail carries specifics
      source: 'derivGateway',
      userId: 'system',
      message: data && data.message ? String(data.message).slice(0, 500) : 'Deriv error',
      code: data && data.code ? String(data.code).slice(0, 80) : undefined,
      derivRequestId: data && data.requestId ? String(data.requestId) : undefined,
      detail: sanitizeDetail(data),
    });
  }

  async _auditError(data) {
    await AuditLog.create({
      action: 'ERROR',
      source: (data && data.source) || 'unknown',
      userId: 'system',
      message: data && data.message ? String(data.message).slice(0, 500) : 'System error',
      code: data && data.code ? String(data.code).slice(0, 80) : undefined,
      detail: sanitizeDetail(data),
    });
  }
}

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

function pickNumber(obj, keys) {
  for (const k of keys) {
    const v = obj[k];
    if (v === undefined || v === null) continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

function deriveResult(profit) {
  if (profit == null) return 'UNKNOWN';
  if (profit > 0) return 'WIN';
  if (profit < 0) return 'LOSS';
  return 'DRAW';
}

function stripSecrets(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  const out = Array.isArray(obj) ? [] : {};
  for (const [k, v] of Object.entries(obj)) {
    if (/token|password|secret|auth/i.test(k)) continue;
    out[k] = (v && typeof v === 'object') ? stripSecrets(v) : v;
  }
  return out;
}

function sanitizeDetail(data) {
  if (!data || typeof data !== 'object') return undefined;
  // Bound the size so a runaway payload cannot bloat the audit collection.
  const json = JSON.stringify(stripSecrets(data));
  if (json.length > 4_000) {
    return { truncated: true, preview: json.slice(0, 4_000) };
  }
  return JSON.parse(json);
}

/* ------------------------------------------------------------------ */
/* exports                                                             */
/* ------------------------------------------------------------------ */

const database = new Database();
const persistence = new PersistenceDispatcher();

module.exports = database;
module.exports.STATES = STATES;
module.exports.PersistenceDispatcher = PersistenceDispatcher;
module.exports.persistence = persistence;
module.exports.init = async function init() {
  await database.connect();
  persistence.start();
};
module.exports.shutdown = async function shutdown() {
  persistence.stop();
  await database.disconnect({ reason: 'shutdown' });
};
