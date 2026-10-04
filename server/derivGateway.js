'use strict';

/**
 * Ray Trading Hub — Deriv WebSocket Gateway.
 *
 * The ONLY module allowed to open a WebSocket connection to Deriv.
 *
 * Responsibilities:
 *   - Own the single WebSocket to Deriv (`wss://ws.derivws.com/websockets/v3`)
 *   - Handle the authorize handshake
 *   - Correlate every request with its response via req_id
 *   - Manage subscriptions (ticks, candles, balance, transactions, contracts)
 *   - Reconnect with exponential backoff
 *   - Restore subscriptions and re-authorize after reconnect
 *   - Heartbeat / keep-alive
 *   - Timeout every request
 *   - Translate raw Deriv messages into canonical Event Bus events
 *   - Surface errors as structured `deriv.error` events
 *
 * It NEVER:
 *   - Persists anything (that is the database module's job)
 *   - Sends anything to the browser (that is the WebSocket gateway's job)
 *   - Predicts anything
 *   - Simulates a trade
 *   - Implements a strategy
 */

const WebSocket = require('ws');
const config = require('./config');
const bus = require('./eventBus');
const { EVENTS } = require('./eventBus');
const { Logger } = require('./logger');

const log = new Logger('derivGateway');

const STATES = Object.freeze({
  IDLE: 'idle',
  CONNECTING: 'connecting',
  CONNECTED: 'connected',
  AUTHORIZING: 'authorizing',
  AUTHORIZED: 'authorized',
  RECONNECTING: 'reconnecting',
  DISCONNECTED: 'disconnected',
  CLOSING: 'closing',
});

const DEFAULT_TIMEOUT_MS = config.deriv.requestTimeoutMs;
const HEARTBEAT_MS = config.deriv.heartbeatMs;
const MAX_RECONNECT_DELAY_MS = config.deriv.maxReconnectDelayMs;
const INITIAL_RECONNECT_DELAY_MS = 1_000;
const AUTHORIZE_TIMEOUT_MS = 15_000;

// Deriv error codes we handle specially.
const ERR_INVALID_TOKEN = 'InvalidToken';
const ERR_AUTHORIZATION = 'AuthorizationRequired';
const ERR_ALREADY_SUBSCRIBED = 'AlreadySubscribed';
const ERR_RATE_LIMIT = 'RateLimit';
const ERR_INPUT_VALIDATION = 'InputValidationFailed';

class DerivGateway {
  constructor() {
    this._ws = null;
    this._state = STATES.IDLE;

    // Request correlation
    this._reqId = 0;
    this._pending = new Map();       // req_id -> { resolve, reject, timer, label }

    // Subscriptions: key -> subscription descriptor
    // key = `${type}|${variant}` where variant is symbol/granularity etc.
    this._subscriptions = new Map();

    // Deriv's subscription ids: req_id -> subscription info
    this._subscriptionIdByReqId = new Map();

    // Reconnect bookkeeping
    this._reconnectAttempt = 0;
    this._reconnectTimer = null;
    this._manualClose = false;

    // Heartbeat
    this._heartbeatTimer = null;

    // Auth state
    this._authorized = false;
    this._authorizePromise = null;
    this._accountInfo = null;

    // Diagnostics
    this._lastMessageAt = 0;
    this._messagesReceived = 0;
    this._messagesSent = 0;
    this._lastError = null;

    this._bind = this._bind.bind(this);
  }

  /* ================================================================ */
  /* public API                                                        */
  /* ================================================================ */

  get state() {
    return this._state;
  }

  get isAuthorized() {
    return this._authorized;
  }

  status() {
    return {
      state: this._state,
      authorized: this._authorized,
      accountId: this._accountInfo ? this._accountInfo.loginid : null,
      accountMode: this._accountInfo ? (this._accountInfo.is_virtual ? 'demo' : 'real') : null,
      reconnectAttempt: this._reconnectAttempt,
      subscriptions: this._subscriptions.size,
      pendingRequests: this._pending.size,
      messagesReceived: this._messagesReceived,
      messagesSent: this._messagesSent,
      lastMessageAt: this._lastMessageAt || null,
      lastError: this._lastError,
    };
  }

  /**
   * Start the gateway. Resolves once the initial connection AND
   * authorization succeed (or rejects if the initial attempt fails
   * before any successful connect — later failures trigger auto-reconnect).
   */
  async start() {
    if (this._state === STATES.CONNECTING || this._state === STATES.AUTHORIZED) return;

    this._manualClose = false;
    await this._connect();
    await this._authorize();
  }

  /**
   * Stop the gateway cleanly. Cancels reconnects, closes the socket,
   * rejects any in-flight requests.
   */
  async stop() {
    this._manualClose = true;
    this._state = STATES.CLOSING;

    if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
    if (this._heartbeatTimer) { clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }

    this._rejectAllPending(new Error('Gateway shutting down'));

    if (this._ws) {
      try { this._ws.close(1000, 'shutdown'); } catch (_) {}
    }
    this._state = STATES.DISCONNECTED;
  }

  /**
   * Send a request to Deriv and await its response.
   * Every request is correlated by req_id. Every request has a timeout.
   *
   * @param {object} payload  a Deriv request object, e.g. { ticks: 'frxEURUSD' }
   * @param {object} [opts]
   * @param {string} [opts.label]    human label for logs / errors
   * @param {number} [opts.timeoutMs]
   * @returns {Promise<object>}      the Deriv response (with `req_id` removed)
   */
  async request(payload, opts = {}) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) {
      throw makeDerivError({
        code: 'NOT_CONNECTED',
        message: 'Deriv gateway is not connected.',
        label: opts.label,
      });
    }

    const reqId = ++this._reqId;
    const label = opts.label || guessLabel(payload);
    const timeoutMs = Number.isFinite(opts.timeoutMs) ? opts.timeoutMs : DEFAULT_TIMEOUT_MS;

    const envelope = { ...payload, req_id: reqId };

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(reqId);
        reject(makeDerivError({
          code: 'REQUEST_TIMEOUT',
          message: `Deriv request timed out after ${timeoutMs}ms`,
          requestId: reqId,
          label,
        }));
      }, timeoutMs);

      this._pending.set(reqId, { resolve, reject, timer, label, payload: envelope });

      try {
        const raw = JSON.stringify(envelope);
        this._ws.send(raw);
        this._messagesSent += 1;
        log.debug('deriv.request.sent', `→ ${label}`, { reqId });
      } catch (err) {
        clearTimeout(timer);
        this._pending.delete(reqId);
        reject(makeDerivError({
          code: 'SEND_FAILED',
          message: `Failed to send Deriv request: ${err.message}`,
          requestId: reqId,
          label,
        }));
      }
    });
  }

  /**
   * Send a request that may also open a subscription.
   * Returns the initial response; the stream arrives as bus events.
   */
  async subscribe(kind, descriptor, payload, opts = {}) {
    const key = subscriptionKey(kind, descriptor);

    if (this._subscriptions.has(key)) {
      log.debug('deriv.subscribe.duplicate', `Already subscribed: ${key}`);
      return this._subscriptions.get(key).lastResponse || null;
    }

    const record = {
      key,
      kind,
      descriptor,
      payload,
      label: opts.label || `subscribe:${key}`,
      reqId: null,
      subscriptionId: null,
      lastResponse: null,
      startedAt: Date.now(),
    };

    this._subscriptions.set(key, record);

    try {
      const response = await this.request(payload, { label: record.label, timeoutMs: opts.timeoutMs });
      record.lastResponse = response;
      record.reqId = response.req_id || null;
      if (response.subscription && response.subscription.id) {
        record.subscriptionId = response.subscription.id;
        this._subscriptionIdByReqId.set(response.req_id, { key, kind });
      }
      log.info('deriv.subscribed', `Subscribed: ${key}`, {
        subscriptionId: record.subscriptionId,
      });
      return response;
    } catch (err) {
      this._subscriptions.delete(key);
      throw err;
    }
  }

  /**
   * Forget a subscription. Deriv does not require explicit forgets for every
   * stream (they end when the socket closes), but this stops local routing.
   */
  async unsubscribe(kind, descriptor) {
    const key = subscriptionKey(kind, descriptor);
    const record = this._subscriptions.get(key);
    if (!record) return;

    // Try a best-effort `forget` if Deriv gave us a subscription id.
    if (record.subscriptionId) {
      try {
        await this.request({ forget: record.subscriptionId }, { label: `forget:${key}`, timeoutMs: 5_000 });
      } catch (err) {
        log.warn('deriv.unsubscribe.forget_failed', `forget failed for ${key}: ${err.message}`);
      }
    }
    this._subscriptions.delete(key);
    if (record.reqId) this._subscriptionIdByReqId.delete(record.reqId);
    log.info('deriv.unsubscribed', `Unsubscribed: ${key}`);
  }

  /* ================================================================ */
  /* connection lifecycle                                              */
  /* ================================================================ */

  async _connect() {
    this._setState(STATES.CONNECTING);

    const url = `${config.deriv.endpoint}?app_id=${encodeURIComponent(config.deriv.appId)}&l=${encodeURIComponent('EN')}&brand=${encodeURIComponent('raytradinghub')}`;

    await new Promise((resolve, reject) => {
      let settled = false;
      const ws = new WebSocket(url, {
        handshakeTimeout: 15_000,
        perMessageDeflate: false,
        // Keep the connection small; we do not negotiate subprotocols.
      });
      this._ws = ws;

      ws.on('open', () => {
        if (settled) return;
        settled = true;
        this._setState(STATES.CONNECTED);
        log.info('deriv.connected', 'Connected to Deriv WebSocket');
        bus.emit(EVENTS.DERIV_CONNECTED, { endpoint: safeEndpoint(config.deriv.endpoint) });
        this._startHeartbeat();
        resolve();
      });

      ws.on('message', (data) => this._onMessage(data));

      ws.on('error', (err) => {
        this._lastError = { code: 'WS_ERROR', message: err.message, at: Date.now() };
        log.error('deriv.ws_error', `WebSocket error: ${err.message}`);
        if (!settled) { settled = true; reject(err); }
      });

      ws.on('close', (code, reasonBuf) => {
        const reason = reasonBuf ? reasonBuf.toString().slice(0, 200) : '';
        this._onClose(code, reason);
      });
    });
  }

  _onClose(code, reason) {
    const wasAuthorized = this._authorized;
    this._authorized = false;
    this._accountInfo = null;
    this._stopHeartbeat();
    this._rejectAllPending(makeDerivError({
      code: 'CONNECTION_CLOSED',
      message: `Deriv connection closed (code ${code})`,
    }));

    if (this._manualClose) {
      this._setState(STATES.DISCONNECTED);
      bus.emit(EVENTS.DERIV_DISCONNECTED, { code, reason, intentional: true });
      return;
    }

    log.warn('deriv.disconnected', `Deriv socket closed`, { code, reason });
    bus.emit(EVENTS.DERIV_DISCONNECTED, { code, reason, intentional: false });

    if (wasAuthorized) {
      // Clear subscriptions so that we re-establish them after re-auth.
      // Keep the records — we will replay them from _subscriptions.
      for (const rec of this._subscriptions.values()) {
        rec.reqId = null;
        rec.subscriptionId = null;
      }
      this._subscriptionIdByReqId.clear();
    }

    this._setState(STATES.RECONNECTING);
    this._scheduleReconnect();
  }

  _scheduleReconnect() {
    if (this._manualClose) return;
    if (this._reconnectTimer) return;

    this._reconnectAttempt += 1;
    const base = Math.min(
      INITIAL_RECONNECT_DELAY_MS * Math.pow(1.8, this._reconnectAttempt - 1),
      MAX_RECONNECT_DELAY_MS
    );
    const delay = base + Math.floor(Math.random() * 500);

    log.warn('deriv.reconnect.scheduled', `Reconnecting to Deriv in ${Math.round(delay / 1000)}s`, {
      attempt: this._reconnectAttempt,
    });
    bus.emit(EVENTS.DERIV_RECONNECTING, { attempt: this._reconnectAttempt, delayMs: delay });

    this._reconnectTimer = setTimeout(async () => {
      this._reconnectTimer = null;
      try {
        await this._connect();
        await this._authorize();
        await this._restoreSubscriptions();
        this._reconnectAttempt = 0;
      } catch (err) {
        log.error('deriv.reconnect.failed', `Reconnect attempt failed: ${err.message}`);
        this._setState(STATES.RECONNECTING);
        this._scheduleReconnect();
      }
    }, delay);
  }

  /* ================================================================ */
  /* authorization                                                     */
  /* ================================================================ */

  async _authorize() {
    if (this._authorizePromise) return this._authorizePromise;
    this._authorizePromise = this._doAuthorize()
      .finally(() => { this._authorizePromise = null; });
    return this._authorizePromise;
  }

  async _doAuthorize() {
    this._setState(STATES.AUTHORIZING);
    const res = await this.request(
      { authorize: config.deriv.apiToken },
      { label: 'authorize', timeoutMs: AUTHORIZE_TIMEOUT_MS }
    );

    if (res.error) {
      const err = makeDerivError({
        code: res.error.code || 'AUTHORIZE_FAILED',
        message: res.error.message || 'Authorization failed',
        requestId: res.req_id,
      });
      log.error('deriv.authorize.failed', err.message, { code: err.code });
      bus.emit(EVENTS.DERIV_ERROR, { code: err.code, message: err.message, requestId: res.req_id });
      throw err;
    }

    const info = res.authorize || {};
    this._authorized = true;
    this._accountInfo = info;
    this._setState(STATES.AUTHORIZED);

    log.info('deriv.authorized', `Authorized as ${info.loginid}`, {
      accountId: info.loginid,
      isVirtual: !!info.is_virtual,
      currency: info.currency,
      scopes: Array.isArray(info.scopes) ? info.scopes.join(',') : undefined,
    });

    bus.emit(EVENTS.DERIV_AUTHORIZED, {
      accountId: info.loginid,
      mode: info.is_virtual ? 'demo' : 'real',
      currency: info.currency,
      email: info.email,
      country: info.country,
      isVirtual: !!info.is_virtual,
      landingCompany: info.landing_company_fullname || info.landing_company_name,
      scopes: info.scopes,
      raw: info,
    });

    // Immediately sync the account snapshot after authorize.
    bus.emit(EVENTS.ACCOUNT_UPDATED, {
      accountId: info.loginid,
      mode: info.is_virtual ? 'demo' : 'real',
      currency: info.currency,
      email: info.email,
      country: info.country,
      isVirtual: !!info.is_virtual,
      landingCompany: info.landing_company_fullname || info.landing_company_name,
      loginid: info.loginid,
      rawAccount: info,
    });

    // Also push a balance snapshot — the authorize response may not include it.
    await this._syncBalance();
  }

  async _syncBalance() {
    try {
      const res = await this.request({ balance: 1, subscribe: 1 }, { label: 'balance' });
      if (res.error) throw new Error(res.error.message || 'Balance request failed');
      const b = res.balance || {};
      bus.emit(EVENTS.ACCOUNT_BALANCE_UPDATED, {
        accountId: this._accountInfo ? this._accountInfo.loginid : undefined,
        mode: this._accountInfo && this._accountInfo.is_virtual ? 'demo' : 'real',
        currency: b.currency || (this._accountInfo && this._accountInfo.currency),
        balance: Number(b.balance),
        loginid: b.loginid,
        epoch: Date.now() / 1000,
        rawBalance: b,
      });
      // Track the balance subscription for restoration after reconnect.
      if (res.subscription && res.subscription.id) {
        this._subscriptions.set('balance|default', {
          key: 'balance|default',
          kind: 'balance',
          descriptor: {},
          payload: { balance: 1, subscribe: 1 },
          label: 'balance',
          reqId: res.req_id,
          subscriptionId: res.subscription.id,
          lastResponse: res,
          startedAt: Date.now(),
        });
        this._subscriptionIdByReqId.set(res.req_id, { key: 'balance|default', kind: 'balance' });
      }
    } catch (err) {
      log.warn('deriv.balance.sync_failed', `Balance sync failed: ${err.message}`);
    }
  }

  /* ================================================================ */
  /* subscriptions restoration                                         */
  /* ================================================================ */

  async _restoreSubscriptions() {
    if (this._subscriptions.size === 0) return;

    log.info('deriv.subscriptions.restore', `Restoring ${this._subscriptions.size} subscriptions`);
    const records = [...this._subscriptions.values()];
    const results = await Promise.allSettled(
      records.map(async (rec) => {
        try {
          const res = await this.request(rec.payload, { label: `restore:${rec.label}` });
          rec.reqId = res.req_id;
          rec.subscriptionId = res.subscription && res.subscription.id ? res.subscription.id : null;
          rec.lastResponse = res;
          if (rec.reqId) this._subscriptionIdByReqId.set(rec.reqId, { key: rec.key, kind: rec.kind });
          log.info('deriv.subscription.restored', `Restored: ${rec.key}`);
        } catch (err) {
          log.warn('deriv.subscription.restore_failed', `Restore failed for ${rec.key}: ${err.message}`);
          this._subscriptions.delete(rec.key);
          throw err;
        }
      })
    );
    const failed = results.filter((r) => r.status === 'rejected').length;
    if (failed > 0) {
      log.warn('deriv.subscriptions.restore_summary', `${failed} subscription(s) failed to restore`);
    }
  }

  /* ================================================================ */
  /* heartbeat                                                         */
  /* ================================================================ */

  _startHeartbeat() {
    this._stopHeartbeat();
    this._heartbeatTimer = setInterval(() => {
      if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
      try {
        this._ws.ping();
      } catch (_) {}
    }, HEARTBEAT_MS);
  }

  _stopHeartbeat() {
    if (this._heartbeatTimer) { clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
  }

  /* ================================================================ */
  /* message handling                                                  */
  /* ================================================================ */

  _onMessage(raw) {
    this._messagesReceived += 1;
    this._lastMessageAt = Date.now();

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      log.warn('deriv.message.parse_failed', `Non-JSON message from Deriv: ${err.message}`);
      return;
    }

    // ------------------------------------------------------------
    // Error responses
    // ------------------------------------------------------------
    if (msg.error) {
      const reqId = msg.req_id;
      const err = makeDerivError({
        code: msg.error.code || 'DERIV_ERROR',
        message: msg.error.message || 'Deriv returned an error',
        requestId: reqId,
        details: msg.error,
      });

      // Attach to the pending request if we can.
      if (reqId != null && this._pending.has(reqId)) {
        const entry = this._pending.get(reqId);
        clearTimeout(entry.timer);
        this._pending.delete(reqId);
        entry.reject(err);
      }

      // Emit as a bus event for observability (only once per error).
      log.warn('deriv.api_error', `Deriv API error: ${err.code} ${err.message}`, {
        reqId,
        code: err.code,
      });
      bus.emit(EVENTS.DERIV_ERROR, {
        code: err.code,
        message: err.message,
        requestId: reqId,
        details: msg.error,
      });
      return;
    }

    // ------------------------------------------------------------
    // Dispatch by message type
    // ------------------------------------------------------------
    if (msg.msg_type) {
      this._dispatchStream(msg);
    }

    // ------------------------------------------------------------
    // Resolve the pending request, if this message carries its req_id.
    // Note: many Deriv subscription pushes carry a req_id equal to the
    // request that opened the subscription. We still resolve that first
    // request exactly once via _pending, and route subsequent pushes
    // purely through _dispatchStream.
    // ------------------------------------------------------------
    const reqId = msg.req_id;
    if (reqId != null && this._pending.has(reqId)) {
      const entry = this._pending.get(reqId);
      clearTimeout(entry.timer);
      this._pending.delete(reqId);
      entry.resolve(msg);
    }
  }

  _dispatchStream(msg) {
    switch (msg.msg_type) {
      case 'tick':            return this._onTick(msg.tick);
      case 'ohlc':            return this._onOhlc(msg.ohlc);
      case 'balance':         return this._onBalance(msg.balance);
      case 'transaction':     return this._onTransaction(msg.transaction);
      case 'proposal_open_contract': return this._onProposalOpenContract(msg.proposal_open_contract);
      default:
        // Other stream types (e.g. `candles`, `proposal`, `buy`) are request/
        // response and are handled via the pending-request resolver above.
        return;
    }
  }

  _onTick(tick) {
    if (!tick || !tick.symbol) return;
    const symbol = config.normalizeSymbol(tick.symbol);
    const event = {
      symbol,
      derivSymbol: tick.symbol,
      quote: Number(tick.quote),
      bid: tick.bid != null ? Number(tick.bid) : null,
      ask: tick.ask != null ? Number(tick.ask) : null,
      epoch: Number(tick.epoch) || Math.floor(Date.now() / 1000),
      pipSize: tick.pip_size != null ? Number(tick.pip_size) : null,
    };
    bus.emit(EVENTS.MARKET_TICK, event);
  }

  _onOhlc(ohlc) {
    if (!ohlc || !ohlc.symbol) return;
    const symbol = config.normalizeSymbol(ohlc.symbol);
    const openTime = Number(ohlc.open_time);
    const granularity = Number(ohlc.granularity);
    if (!Number.isFinite(openTime) || !Number.isFinite(granularity)) return;

    const event = {
      symbol,
      derivSymbol: ohlc.symbol,
      granularity,
      epoch: openTime,
      open: Number(ohlc.open),
      high: Number(ohlc.high),
      low: Number(ohlc.low),
      close: Number(ohlc.close),
      volume: ohlc.volume != null ? Number(ohlc.volume) : undefined,
      openTime,
      closeTime: openTime + granularity,
      closed: openTime + granularity <= Math.floor(Date.now() / 1000),
    };

    if (event.closed) {
      bus.emit(EVENTS.MARKET_CANDLE_CLOSED, event);
    } else {
      bus.emit(EVENTS.MARKET_CANDLE_UPDATED, event);
    }
  }

  _onBalance(balance) {
    if (!balance) return;
    bus.emit(EVENTS.ACCOUNT_BALANCE_UPDATED, {
      accountId: this._accountInfo ? this._accountInfo.loginid : (balance.loginid || undefined),
      mode: this._accountInfo && this._accountInfo.is_virtual ? 'demo' : 'real',
      currency: balance.currency,
      balance: Number(balance.balance),
      loginid: balance.loginid,
      epoch: Date.now() / 1000,
      rawBalance: balance,
    });
  }

  _onTransaction(tx) {
    if (!tx || tx.transaction_id == null) return;
    bus.emit(EVENTS.ACCOUNT_TRANSACTION, {
      transactionId: Number(tx.transaction_id),
      accountId: this._accountInfo ? this._accountInfo.loginid : undefined,
      actionType: tx.action_type || 'unknown',
      amount: Number(tx.amount),
      balanceAfter: tx.balance != null ? Number(tx.balance) : undefined,
      currency: tx.currency,
      contractId: tx.contract_id != null ? String(tx.contract_id) : undefined,
      reference: tx.reference,
      transactionTime: Number(tx.transaction_time) || Math.floor(Date.now() / 1000),
      raw: tx,
    });
  }

  _onProposalOpenContract(poc) {
    if (!poc || poc.contract_id == null) return;
    const contractId = String(poc.contract_id);
    const symbol = poc.underlying ? config.normalizeSymbol(poc.underlying) : undefined;

    const isSold = !!poc.is_sold;
    const isExpired = !!poc.is_expired;
    const isSettled = !!poc.is_settled;
    const profit = poc.profit != null ? Number(poc.profit) : undefined;

    const normalised = {
      contractId,
      clientOrderId: poc.client_order_id || poc.purchase_reference || undefined,
      accountId: this._accountInfo ? this._accountInfo.loginid : undefined,
      userId: 'operator',
      symbol,
      contractType: poc.contract_type,
      currency: poc.currency,
      underlying: poc.underlying,
      buyPrice: poc.buy_price != null ? Number(poc.buy_price) : undefined,
      stake: poc.buy_price != null ? Number(poc.buy_price) : undefined,
      payout: poc.payout != null ? Number(poc.payout) : undefined,
      entrySpot: poc.entry_spot != null ? Number(poc.entry_spot) : undefined,
      currentSpot: poc.current_spot != null ? Number(poc.current_spot) : undefined,
      exitSpot: poc.exit_tick != null ? Number(poc.exit_tick) : undefined,
      entryTickTime: poc.entry_tick_time != null ? Number(poc.entry_tick_time) : undefined,
      exitTickTime: poc.exit_tick_time != null ? Number(poc.exit_tick_time) : undefined,
      profit,
      profitCurrency: poc.profit_currency,
      multiplier: poc.multiplier != null ? Number(poc.multiplier) : undefined,
      dateStart: poc.date_start != null ? Number(poc.date_start) : undefined,
      dateExpiry: poc.date_expiry != null ? Number(poc.date_expiry) : undefined,
      duration: poc.duration != null ? Number(poc.duration) : undefined,
      durationUnit: poc.duration_unit,
      tickCount: poc.tick_count != null ? Number(poc.tick_count) : undefined,
      barrier: poc.barrier,
      status: isSold ? 'CLOSED' : 'OPEN',
      isSold,
      isExpired,
      isSettled,
      transactionId: poc.transaction_id != null ? Number(poc.transaction_id) : undefined,
      sellTime: poc.sell_time != null ? Number(poc.sell_time) : undefined,
      updateCount: 1,
      lastUpdatedAt: new Date(),
      raw: poc,
    };

    if (isSold || isSettled) {
      bus.emit(EVENTS.CONTRACT_CLOSED, normalised);
      bus.emit(EVENTS.POSITION_CLOSED, normalised);

      // Only emit trade.completed for genuinely settled contracts, so an
      // `is_expired` intermediate push does not create a duplicate Trade.
      if (isSettled) {
        bus.emit(EVENTS.TRADE_COMPLETED, {
          ...normalised,
          result: deriveResult(profit),
          closeTime: normalised.sellTime || normalised.exitTickTime || Math.floor(Date.now() / 1000),
        });
      }
    } else {
      bus.emit(EVENTS.CONTRACT_UPDATED, normalised);
      bus.emit(EVENTS.POSITION_UPDATED, normalised);
    }
  }

  /* ================================================================ */
  /* helpers                                                           */
  /* ================================================================ */

  _setState(next) {
    if (this._state === next) return;
    this._state = next;
    bus.emit(EVENTS.SYSTEM_STATUS, {
      deriv: { state: next, at: Date.now() },
    });
  }

  _rejectAllPending(err) {
    for (const [, entry] of this._pending) {
      clearTimeout(entry.timer);
      try { entry.reject(err); } catch (_) {}
    }
    this._pending.clear();
  }
}

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

function subscriptionKey(kind, descriptor) {
  if (!descriptor || Object.keys(descriptor).length === 0) return `${kind}|default`;
  const parts = Object.keys(descriptor).sort().map((k) => `${k}=${descriptor[k]}`);
  return `${kind}|${parts.join('&')}`;
}

function makeDerivError({ code, message, requestId, label, details }) {
  const err = new Error(message || 'Deriv error');
  err.name = 'DerivError';
  err.code = code || 'DERIV_ERROR';
  if (requestId != null) err.requestId = requestId;
  if (label) err.label = label;
  if (details) err.details = details;
  return err;
}

function guessLabel(payload) {
  if (!payload || typeof payload !== 'object') return 'request';
  const keys = Object.keys(payload).filter((k) => k !== 'req_id');
  return keys.length ? keys.join(',') : 'request';
}

function safeEndpoint(url) {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.host}${u.pathname}`;
  } catch (_) {
    return 'deriv';
  }
}

function deriveResult(profit) {
  if (profit == null) return 'UNKNOWN';
  if (profit > 0) return 'WIN';
  if (profit < 0) return 'LOSS';
  return 'DRAW';
}

/* ------------------------------------------------------------------ */
/* singleton                                                           */
/* ------------------------------------------------------------------ */

const gateway = new DerivGateway();

module.exports = gateway;
module.exports.DerivGateway = DerivGateway;
module.exports.STATES = STATES;
