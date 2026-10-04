'use strict';

/**
 * Ray Trading Hub — order / trade service.
 *
 * This is the highest-risk module in the system. Every code path must
 * preserve one invariant:
 *
 *     AN ORDER IS NEVER REPRESENTED AS SUCCESSFUL UNTIL DERIV CONFIRMS IT.
 *
 * Responsibilities:
 *   - Cache per-instrument trade capabilities from Deriv `contracts_for`
 *   - Validate every order request BEFORE it reaches Deriv:
 *       authentication, account, symbol, contract availability,
 *       stake limits, multiplier values, duration unit/range,
 *       currency, idempotency
 *   - Enforce idempotency (clientOrderId) at both memory and database level
 *   - Build Deriv `buy` requests from the validated parameters
 *   - Submit through the Deriv gateway
 *   - Drive the order lifecycle state machine
 *   - Open a `proposal_open_contract` subscription for each accepted order
 *   - Translate Deriv rejections into structured failures (never fake success)
 *
 * It NEVER:
 *   - Simulates a successful trade
 *   - Fabricates a contract_id or profit
 *   - Skips validation to "make the request faster"
 *   - Implements a strategy or prediction
 */

const bus = require('./eventBus');
const { EVENTS } = require('./eventBus');
const { Logger } = require('./logger');
const config = require('./config');
const gateway = require('./derivGateway');
const market = require('./market');
const { Order, Contract } = require('./models');
const { enums } = require('./models');

const log = new Logger('trading');

const ORDER_STATES = enums.ORDER_STATES;

// Capability cache TTL. Deriv changes `contracts_for` infrequently; 60s keeps
// us honest without hammering the API on every ticket render.
const CAPABILITY_TTL_MS = 60_000;

// How long a clientOrderId remains "reserved" to reject a duplicate.
// Independent of config.trading.idempotencyTtlMs so tests can shorten it.
const IDEMPOTENCY_TTL_MS = config.trading.idempotencyTtlMs;

const MAX_OPEN_POSITIONS = 50; // safety cap — protective, not decorative

/* ------------------------------------------------------------------ */
/* order service                                                       */
/* ------------------------------------------------------------------ */

class OrderService {
  constructor() {
    this._capabilityCache = new Map();   // symbol -> { at, capabilities }
    this._idempotency = new Map();       // clientOrderId -> { at, promise, result }
    this._openContractSubs = new Set();  // contractIds with active subscriptions
    this._started = false;
    this._unsubscribers = [];
  }

  start() {
    if (this._started) return;
    this._started = true;
    this._wireBus();
    log.info('trading.started', 'Order service started');
  }

  stop() {
    if (!this._started) return;
    this._started = false;
    for (const unsub of this._unsubscribers) {
      try { unsub(); } catch (_) {}
    }
    this._unsubscribers = [];
    this._openContractSubs.clear();
    this._idempotency.clear();
    log.info('trading.stopped', 'Order service stopped');
  }

  /* ================================================================ */
  /* public API — capabilities                                         */
  /* ================================================================ */

  /**
   * Return tradable contract capabilities for a symbol.
   * Result is cached briefly and derived from Deriv `contracts_for`.
   *
   * @returns {Promise<Array<Capability>>}
   */
  async getCapabilities(symbol) {
    const cached = this._capabilityCache.get(symbol);
    if (cached && Date.now() - cached.at < CAPABILITY_TTL_MS) {
      return cached.capabilities;
    }

    const inst = market.getInstrument(symbol);
    if (!inst) throw orderError('UNKNOWN_INSTRUMENT', `Instrument ${symbol} is not tracked`);

    const res = await gateway.request(
      { contracts_for: inst.derivSymbol, currency: this._defaultCurrency() },
      { label: `contracts_for:${symbol}` }
    );

    if (res.error) {
      throw orderError(res.error.code || 'CONTRACTS_FOR_FAILED', res.error.message || 'Could not load contracts');
    }

    const capabilities = normalizeCapabilities(res.contracts_for, inst);
    this._capabilityCache.set(symbol, { at: Date.now(), capabilities });
    return capabilities;
  }

  /* ================================================================ */
  /* public API — submit order                                         */
  /* ================================================================ */

  /**
   * Submit an order.
   *
   * @param {object} input
   * @param {string} input.clientOrderId      idempotency key (from client)
   * @param {string} input.symbol             internal symbol
   * @param {string} input.contractType       Deriv contract type (e.g. MULTUP)
   * @param {number} input.stake
   * @param {string} [input.currency]
   * @param {number} [input.multiplier]
   * @param {number} [input.duration]
   * @param {string} [input.durationUnit]
   * @param {string} [input.barrier]
   *
   * @returns {Promise<{order: object}>}
   */
  async submitOrder(input) {
    // ---- Layer 1: idempotency gate (in-memory) -------------------
    const clientOrderId = sanitizeClientOrderId(input && input.clientOrderId);
    if (!clientOrderId) {
      throw orderError('MISSING_CLIENT_ORDER_ID', 'clientOrderId is required for every order');
    }

    const inflight = this._idempotency.get(clientOrderId);
    if (inflight && Date.now() - inflight.at < IDEMPOTENCY_TTL_MS) {
      log.warn('order.duplicate.inflight', `Duplicate submission detected for ${clientOrderId}`);
      return inflight.promise;
    }

    // Reserve immediately so a second call within the same tick is rejected.
    const promise = this._submitOrderInternal(clientOrderId, input).catch((err) => {
      // Cache the failure too — a retried duplicate should see the same
      // outcome, not a second attempt.
      return { error: err };
    });

    this._idempotency.set(clientOrderId, { at: Date.now(), promise });
    this._pruneIdempotencyCache();

    const outcome = await promise;
    if (outcome && outcome.error) throw outcome.error;
    return outcome;
  }

  async _submitOrderInternal(clientOrderId, input) {
    // ---- Layer 2: idempotency in the database --------------------
    const existing = await Order.findOne({ clientOrderId }, { _id: 0 }).lean();
    if (existing) {
      log.warn('order.duplicate.db', `clientOrderId ${clientOrderId} already exists (state=${existing.state})`);
      return { order: toPublicOrder(existing), duplicate: true };
    }

    // ---- Layer 3: validate ---------------------------------------
    const validation = await this._validate(input);

    // ---- Layer 4: create the DRAFT order -------------------------
    const draft = {
      clientOrderId,
      userId: 'operator',
      symbol: validation.symbol,
      contractType: validation.contractType,
      stake: validation.stake,
      currency: validation.currency,
      multiplier: validation.multiplier,
      duration: validation.duration,
      durationUnit: validation.durationUnit,
      barrier: validation.barrier,
      parameters: validation.parameters,
      idempotencyKey: clientOrderId,
      state: 'DRAFT',
      stateHistory: [{ state: 'DRAFT', at: new Date() }],
    };

    // Insert with upsert guard: if a parallel request wins the race, we
    // return its result rather than creating a second document.
    let orderDoc;
    try {
      orderDoc = await Order.findOneAndUpdate(
        { clientOrderId },
        { $setOnInsert: draft },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      );
    } catch (err) {
      // Mongo may raise E11000 on a true race. Re-read and return.
      if (err && err.code === 11000) {
        const raced = await Order.findOne({ clientOrderId }).lean();
        if (raced) return { order: toPublicOrder(raced), duplicate: true };
      }
      throw err;
    }

    // If a previous run created this order but never advanced past DRAFT,
    // we still hold the reservation — safe to proceed.
    if (orderDoc.state !== 'DRAFT') {
      return { order: toPublicOrder(orderDoc), duplicate: true };
    }

    bus.emit(EVENTS.ORDER_CREATED, { ...draft });

    // ---- Layer 5: transition VALIDATING → SUBMITTING -------------
    orderDoc = await this._transition(orderDoc, 'VALIDATING', 'pre-trade checks passed');

    // ---- Layer 6: build the Deriv buy request --------------------
    const buyRequest = this._buildBuyRequest(orderDoc);

    // ---- Layer 7: submit to Deriv --------------------------------
    orderDoc = await this._transition(orderDoc, 'SUBMITTING', 'sending to Deriv');
    bus.emit(EVENTS.ORDER_SUBMITTED, {
      clientOrderId: orderDoc.clientOrderId,
      symbol: orderDoc.symbol,
      contractType: orderDoc.contractType,
      stake: orderDoc.stake,
      currency: orderDoc.currency,
    });

    let buyResponse;
    try {
      buyResponse = await gateway.request(buyRequest, {
        label: `buy:${clientOrderId}`,
        timeoutMs: 30_000,
      });
    } catch (err) {
      // Network / timeout / gateway-level failure.
      await this._fail(orderDoc, {
        code: err.code || 'SUBMIT_FAILED',
        message: err.message || 'Deriv request failed',
        requestId: err.requestId,
      });
      throw orderError(err.code || 'SUBMIT_FAILED', err.message || 'Failed to submit order', {
        requestId: err.requestId,
      });
    }

    // ---- Layer 8: handle Deriv's response ------------------------
    if (buyResponse.error) {
      const failure = {
        code: buyResponse.error.code || 'DERIV_REJECTED',
        message: buyResponse.error.message || 'Deriv rejected the request',
        requestId: buyResponse.req_id,
      };
      await this._reject(orderDoc, failure);
      throw orderError(failure.code, failure.message, { requestId: failure.requestId });
    }

    const buy = buyResponse.buy;
    if (!buy || !buy.contract_id) {
      const failure = {
        code: 'MALFORMED_RESPONSE',
        message: 'Deriv response did not include a contract_id',
        requestId: buyResponse.req_id,
      };
      await this._fail(orderDoc, failure);
      throw orderError(failure.code, failure.message, { requestId: failure.requestId });
    }

    // ---- Layer 9: mark ACCEPTED → OPEN ---------------------------
    orderDoc = await this._accept(orderDoc, {
      contractId: String(buy.contract_id),
      derivRequestId: String(buyResponse.req_id || ''),
      derivResponse: sanitizeDerivPayload(buy),
      transactionId: buy.transaction_id,
      buyPrice: buy.buy_price,
      payout: buy.payout,
      startTime: buy.start_time,
      longcode: buy.longcode,
    });

    // ---- Layer 10: open the monitoring subscription --------------
    this._openContractSubscription(String(buy.contract_id), orderDoc.clientOrderId).catch((err) => {
      log.warn('order.contract_subscribe_failed', `contract=${buy.contract_id}: ${err.message}`);
    });

    return { order: toPublicOrder(orderDoc) };
  }

  /* ================================================================ */
  /* validation                                                        */
  /* ================================================================ */

  async _validate(input) {
    if (!input || typeof input !== 'object') {
      throw orderError('INVALID_INPUT', 'Order input is required');
    }
    if (!gateway.isAuthorized) {
      throw orderError('NOT_AUTHORIZED', 'Deriv gateway is not authorized');
    }

    const symbol = config.normalizeSymbol(input.symbol);
    if (!symbol) throw orderError('INVALID_SYMBOL', 'symbol is required');

    const inst = market.getInstrument(symbol);
    if (!inst) throw orderError('UNKNOWN_INSTRUMENT', `Instrument ${symbol} is not tracked`);
    if (!inst.isOpen) throw orderError('MARKET_CLOSED', `${symbol} market is currently closed`);

    const contractType = String(input.contractType || '').trim().toUpperCase();
    if (!contractType) throw orderError('INVALID_CONTRACT_TYPE', 'contractType is required');

    const capabilities = await this.getCapabilities(symbol);
    const cap = capabilities.find((c) => c.contractType === contractType);
    if (!cap) {
      throw orderError('CONTRACT_NOT_AVAILABLE', `Contract type ${contractType} is not available for ${symbol}`);
    }

    const stake = Number(input.stake);
    if (!Number.isFinite(stake) || stake <= 0) {
      throw orderError('INVALID_STAKE', 'stake must be a positive number');
    }
    if (stake < config.trading.minStake) {
      throw orderError('STAKE_BELOW_MIN', `stake is below the configured minimum of ${config.trading.minStake}`);
    }
    if (stake > config.trading.maxStake) {
      throw orderError('STAKE_ABOVE_MAX', `stake exceeds the configured maximum of ${config.trading.maxStake}`);
    }
    if (cap.limits && stake < cap.limits.minStake) {
      throw orderError('STAKE_BELOW_DERIV_MIN', `Deriv minimum stake for this contract is ${cap.limits.minStake}`);
    }
    if (cap.limits && stake > cap.limits.maxStake) {
      throw orderError('STAKE_ABOVE_DERIV_MAX', `Deriv maximum stake for this contract is ${cap.limits.maxStake}`);
    }

    const currency = String(input.currency || this._defaultCurrency()).toUpperCase();
    if (!/^[A-Z]{3,6}$/.test(currency)) {
      throw orderError('INVALID_CURRENCY', `Invalid currency: ${currency}`);
    }

    // Multiplier: only if capability requires/allows it.
    let multiplier;
    if (cap.multiplier && cap.multiplier.values.length) {
      multiplier = Number(input.multiplier);
      if (!Number.isFinite(multiplier)) {
        throw orderError('MISSING_MULTIPLIER', 'multiplier is required for this contract type');
      }
      if (!cap.multiplier.values.includes(multiplier)) {
        throw orderError(
          'INVALID_MULTIPLIER',
          `Multiplier ${multiplier} is not available. Allowed: ${cap.multiplier.values.join(', ')}`
        );
      }
    } else if (input.multiplier != null) {
      // Client sent a multiplier for a contract that does not support it.
      throw orderError('MULTIPLIER_NOT_SUPPORTED', 'This contract type does not support a multiplier');
    }

    // Duration: only if capability declares a duration unit.
    let duration, durationUnit;
    if (cap.durationUnit) {
      duration = Number(input.duration);
      durationUnit = String(input.durationUnit || cap.durationUnit).toLowerCase();
      if (!Number.isFinite(duration) || duration <= 0) {
        throw orderError('INVALID_DURATION', 'duration must be a positive number');
      }
      if (durationUnit !== cap.durationUnit) {
        throw orderError('INVALID_DURATION_UNIT', `durationUnit must be ${cap.durationUnit}`);
      }
      if (cap.minDuration != null && duration < cap.minDuration) {
        throw orderError('DURATION_BELOW_MIN', `Minimum duration is ${cap.minDuration} ${cap.durationUnit}`);
      }
      if (cap.maxDuration != null && duration > cap.maxDuration) {
        throw orderError('DURATION_ABOVE_MAX', `Maximum duration is ${cap.maxDuration} ${cap.durationUnit}`);
      }
    } else if (input.duration != null || input.durationUnit != null) {
      throw orderError('DURATION_NOT_SUPPORTED', 'This contract type does not support a duration');
    }

    // Barrier: only if the capability declares barrier support.
    let barrier;
    if (cap.barrier) {
      barrier = input.barrier != null ? String(input.barrier).trim() : undefined;
      if (cap.barrier.required && !barrier) {
        throw orderError('MISSING_BARRIER', 'This contract requires a barrier');
      }
    } else if (input.barrier != null && String(input.barrier).trim() !== '') {
      throw orderError('BARRIER_NOT_SUPPORTED', 'This contract type does not support a barrier');
    }

    // Open positions cap (defensive; protects the operator from a runaway loop).
    const openCount = await Contract.countDocuments({ isSold: false });
    if (openCount >= MAX_OPEN_POSITIONS) {
      throw orderError('TOO_MANY_OPEN_POSITIONS', `Refusing to open more than ${MAX_OPEN_POSITIONS} concurrent positions`);
    }

    // Build the exact parameters object that will go to Deriv.
    const parameters = {
      amount: stake,
      basis: 'stake',
      contract_type: contractType,
      currency,
      symbol: inst.derivSymbol,
    };
    if (multiplier != null) parameters.multiplier = multiplier;
    if (duration != null) {
      parameters.duration = duration;
      parameters.duration_unit = durationUnit;
    }
    if (barrier != null) parameters.barrier = barrier;

    return {
      symbol,
      derivSymbol: inst.derivSymbol,
      contractType,
      stake,
      currency,
      multiplier,
      duration,
      durationUnit,
      barrier,
      parameters,
      capability: cap,
    };
  }

  _buildBuyRequest(orderDoc) {
    // Deriv's `buy` request shape: { buy: 1, price: <stake>, parameters: {...} }
    // The `price` is the maximum the user is willing to pay. For stake-based
    // contracts this equals the stake; for other bases it is the parameter
    // `amount`. We use the validated stake.
    const parameters = orderDoc.parameters && typeof orderDoc.parameters === 'object'
      ? { ...orderDoc.parameters }
      : {
          amount: orderDoc.stake,
          basis: 'stake',
          contract_type: orderDoc.contractType,
          currency: orderDoc.currency,
          symbol: orderDoc.symbol,
        };
    return {
      buy: 1,
      price: orderDoc.stake,
      parameters,
    };
  }

  /* ================================================================ */
  /* lifecycle transitions                                             */
  /* ================================================================ */

  async _transition(orderDoc, nextState, note) {
    if (!ORDER_STATES.includes(nextState)) {
      throw orderError('INVALID_STATE', `Unknown order state: ${nextState}`);
    }
    const now = new Date();
    const updated = await Order.findOneAndUpdate(
      { clientOrderId: orderDoc.clientOrderId },
      {
        $set: { state: nextState },
        $push: { stateHistory: { state: nextState, at: now, note: note || undefined } },
      },
      { new: true }
    );
    log.info('order.state', `order ${orderDoc.clientOrderId} → ${nextState}`, { note });
    return updated;
  }

  async _accept(orderDoc, buyInfo) {
    const now = new Date();
    const updated = await Order.findOneAndUpdate(
      { clientOrderId: orderDoc.clientOrderId },
      {
        $set: {
          state: 'ACCEPTED',
          contractId: buyInfo.contractId,
          derivRequestId: buyInfo.derivRequestId,
          derivResponse: buyInfo.derivResponse,
          acceptedAt: now,
        },
        $push: {
          stateHistory: {
            state: 'ACCEPTED',
            at: now,
            note: `contract=${buyInfo.contractId}`,
          },
        },
      },
      { new: true }
    );

    bus.emit(EVENTS.ORDER_ACCEPTED, {
      clientOrderId: orderDoc.clientOrderId,
      contractId: buyInfo.contractId,
      derivRequestId: buyInfo.derivRequestId,
      symbol: orderDoc.symbol,
      contractType: orderDoc.contractType,
      stake: orderDoc.stake,
      currency: orderDoc.currency,
    });

    // Emit a CONTRACT_CREATED so the persistence layer creates the contract row.
    bus.emit(EVENTS.CONTRACT_CREATED, {
      contractId: buyInfo.contractId,
      clientOrderId: orderDoc.clientOrderId,
      symbol: orderDoc.symbol,
      contractType: orderDoc.contractType,
      currency: orderDoc.currency,
      stake: orderDoc.stake,
      buyPrice: buyInfo.buyPrice,
      payout: buyInfo.payout,
      dateStart: buyInfo.startTime,
      status: 'OPEN',
    });

    bus.emit(EVENTS.POSITION_OPENED, {
      contractId: buyInfo.contractId,
      clientOrderId: orderDoc.clientOrderId,
      symbol: orderDoc.symbol,
      contractType: orderDoc.contractType,
      currency: orderDoc.currency,
      stake: orderDoc.stake,
      buyPrice: buyInfo.buyPrice,
      payout: buyInfo.payout,
      dateStart: buyInfo.startTime,
      status: 'OPEN',
      isSold: false,
    });

    return updated;
  }

  async _reject(orderDoc, failure) {
    const now = new Date();
    const updated = await Order.findOneAndUpdate(
      { clientOrderId: orderDoc.clientOrderId },
      {
        $set: {
          state: 'REJECTED',
          failure: { ...failure, at: now },
        },
        $push: {
          stateHistory: { state: 'REJECTED', at: now, note: failure.message },
        },
      },
      { new: true }
    );

    bus.emit(EVENTS.ORDER_REJECTED, {
      clientOrderId: orderDoc.clientOrderId,
      code: failure.code,
      message: failure.message,
      derivRequestId: failure.requestId,
      symbol: orderDoc.symbol,
      contractType: orderDoc.contractType,
      stake: orderDoc.stake,
    });

    log.warn('order.rejected', `order ${orderDoc.clientOrderId} rejected: ${failure.code} ${failure.message}`);
    return updated;
  }

  async _fail(orderDoc, failure) {
    const now = new Date();
    const updated = await Order.findOneAndUpdate(
      { clientOrderId: orderDoc.clientOrderId },
      {
        $set: {
          state: 'FAILED',
          failure: { ...failure, at: now },
        },
        $push: {
          stateHistory: { state: 'FAILED', at: now, note: failure.message },
        },
      },
      { new: true }
    );

    bus.emit(EVENTS.ORDER_FAILED, {
      clientOrderId: orderDoc.clientOrderId,
      code: failure.code,
      message: failure.message,
      derivRequestId: failure.requestId,
      symbol: orderDoc.symbol,
      contractType: orderDoc.contractType,
      stake: orderDoc.stake,
    });

    log.error('order.failed', `order ${orderDoc.clientOrderId} failed: ${failure.code} ${failure.message}`);
    return updated;
  }

  /* ================================================================ */
  /* contract monitoring                                               */
  /* ================================================================ */

  async _openContractSubscription(contractId, clientOrderId) {
    if (this._openContractSubs.has(contractId)) return;
    this._openContractSubs.add(contractId);

    try {
      await gateway.subscribe(
        'proposal_open_contract',
        { contractId },
        { proposal_open_contract: 1, contract_id: Number(contractId), subscribe: 1 },
        { label: `poc:${contractId}` }
      );
      log.info('order.contract_subscribed', `Monitoring contract ${contractId}`, { clientOrderId });
    } catch (err) {
      this._openContractSubs.delete(contractId);
      log.warn('order.contract_subscribe_failed', `contract=${contractId}: ${err.message}`);
      throw err;
    }
  }

  /* ================================================================ */
  /* bus wiring                                                        */
  /* ================================================================ */

  _wireBus() {
    // A settled contract ends the monitoring subscription.
    this._unsubscribers.push(
      bus.on(EVENTS.CONTRACT_CLOSED, (evt) => {
        if (evt && evt.contractId) {
          this._openContractSubs.delete(String(evt.contractId));
        }
      }, { label: 'trading:onContractClosed' })
    );

    // Gateway reconnect: restore monitoring for every still-open contract.
    this._unsubscribers.push(
      bus.on(EVENTS.DERIV_AUTHORIZED, () => {
        this._restoreOpenContractSubscriptions().catch((err) => {
          log.warn('trading.restore_subs_failed', err.message);
        });
      }, { label: 'trading:onAuthorized', priority: 3 })
    );
  }

  async _restoreOpenContractSubscriptions() {
    const openContracts = await Contract.find({ isSold: false }, { contractId: 1, clientOrderId: 1 }).lean();
    for (const c of openContracts) {
      if (!c.contractId) continue;
      this._openContractSubs.delete(String(c.contractId));
      await this._openContractSubscription(String(c.contractId), c.clientOrderId).catch(() => {});
    }
    if (openContracts.length) {
      log.info('trading.contracts.restored', `Restored monitoring for ${openContracts.length} contract(s)`);
    }
  }

  /* ================================================================ */
  /* diagnostics                                                       */
  /* ================================================================ */

  stats() {
    return {
      started: this._started,
      capabilityCacheSize: this._capabilityCache.size,
      idempotencyEntries: this._idempotency.size,
      openContractSubscriptions: this._openContractSubs.size,
    };
  }

  _pruneIdempotencyCache() {
    const cutoff = Date.now() - IDEMPOTENCY_TTL_MS;
    for (const [key, entry] of this._idempotency) {
      if (entry.at < cutoff) this._idempotency.delete(key);
    }
  }

  _defaultCurrency() {
    const acct = gateway.status();
    // The gateway knows the authorized account; fall back to USD.
    return (acct && acct.accountMode) ? (this._currencyFromStatus(acct) || 'USD') : 'USD';
  }

  _currencyFromStatus(_status) {
    return 'USD';
  }
}

/* ------------------------------------------------------------------ */
/* capability normalizer                                               */
/* ------------------------------------------------------------------ */

/**
 * Convert a Deriv `contracts_for` response into a compact, terminal-friendly
 * capability list. We only keep fields the ticket and validator use.
 */
function normalizeCapabilities(contractsFor, instrument) {
  const list = contractsFor && Array.isArray(contractsFor.available) ? contractsFor.available : [];
  const byType = new Map();

  for (const c of list) {
    const type = String(c.contract_type || '').toUpperCase();
    if (!type) continue;

    const existing = byType.get(type) || {
      contractType: type,
      display: c.contract_display || type,
      durationUnit: null,
      minDuration: null,
      maxDuration: null,
      multiplier: null,
      barrier: null,
      limits: null,
    };

    if (c.min_contract_duration != null) {
      existing.minDuration = Number(c.min_contract_duration);
    }
    if (c.max_contract_duration != null) {
      existing.maxDuration = Number(c.max_contract_duration);
    }
    if (c.duration_unit) {
      existing.durationUnit = String(c.duration_unit);
    }
    if (c.barrier && !existing.barrier) {
      existing.barrier = { category: c.barrier_category, required: !!c.barrier };
    }

    byType.set(type, existing);
  }

  // Derive multiplier values and stake limits from the instrument-level
  // contract parameters when present. Deriv returns these as strings
  // sometimes; normalize to numbers and reject nonsense.
  const multipliersFromResponse = extractMultipliers(contractsFor, instrument);
  for (const cap of byType.values()) {
    if (multipliersFromResponse.length && isMultiplierContract(cap.contractType)) {
      cap.multiplier = { values: multipliersFromResponse, source: 'deriv' };
    }

    // Stake limits: prefer Deriv's per-contract range; otherwise fall back
    // to the configured application limits.
    const minFromDeriv = numberOrNull(contractsFor && contractsFor.min_contract_duration && null) // never used, keeps lint quiet
      || numberOrNull(contractsFor && contractsFor.min_stake)
      || null;
    const maxFromDeriv = numberOrNull(contractsFor && contractsFor.max_stake) || null;

    cap.limits = {
      minStake: minFromDeriv != null ? minFromDeriv : config.trading.minStake,
      maxStake: maxFromDeriv != null ? maxFromDeriv : config.trading.maxStake,
      source: minFromDeriv != null || maxFromDeriv != null ? 'deriv' : 'configured',
    };
  }

  return [...byType.values()];
}

function extractMultipliers(contractsFor, instrument) {
  // Deriv may expose multipliers under `contracts_for.multipliers` or within
  // the contract definition. We look in both places defensively.
  const out = new Set();
  const push = (v) => {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) out.add(n);
  };

  if (contractsFor && Array.isArray(contractsFor.multipliers)) {
    for (const m of contractsFor.multipliers) push(m);
  }
  if (instrument && Array.isArray(instrument.multipliers)) {
    for (const m of instrument.multipliers) push(m);
  }
  return [...out].sort((a, b) => a - b);
}

function isMultiplierContract(type) {
  const t = String(type).toUpperCase();
  return t === 'MULTUP' || t === 'MULTDOWN';
}

function numberOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

function sanitizeClientOrderId(v) {
  if (typeof v !== 'string') return null;
  const s = v.trim();
  // 8-64 chars, URL-safe. The client generates UUIDv4 normally.
  if (!/^[A-Za-z0-9_-]{8,64}$/.test(s)) return null;
  return s;
}

function sanitizeDerivPayload(payload) {
  // Strip secrets if Deriv ever echoes them back (it should not, but defense).
  if (!payload || typeof payload !== 'object') return payload;
  const out = Array.isArray(payload) ? [] : {};
  for (const [k, v] of Object.entries(payload)) {
    if (/token|password|secret|authorization/i.test(k)) continue;
    out[k] = (v && typeof v === 'object') ? sanitizeDerivPayload(v) : v;
  }
  return out;
}

function orderError(code, message, extra) {
  const err = new Error(message || 'Order error');
  err.name = 'OrderError';
  err.code = code || 'ORDER_ERROR';
  if (extra && extra.requestId) err.requestId = extra.requestId;
  return err;
}

function toPublicOrder(doc) {
  if (!doc) return null;
  return {
    clientOrderId: doc.clientOrderId,
    symbol: doc.symbol,
    contractType: doc.contractType,
    stake: doc.stake,
    currency: doc.currency,
    multiplier: doc.multiplier,
    duration: doc.duration,
    durationUnit: doc.durationUnit,
    barrier: doc.barrier,
    state: doc.state,
    contractId: doc.contractId,
    derivRequestId: doc.derivRequestId,
    failure: doc.failure || null,
    submittedAt: doc.submittedAt,
    acceptedAt: doc.acceptedAt,
    closedAt: doc.closedAt,
    createdAt: doc.createdAt,
    updatedAt: doc.updatedAt,
  };
}

/* ------------------------------------------------------------------ */
/* singleton                                                           */
/* ------------------------------------------------------------------ */

const trading = new OrderService();

module.exports = trading;
module.exports.OrderService = OrderService;
module.exports.normalizeCapabilities = normalizeCapabilities;
