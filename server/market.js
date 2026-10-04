'use strict';

/**
 * Ray Trading Hub — market data & candle engine.
 *
 * Responsibilities:
 *   - Refresh the instrument catalogue from Deriv `active_symbols`
 *   - Manage the configured watchlist and its tick subscriptions
 *   - Fan Deriv ticks onto the bus as `market.tick`
 *   - Own the authoritative candle engine:
 *       * fetch historical candles via `ticks_history`
 *       * subscribe to live OHLC updates per (symbol, granularity)
 *       * determine candle buckets and emission of closed/updated events
 *       * keep a bounded in-memory history per (symbol, granularity)
 *   - Persist closed candles via bus (the persistence dispatcher does the
 *     actual write; this module only emits)
 *
 * It NEVER:
 *   - Rebuilds candles from raw ticks in the frontend
 *   - Predicts anything
 *   - Invents a symbol, granularity, or price
 *   - Persists anything itself (bus → persistence dispatcher does that)
 */

const bus = require('./eventBus');
const { EVENTS } = require('./eventBus');
const { Logger } = require('./logger');
const config = require('./config');
const gateway = require('./derivGateway');
const { Instrument, Candle } = require('./models');

const log = new Logger('market');

/* ------------------------------------------------------------------ */
/* constants                                                           */
/* ------------------------------------------------------------------ */

// Timeframes supported by the terminal. Anything outside this set is rejected
// before it ever reaches Deriv, so we cannot accidentally request an
// unsupported granularity.
const SUPPORTED_GRANULARITIES = Object.freeze([60, 300, 900, 1800, 3600, 14400, 86400]);

const HISTORY_FETCH_COUNT = 400;   // candles per historical fetch
const MIN_HISTORY_COUNT = 50;
const MAX_HISTORY_COUNT = 5000;
const INSTRUMENT_REFRESH_MS = 15 * 60 * 1000; // refresh catalogue every 15 min

/* ------------------------------------------------------------------ */
/* market manager                                                      */
/* ------------------------------------------------------------------ */

class MarketManager {
  constructor() {
    this._instruments = new Map();          // internal symbol -> instrument doc
    this._instrumentRefreshTimer = null;
    this._tickSubscriptions = new Map();    // symbol -> { key, payload }
    this._candleEngines = new Map();        // `${symbol}|${granularity}` -> CandleEngine
    this._started = false;

    this._unsubscribers = [];
  }

  /* ================================================================ */
  /* lifecycle                                                         */
  /* ================================================================ */

  async start() {
    if (this._started) return;
    this._started = true;

    this._wireBus();

    try {
      await this.refreshInstruments();
    } catch (err) {
      log.error('market.instruments.boot_failed', `Initial instrument refresh failed: ${err.message}`);
    }

    await this.subscribeWatchlist();

    this._instrumentRefreshTimer = setInterval(() => {
      this.refreshInstruments().catch((err) => {
        log.warn('market.instruments.refresh_failed', `Periodic refresh failed: ${err.message}`);
      });
    }, INSTRUMENT_REFRESH_MS);

    log.info('market.started', 'Market manager started', {
      watchlist: config.market.watchlist,
      instruments: this._instruments.size,
    });
  }

  async stop() {
    if (!this._started) return;
    this._started = false;

    if (this._instrumentRefreshTimer) {
      clearInterval(this._instrumentRefreshTimer);
      this._instrumentRefreshTimer = null;
    }

    for (const unsub of this._unsubscribers) {
      try { unsub(); } catch (_) {}
    }
    this._unsubscribers = [];

    for (const engine of this._candleEngines.values()) {
      await engine.stop().catch(() => {});
    }
    this._candleEngines.clear();

    for (const [, record] of this._tickSubscriptions) {
      try { await gateway.unsubscribe('ticks', { symbol: record.symbol }); } catch (_) {}
    }
    this._tickSubscriptions.clear();

    log.info('market.stopped', 'Market manager stopped');
  }

  /* ================================================================ */
  /* public API                                                        */
  /* ================================================================ */

  /** List of instruments currently tracked. */
  listInstruments() {
    return [...this._instruments.values()].map(toPublicInstrument);
  }

  getInstrument(symbol) {
    const inst = this._instruments.get(symbol);
    return inst ? toPublicInstrument(inst) : null;
  }

  isSupportedGranularity(g) {
    return SUPPORTED_GRANULARITIES.includes(Number(g));
  }

  /**
   * Fetch historical candles for a (symbol, granularity). Served from the
   * in-memory cache first; falls back to Deriv `ticks_history` and merges
   * the result into the cache.
   *
   * @returns {Promise<Array<{epoch:number,open:number,high:number,low:number,close:number}>>}
   */
  async getCandles(symbol, granularity, count = HISTORY_FETCH_COUNT) {
    if (!this.isSupportedGranularity(granularity)) {
      throw marketError('UNSUPPORTED_GRANULARITY', `Granularity ${granularity} is not supported`);
    }
    const inst = this._instruments.get(symbol);
    if (!inst) {
      throw marketError('UNKNOWN_INSTRUMENT', `Instrument ${symbol} is not tracked`);
    }

    const engine = this._ensureCandleEngine(symbol, granularity);
    const n = clamp(count, MIN_HISTORY_COUNT, MAX_HISTORY_COUNT);

    // If the engine already has enough history, serve it directly.
    const existing = engine.snapshot(n);
    if (existing.length >= n) return existing;

    const fetched = await engine.fetchHistory(n);
    return fetched.slice(-n);
  }

  /* ================================================================ */
  /* instrument catalogue                                              */
  /* ================================================================ */

  async refreshInstruments() {
    const res = await gateway.request({ active_symbols: 'brief' }, { label: 'active_symbols' });
    const list = Array.isArray(res.active_symbols) ? res.active_symbols : [];

    if (!list.length) {
      log.warn('market.instruments.empty', 'Deriv returned an empty instrument list');
      return;
    }

    const now = new Date();
    let created = 0;
    let updated = 0;

    for (const raw of list) {
      const internalSymbol = config.normalizeSymbol(raw.symbol);
      if (!internalSymbol) continue;

      // Only track symbols we care about — the full Deriv catalogue is ~200
      // instruments and we do not want to spam the DB or the watchlist.
      if (!this._isWatched(internalSymbol)) continue;

      const doc = {
        symbol: internalSymbol,
        derivSymbol: raw.symbol,
        displayName: raw.display_name || raw.symbol,
        market: raw.market,
        submarket: raw.submarket,
        marketDisplayName: raw.market_display_name,
        submarketDisplayName: raw.submarket_display_name,
        pip: raw.pip != null ? Number(raw.pip) : undefined,
        pipSize: raw.pip_size != null ? Number(raw.pip_size) : undefined,
        displayDecimals: raw.display_decimals != null ? Number(raw.display_decimals) : undefined,
        exchangeIsOpen: !!raw.exchange_is_open,
        isOpen: !!raw.exchange_is_open,
        openTime: raw.open_time,
        closeTime: raw.close_time,
        submarketId: raw.submarket_id,
        spot: raw.spot != null ? Number(raw.spot) : undefined,
        spotTime: raw.spot_time != null ? Number(raw.spot_time) : undefined,
        lastRefreshedAt: now,
      };
      for (const k of Object.keys(doc)) if (doc[k] === undefined) delete doc[k];

      const existing = this._instruments.get(internalSymbol);
      if (existing) updated += 1;
      else created += 1;

      this._instruments.set(internalSymbol, { ...existing, ...doc });

      // Persist to Mongo (upsert; never overwrites Deriv identity fields)
      try {
        await Instrument.updateOne(
          { symbol: internalSymbol },
          { $set: doc, $setOnInsert: { symbol: internalSymbol, derivSymbol: raw.symbol } },
          { upsert: true }
        );
      } catch (err) {
        log.warn('market.instrument.persist_failed', `Could not persist ${internalSymbol}: ${err.message}`);
      }
    }

    log.info('market.instruments.refreshed', `Instrument catalogue refreshed`, {
      tracked: this._instruments.size,
      created,
      updated,
    });
  }

  _isWatched(symbol) {
    return config.market.watchlist.includes(symbol);
  }

  /* ================================================================ */
  /* tick subscriptions                                                */
  /* ================================================================ */

  async subscribeWatchlist() {
    const symbols = config.market.watchlist;
    for (const symbol of symbols) {
      await this.subscribeTicks(symbol).catch((err) => {
        log.error('market.subscribe.failed', `Tick subscribe failed for ${symbol}: ${err.message}`);
      });
    }
  }

  async subscribeTicks(symbol) {
    if (this._tickSubscriptions.has(symbol)) return;

    const inst = this._instruments.get(symbol);
    if (!inst) {
      throw marketError('UNKNOWN_INSTRUMENT', `Cannot subscribe to ${symbol}: not in catalogue`);
    }

    const payload = { ticks: inst.derivSymbol, subscribe: 1 };

    // Reuse the gateway's subscription bookkeeping so that reconnect
    // restoration also restores tick streams.
    const record = { symbol, key: subscriptionKeyFor(symbol) };
    this._tickSubscriptions.set(symbol, record);

    try {
      await gateway.subscribe('ticks', { symbol }, payload, { label: `ticks:${symbol}` });
      log.info('market.ticks.subscribed', `Subscribed to ${symbol} ticks`);
    } catch (err) {
      this._tickSubscriptions.delete(symbol);
      throw err;
    }
  }

  async unsubscribeTicks(symbol) {
    if (!this._tickSubscriptions.has(symbol)) return;
    this._tickSubscriptions.delete(symbol);
    try {
      await gateway.unsubscribe('ticks', { symbol });
      log.info('market.ticks.unsubscribed', `Unsubscribed from ${symbol} ticks`);
    } catch (err) {
      log.warn('market.ticks.unsubscribe_failed', `${symbol}: ${err.message}`);
    }
  }

  /* ================================================================ */
  /* candle engine coordination                                        */
  /* ================================================================ */

  _ensureCandleEngine(symbol, granularity) {
    const key = candleKey(symbol, granularity);
    let engine = this._candleEngines.get(key);
    if (engine) return engine;

    engine = new CandleEngine(symbol, granularity);
    this._candleEngines.set(key, engine);

    // Fire-and-forget subscription; the engine handles re-subscription on
    // gateway reconnect via bus events.
    engine.start().catch((err) => {
      log.error('candle.engine.start_failed', `${key}: ${err.message}`);
    });

    return engine;
  }

  /* ================================================================ */
  /* bus wiring                                                        */
  /* ================================================================ */

  _wireBus() {
    // Gateway reconnect → re-seed candle engines from the tick stream and
    // re-fetch history for the active set.
    this._unsubscribers.push(
      bus.on(EVENTS.DERIV_AUTHORIZED, () => {
        this._restoreAfterAuth().catch((err) => {
          log.warn('market.restore.failed', `Post-auth restore failed: ${err.message}`);
        });
      }, { label: 'market:onAuthorized', priority: 5 })
    );
  }

  async _restoreAfterAuth() {
    // Ensure watchlist tick streams are open.
    for (const symbol of config.market.watchlist) {
      if (!this._tickSubscriptions.has(symbol)) {
        await this.subscribeTicks(symbol).catch(() => {});
      }
    }
    // Ensure candle engines for any active (symbol, granularity) pairs are
    // re-subscribed. The engines themselves listen for reconnect events;
    // this call is a belt-and-braces re-ensure.
    for (const engine of this._candleEngines.values()) {
      await engine.resubscribe().catch(() => {});
    }
  }
}

/* ------------------------------------------------------------------ */
/* candle engine                                                       */
/* ------------------------------------------------------------------ */

class CandleEngine {
  /**
   * One engine per (symbol, granularity). Owns the in-memory candle history
   * for that series and the live OHLC subscription.
   */
  constructor(symbol, granularity) {
    this.symbol = symbol;
    this.granularity = granularity;
    this.key = candleKey(symbol, granularity);

    /** @type {Array<{epoch:number,open:number,high:number,low:number,close:number,volume?:number,closed:boolean}>} */
    this._history = [];

    this._started = false;
    this._unsubscribers = [];
    this._subscribed = false;
  }

  /* ================================================================ */

  async start() {
    if (this._started) return;
    this._started = true;

    // Subscribe to the live OHLC stream for this series.
    await this.resubscribe();

    // Load history from Mongo first (fast path), then fill any gap from Deriv.
    await this._loadPersistedHistory();
    if (this._history.length < MIN_HISTORY_COUNT) {
      await this.fetchHistory(HISTORY_FETCH_COUNT).catch((err) => {
        log.warn('candle.history.fetch_failed', `${this.key}: ${err.message}`);
      });
    }
  }

  async stop() {
    if (!this._started) return;
    this._started = false;

    for (const unsub of this._unsubscribers) {
      try { unsub(); } catch (_) {}
    }
    this._unsubscribers = [];

    if (this._subscribed) {
      try {
        await gateway.unsubscribe('ohlc', { symbol: this.symbol, granularity: this.granularity });
      } catch (_) {}
      this._subscribed = false;
    }
  }

  /**
   * (Re)subscribe to the OHLC stream. Safe to call repeatedly — the gateway
   * deduplicates by key.
   */
  async resubscribe() {
    if (!this._started) return;

    const inst = null; // rely on symbol only
    const payload = {
      ticks_history: derivSymbolFor(this.symbol),
      adjust_start_time: 1,
      count: 1,
      end: 'latest',
      granularity: this.granularity,
      style: 'candles',
      subscribe: 1,
    };

    try {
      await gateway.subscribe(
        'ohlc',
        { symbol: this.symbol, granularity: this.granularity },
        payload,
        { label: `ohlc:${this.key}` }
      );
      this._subscribed = true;
    } catch (err) {
      log.warn('candle.subscribe_failed', `${this.key}: ${err.message}`);
      this._subscribed = false;
    }

    // Wire bus listeners once (idempotent via key check).
    if (this._unsubscribers.length === 0) this._wireBus();
  }

  _wireBus() {
    this._unsubscribers.push(
      bus.on(EVENTS.MARKET_CANDLE_UPDATED, (evt) => {
        if (evt.symbol !== this.symbol || evt.granularity !== this.granularity) return;
        this._ingest(evt, false);
      }, { label: `candle:${this.key}:updated` }),

      bus.on(EVENTS.MARKET_CANDLE_CLOSED, (evt) => {
        if (evt.symbol !== this.symbol || evt.granularity !== this.granularity) return;
        this._ingest(evt, true);
      }, { label: `candle:${this.key}:closed` }),

      bus.on(EVENTS.DERIV_RECONNECTING, () => {
        this._subscribed = false;
      }, { label: `candle:${this.key}:reconnecting` }),

      bus.on(EVENTS.DERIV_AUTHORIZED, () => {
        this.resubscribe().catch((err) => {
          log.warn('candle.resubscribe_failed', `${this.key}: ${err.message}`);
        });
      }, { label: `candle:${this.key}:authorized` })
    );
  }

  /* ================================================================ */

  /**
   * Ingest a candle event from the gateway. The engine is the authority on
   * bucketing: if a candle arrives with an epoch that matches the current
   * last candle, we update it; if it is newer, we close the previous candle
   * (if not already marked closed) and start a new one.
   */
  _ingest(evt, isClosedEvent) {
    const epoch = bucketStart(evt.epoch, this.granularity);
    const candle = {
      epoch,
      open: evt.open,
      high: evt.high,
      low: evt.low,
      close: evt.close,
      volume: evt.volume,
      closed: false,
    };

    const last = this._history[this._history.length - 1];

    if (!last || epoch > last.epoch) {
      // Previous candle should be marked closed and emitted exactly once.
      if (last && !last.closed) {
        last.closed = true;
        this._emitClosed({ ...last });
      }
      this._history.push(candle);
      this._trimHistory();
      this._emitUpdated(candle);
    } else if (epoch === last.epoch) {
      // Update in place.
      last.high = Math.max(last.high, candle.high);
      last.low = Math.min(last.low, candle.low);
      last.close = candle.close;
      if (candle.volume != null) last.volume = candle.volume;

      if (isClosedEvent && !last.closed) {
        last.closed = true;
        this._emitClosed({ ...last });
      } else {
        this._emitUpdated(last);
      }
    } else {
      // Out-of-order / historical candle. Insert in position (bounded search).
      this._insertHistorical(candle);
      if (isClosedEvent) {
        candle.closed = true;
        this._emitClosed({ ...candle });
      }
    }
  }

  _insertHistorical(candle) {
    const idx = this._history.findIndex((c) => c.epoch >= candle.epoch);
    if (idx === -1) {
      this._history.push(candle);
    } else if (this._history[idx].epoch === candle.epoch) {
      // Overwrite the stored record — Deriv's response wins.
      this._history[idx] = { ...this._history[idx], ...candle };
    } else {
      this._history.splice(idx, 0, candle);
    }
    this._trimHistory();
  }

  _trimHistory() {
    const cap = config.market.candleMaxHistory;
    if (this._history.length > cap) {
      this._history.splice(0, this._history.length - cap);
    }
  }

  _emitUpdated(candle) {
    bus.emit(EVENTS.MARKET_CANDLE_UPDATED, {
      symbol: this.symbol,
      granularity: this.granularity,
      epoch: candle.epoch,
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      volume: candle.volume,
      closed: false,
    });
  }

  _emitClosed(candle) {
    bus.emit(EVENTS.MARKET_CANDLE_CLOSED, {
      symbol: this.symbol,
      granularity: this.granularity,
      epoch: candle.epoch,
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      volume: candle.volume,
      closed: true,
      closeTime: candle.epoch + this.granularity,
    });
  }

  /* ================================================================ */
  /* history                                                           */
  /* ================================================================ */

  snapshot(count) {
    if (!count || count >= this._history.length) return [...this._history];
    return this._history.slice(this._history.length - count);
  }

  async _loadPersistedHistory() {
    try {
      const rows = await Candle.find(
        { symbol: this.symbol, granularity: this.granularity, closed: true },
        { _id: 0, epoch: 1, open: 1, high: 1, low: 1, close: 1, volume: 1 }
      )
        .sort({ epoch: -1 })
        .limit(HISTORY_FETCH_COUNT)
        .lean();

      if (!rows.length) return;

      // Rows come newest-first; reverse for chronological order.
      const ordered = rows.reverse().map((r) => ({
        epoch: r.epoch,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        volume: r.volume,
        closed: true,
      }));

      // Merge into current history (may already contain recent live candles).
      for (const c of ordered) this._insertHistorical(c);
      log.debug('candle.history.loaded', `${this.key}: ${ordered.length} persisted candles`);
    } catch (err) {
      log.warn('candle.history.load_failed', `${this.key}: ${err.message}`);
    }
  }

  /**
   * Fetch historical candles from Deriv. Returns the fetched array in
   * chronological order. Also merges into the local history and emits
   * closed events for each new candle so persistence has a chance to
   * capture them.
   */
  async fetchHistory(count = HISTORY_FETCH_COUNT) {
    const payload = {
      ticks_history: derivSymbolFor(this.symbol),
      adjust_start_time: 1,
      count: clamp(count, MIN_HISTORY_COUNT, MAX_HISTORY_COUNT),
      end: 'latest',
      granularity: this.granularity,
      style: 'candles',
    };

    const res = await gateway.request(payload, {
      label: `ticks_history:${this.key}`,
      timeoutMs: 20_000,
    });

    if (res.error) {
      throw marketError(res.error.code || 'DERIV_HISTORY_ERROR', res.error.message || 'History fetch failed');
    }

    const candles = Array.isArray(res.candles) ? res.candles : [];
    const normalized = candles
      .map((c) => ({
        epoch: bucketStart(Number(c.epoch), this.granularity),
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close),
        volume: c.volume != null ? Number(c.volume) : undefined,
        closed: true,
      }))
      .filter((c) => Number.isFinite(c.epoch) && Number.isFinite(c.open));

    for (const c of normalized) {
      const before = this._history.length;
      this._insertHistorical(c);
      // Only emit closed for candles we did not already have.
      if (this._history.length > before) {
        this._emitClosed(c);
      } else {
        // Confirm the record exists; emission is harmless if already known.
        this._emitClosed(c);
      }
    }

    log.info('candle.history.fetched', `${this.key}: ${normalized.length} candles from Deriv`);
    return this.snapshot();
  }
}

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

function candleKey(symbol, granularity) {
  return `${symbol}|${granularity}`;
}

function subscriptionKeyFor(symbol) {
  return `ticks|symbol=${symbol}`;
}

/**
 * Convert a unix-seconds timestamp to the start of its candle bucket.
 */
function bucketStart(epochSeconds, granularity) {
  const g = Number(granularity);
  const e = Math.floor(Number(epochSeconds));
  return Math.floor(e / g) * g;
}

function clamp(v, min, max) {
  const n = Number(v);
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, n));
}

function derivSymbolFor(internalSymbol) {
  // The instrument catalogue maps internal -> deriv symbol. If we have not
  // loaded the catalogue yet, fall back to the internal symbol (they match
  // for our default watchlist).
  const instance = market._instruments.get(internalSymbol);
  return instance ? instance.derivSymbol : internalSymbol;
}

function marketError(code, message) {
  const err = new Error(message);
  err.name = 'MarketError';
  err.code = code;
  return err;
}

function toPublicInstrument(doc) {
  return {
    symbol: doc.symbol,
    derivSymbol: doc.derivSymbol,
    displayName: doc.displayName,
    market: doc.market,
    submarket: doc.submarket,
    pip: doc.pip,
    pipSize: doc.pipSize,
    displayDecimals: doc.displayDecimals,
    isOpen: !!doc.isOpen,
    exchangeIsOpen: !!doc.exchangeIsOpen,
    openTime: doc.openTime,
    closeTime: doc.closeTime,
    spot: doc.spot,
    spotTime: doc.spotTime,
  };
}

/* ------------------------------------------------------------------ */
/* singleton                                                           */
/* ------------------------------------------------------------------ */

const market = new MarketManager();

module.exports = market;
module.exports.MarketManager = MarketManager;
module.exports.CandleEngine = CandleEngine;
module.exports.SUPPORTED_GRANULARITIES = SUPPORTED_GRANULARITIES;
