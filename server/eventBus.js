'use strict';

/**
 * Ray Trading Hub — internal Event Bus.
 *
 * This is the internal communication backbone of the entire application.
 * Deriv Gateway publishes here. Market data, candle engine, order service,
 * contract monitor, audit layer, WebSocket gateway — all subscribe here.
 *
 * Design rules:
 *  - NO module reaches into another module's internals. Everything flows
 *    through the bus.
 *  - Handler errors are isolated. One bad subscriber never kills an emit.
 *  - Async handlers are supported. Rejections are captured, logged, and
 *    emitted as `system.error` (never silently swallowed).
 *  - Wildcard subscriptions (`market.*`, `order.*`) are supported so future
 *    modules can observe a domain without touching the publisher.
 *  - A bounded history of recent events is kept for debugging / the system
 *    events panel. History is intentionally small; this is a live bus, not
 *    a message log.
 *  - The bus NEVER persists anything. Persistence is the audit layer's job
 *    and it decides based on `config.persistence.persistedEvents`.
 *  - The bus NEVER sends anything to the browser. That is the WebSocket
 *    gateway's job, and it only forwards a controlled subset of events.
 */

const EventEmitter = require('events');
const config = require('./config');

/* ------------------------------------------------------------------ */
/* constants                                                           */
/* ------------------------------------------------------------------ */

// Canonical event names. Using constants prevents typo-driven silent
// failures (a subscriber on 'market.tickk' would otherwise never fire).
const EVENTS = Object.freeze({
  // market / candles
  MARKET_TICK: 'market.tick',
  MARKET_CANDLE_UPDATED: 'market.candle.updated',
  MARKET_CANDLE_CLOSED: 'market.candle.closed',

  // account
  ACCOUNT_UPDATED: 'account.updated',
  ACCOUNT_BALANCE_UPDATED: 'account.balance.updated',
  ACCOUNT_TRANSACTION: 'account.transaction',

  // orders
  ORDER_CREATED: 'order.created',
  ORDER_SUBMITTED: 'order.submitted',
  ORDER_ACCEPTED: 'order.accepted',
  ORDER_REJECTED: 'order.rejected',
  ORDER_FAILED: 'order.failed',

  // contracts
  CONTRACT_CREATED: 'contract.created',
  CONTRACT_OPENED: 'contract.opened',
  CONTRACT_UPDATED: 'contract.updated',
  CONTRACT_CLOSED: 'contract.closed',

  // positions
  POSITION_OPENED: 'position.opened',
  POSITION_UPDATED: 'position.updated',
  POSITION_CLOSED: 'position.closed',

  // trades
  TRADE_PROFIT_UPDATED: 'trade.profit.updated',
  TRADE_COMPLETED: 'trade.completed',

  // deriv gateway lifecycle
  DERIV_CONNECTED: 'deriv.connected',
  DERIV_AUTHORIZED: 'deriv.authorized',
  DERIV_DISCONNECTED: 'deriv.disconnected',
  DERIV_RECONNECTING: 'deriv.reconnecting',
  DERIV_ERROR: 'deriv.error',

  // system
  SYSTEM_STARTED: 'system.started',
  SYSTEM_STOPPED: 'system.stopped',
  SYSTEM_ERROR: 'system.error',
  SYSTEM_STATUS: 'system.status',
});

// Events that are inherently high-frequency and MUST NOT be logged per-emit
// under normal operation. Only logged when config.deriv.logTicks is true
// or LOG_LEVEL is 'debug'.
const HIGH_FREQUENCY_EVENTS = new Set([
  EVENTS.MARKET_TICK,
  EVENTS.MARKET_CANDLE_UPDATED,
  EVENTS.CONTRACT_UPDATED,
  EVENTS.POSITION_UPDATED,
  EVENTS.TRADE_PROFIT_UPDATED,
]);

// Bound for the debug ring buffer. Small on purpose.
const HISTORY_LIMIT = 200;

/* ------------------------------------------------------------------ */
/* logger — minimal, structured, level-aware                           */
/* ------------------------------------------------------------------ */

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
const ACTIVE_LEVEL = LEVELS[config.logging.level] ?? LEVELS.info;

function ts() {
  return new Date().toISOString();
}

const log = {
  debug(...args) { if (ACTIVE_LEVEL <= LEVELS.debug) console.debug(`[${ts()}] [DEBUG] [eventBus]`, ...args); },
  info(...args)  { if (ACTIVE_LEVEL <= LEVELS.info)  console.log(`[${ts()}] [INFO ] [eventBus]`, ...args); },
  warn(...args)  { if (ACTIVE_LEVEL <= LEVELS.warn)  console.warn(`[${ts()}] [WARN ] [eventBus]`, ...args); },
  error(...args) { if (ACTIVE_LEVEL <= LEVELS.error) console.error(`[${ts()}] [ERROR] [eventBus]`, ...args); },
};

/* ------------------------------------------------------------------ */
/* wildcard matching                                                   */
/* ------------------------------------------------------------------ */

/**
 * Returns true if `pattern` matches `eventName`.
 * Supports a single trailing wildcard segment: `market.*` matches
 * `market.tick` and `market.candle.closed`. `*` matches everything.
 */
function matches(pattern, eventName) {
  if (pattern === '*') return true;
  if (pattern === eventName) return true;
  if (pattern.endsWith('.*')) {
    const prefix = pattern.slice(0, -2);
    return eventName === prefix || eventName.startsWith(prefix + '.');
  }
  return false;
}

/* ------------------------------------------------------------------ */
/* bus implementation                                                  */
/* ------------------------------------------------------------------ */

class EventBus extends EventEmitter {
  constructor() {
    super();
    // Prevent Node's default "10 listeners" warning from firing for hot
    // events like market.tick that legitimately have several subscribers.
    this.setMaxListeners(100);

    // pattern -> Set<handlerRecord>  (for wildcard tracking)
    this._wildcards = new Map();

    // name -> Set<handlerRecord>     (direct subscriptions, fast path)
    this._direct = new Map();

    // Ring buffer of recent events for the system events panel / debugging.
    this._history = [];

    // Per-event emit counters. Cheap to maintain, invaluable for diagnostics.
    this._metrics = new Map();

    // Global error handler for async rejections from subscribers.
    this._onSubscriberError = null;
  }

  /**
   * Register a subscriber.
   *
   * @param {string|string[]} pattern  event name or wildcard pattern
   * @param {(data:any, meta:object)=>any} handler
   * @param {object} [opts]
   * @param {string} [opts.label]      human label for logs (e.g. 'candleEngine')
   * @param {number} [opts.priority]   higher runs first (default 0)
   * @param {boolean} [opts.once]      auto-remove after first delivery
   * @returns {() => void}             unsubscribe function
   */
  on(pattern, handler, opts = {}) {
    if (typeof handler !== 'function') {
      throw new TypeError('eventBus.on: handler must be a function');
    }
    const patterns = Array.isArray(pattern) ? pattern : [pattern];
    const record = {
      handler,
      label: opts.label || handler.name || 'anonymous',
      priority: Number.isFinite(opts.priority) ? opts.priority : 0,
      once: !!opts.once,
      patterns,
    };

    for (const p of patterns) {
      if (p.includes('*')) {
        if (!this._wildcards.has(p)) this._wildcards.set(p, new Set());
        this._wildcards.get(p).add(record);
      } else {
        if (!this._direct.has(p)) this._direct.set(p, new Set());
        this._direct.get(p).add(record);
      }
    }

    // Unsubscribe function returned to the caller.
    return () => this.off(pattern, handler);
  }

  /** Subscribe for a single delivery. */
  once(pattern, handler, opts = {}) {
    return this.on(pattern, handler, { ...opts, once: true });
  }

  /** Remove a subscriber registered with `on`/`once`. */
  off(pattern, handler) {
    const patterns = Array.isArray(pattern) ? pattern : [pattern];
    for (const p of patterns) {
      const bucket = p.includes('*') ? this._wildcards.get(p) : this._direct.get(p);
      if (!bucket) continue;
      for (const rec of bucket) {
        if (rec.handler === handler) bucket.delete(rec);
      }
      if (bucket.size === 0) {
        if (p.includes('*')) this._wildcards.delete(p);
        else this._direct.delete(p);
      }
    }
  }

  /**
   * Publish an event.
   *
   * Returns the number of handlers that were invoked. Handlers run
   * synchronously in priority order; async handlers are supported — their
   * promise rejections are captured and surfaced as `system.error`, but the
   * emit call does NOT await them (the bus must never block the gateway).
   */
  emit(eventName, data = null, meta = {}) {
    if (typeof eventName !== 'string' || !eventName) {
      throw new TypeError('eventBus.emit: eventName must be a non-empty string');
    }

    const now = Date.now();
    const enrichedMeta = {
      emittedAt: now,
      event: eventName,
      ...meta,
    };

    // Metrics
    this._metrics.set(eventName, (this._metrics.get(eventName) || 0) + 1);

    // History (bounded)
    this._history.push({
      at: now,
      event: eventName,
      // Store a shallow snapshot; deep cloning every tick would kill perf.
      // The history is for inspection, not for mutation.
      data: config.isProduction ? undefined : data,
    });
    if (this._history.length > HISTORY_LIMIT) this._history.shift();

    // Verbose logging policy
    const isHighFreq = HIGH_FREQUENCY_EVENTS.has(eventName);
    if (!isHighFreq) {
      log.debug(`emit ${eventName}`);
    } else if (config.deriv.logTicks && ACTIVE_LEVEL <= LEVELS.debug) {
      log.debug(`emit ${eventName} (hf)`);
    }

    // Collect handlers: direct + wildcard matches, deduplicated.
    const targets = new Set();
    const direct = this._direct.get(eventName);
    if (direct) for (const r of direct) targets.add(r);

    for (const [pattern, bucket] of this._wildcards) {
      if (matches(pattern, eventName)) {
        for (const r of bucket) targets.add(r);
      }
    }

    if (targets.size === 0) return 0;

    // Sort by priority (descending), then by insertion order implicitly.
    const ordered = [...targets].sort((a, b) => b.priority - a.priority);

    let invoked = 0;
    for (const rec of ordered) {
      invoked += 1;
      if (rec.once) this.off(rec.patterns, rec.handler);

      try {
        const result = rec.handler(data, enrichedMeta);
        // Async handler — capture rejection without blocking emit.
        if (result && typeof result.then === 'function') {
          result.catch((err) => {
            this._handleSubscriberError(err, rec, eventName, data);
          });
        }
      } catch (err) {
        this._handleSubscriberError(err, rec, eventName, data);
      }
    }

    return invoked;
  }

  /**
   * Emit and await all async subscribers. Use sparingly — for graceful
   * shutdown and for the order-submission path where the caller must know
   * that downstream observers have completed.
   *
   * Synchronous handlers are still invoked synchronously; async handlers
   * are awaited.
   */
  async emitAndWait(eventName, data = null, meta = {}) {
    if (typeof eventName !== 'string' || !eventName) {
      throw new TypeError('eventBus.emitAndWait: eventName must be a non-empty string');
    }

    const targets = new Set();
    const direct = this._direct.get(eventName);
    if (direct) for (const r of direct) targets.add(r);
    for (const [pattern, bucket] of this._wildcards) {
      if (matches(pattern, eventName)) {
        for (const r of bucket) targets.add(r);
      }
    }

    const ordered = [...targets].sort((a, b) => b.priority - a.priority);
    const enrichedMeta = { emittedAt: Date.now(), event: eventName, awaited: true, ...meta };

    this._metrics.set(eventName, (this._metrics.get(eventName) || 0) + 1);

    for (const rec of ordered) {
      if (rec.once) this.off(rec.patterns, rec.handler);
      try {
        const result = rec.handler(data, enrichedMeta);
        if (result && typeof result.then === 'function') {
          await result;
        }
      } catch (err) {
        this._handleSubscriberError(err, rec, eventName, data);
      }
    }
  }

  /* ---------------------------------------------------------------- */
  /* diagnostics                                                       */
  /* ---------------------------------------------------------------- */

  /** Return a snapshot of per-event emit counts. */
  metrics() {
    return Object.fromEntries(this._metrics);
  }

  /** Return a shallow copy of the recent-events ring buffer. */
  history(limit = 50) {
    const n = Math.max(1, Math.min(limit, this._history.length));
    return this._history.slice(this._history.length - n);
  }

  /** Count of currently-registered subscribers for an event or wildcard. */
  listenerCountFor(pattern) {
    if (pattern.includes('*')) {
      const bucket = this._wildcards.get(pattern);
      return bucket ? bucket.size : 0;
    }
    const bucket = this._direct.get(pattern);
    return bucket ? bucket.size : 0;
  }

  /** List all active patterns (direct + wildcard). Useful at boot. */
  patterns() {
    return {
      direct: [...this._direct.keys()],
      wildcard: [...this._wildcards.keys()],
    };
  }

  /** Remove every subscriber. Used in tests and during shutdown. */
  removeAll() {
    this._direct.clear();
    this._wildcards.clear();
    this.removeAllListeners();
  }

  /* ---------------------------------------------------------------- */
  /* internal                                                          */
  /* ---------------------------------------------------------------- */

  _handleSubscriberError(err, rec, eventName, data) {
    const message = err && err.message ? err.message : String(err);
    const stack = err && err.stack ? err.stack : '(no stack)';

    log.error(
      `subscriber failed — event=${eventName} subscriber=${rec.label} error=${message}\n${stack}`
    );

    // Surface as a system error so the diagnostics panel and any observer
    // module can react. Guard against recursion: never re-emit system.error
    // from within the handler for system.error itself.
    if (eventName !== EVENTS.SYSTEM_ERROR) {
      try {
        this.emit(
          EVENTS.SYSTEM_ERROR,
          {
            source: 'eventBus',
            event: eventName,
            subscriber: rec.label,
            message,
            code: 'SUBSCRIBER_ERROR',
          },
          { originalEvent: eventName }
        );
      } catch (_) {
        // Nothing else we can do — do not recurse further.
      }
    }

    // Hook for tests / process-level supervisors.
    if (typeof this._onSubscriberError === 'function') {
      try { this._onSubscriberError(err, { eventName, subscriber: rec.label, data }); } catch (_) {}
    }
  }

  setSubscriberErrorHook(fn) {
    this._onSubscriberError = typeof fn === 'function' ? fn : null;
  }
}

/* ------------------------------------------------------------------ */
/* singleton export                                                    */
/* ------------------------------------------------------------------ */

const bus = new EventBus();

module.exports = bus;
module.exports.EVENTS = EVENTS;
module.exports.matches = matches;
module.exports.EventBus = EventBus;
