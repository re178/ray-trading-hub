'use strict';

/**
 * Ray Trading Hub — structured logger.
 *
 * Design rules:
 *  - No external dependencies. Uses only `process.stdout` / `process.stderr`.
 *  - Two output shapes:
 *      development → human-readable, aligned, colored by level
 *      production  → one JSON object per line (log-aggregator friendly)
 *  - Every log record carries: timestamp, level, module, event, message, and
 *    any structured context fields (requestId, userId, symbol, code, ...).
 *  - Sensitive values are REDACTED at the boundary. A Deriv token can never
 *    appear in a log line even if a caller accidentally passes one.
 *  - Child loggers (`logger.with({ requestId })`) let modules bind context
 *    once and get it merged into every subsequent record.
 *  - Errors are serialized into { name, message, code, stack } — stack only
 *    in development. Production logs never expose stack traces to disk.
 *  - Hot-path protection: `logger.tick(...)` exists so market-tick logging
 *    can be globally disabled without callers having to check the flag.
 *
 * This module has NO dependency on eventBus, config side-effects, or the
 * Deriv gateway. It is a leaf utility and can be required from anywhere.
 */

const config = require('./config');

/* ------------------------------------------------------------------ */
/* levels                                                              */
/* ------------------------------------------------------------------ */

const LEVELS = Object.freeze({
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
});

const LEVEL_LABEL = Object.freeze({
  10: 'DEBUG',
  20: 'INFO ',
  30: 'WARN ',
  40: 'ERROR',
});

const ACTIVE_LEVEL = LEVELS[config.logging.level] ?? LEVELS.info;

/* ------------------------------------------------------------------ */
/* redaction                                                           */
/* ------------------------------------------------------------------ */

// Case-insensitive key names whose values are never logged.
const REDACT_KEYS = new Set([
  'password',
  'passwd',
  'pwd',
  'token',
  'apitoken',
  'api_token',
  'apikey',
  'api_key',
  'authorization',
  'auth',
  'cookie',
  'set-cookie',
  'sessionsecret',
  'session_secret',
  'secret',
  'derivtoken',
  'deriv_token',
  'derivapitoken',
  'deriv_api_token',
  'privatekey',
  'private_key',
  'access_token',
  'refresh_token',
  'bearer',
]);

const REDACTED = '[REDACTED]';
const MAX_DEPTH = 6;
const MAX_ARRAY = 50;
const MAX_STRING = 4096;

function isPlainObject(v) {
  return v !== null && typeof v === 'object' && (v.constructor === Object || Object.getPrototypeOf(v) === null);
}

function isKeySensitive(key) {
  if (typeof key !== 'string') return false;
  return REDACT_KEYS.has(key.toLowerCase());
}

/**
 * Recursively walk a value, redacting sensitive keys and truncating
 * runaway strings/arrays so a single log call cannot blow up memory
 * or spill a token into the log sink.
 */
function redact(value, depth = 0, seen = new WeakSet()) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') {
    return value.length > MAX_STRING ? value.slice(0, MAX_STRING) + '…[truncated]' : value;
  }
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return value;
  if (typeof value === 'function') return '[Function]';
  if (typeof value === 'symbol') return String(value);

  if (typeof value === 'object') {
    if (seen.has(value)) return '[Circular]';
    seen.add(value);

    if (depth >= MAX_DEPTH) return '[MaxDepth]';

    if (Array.isArray(value)) {
      const out = value.slice(0, MAX_ARRAY).map((v) => redact(v, depth + 1, seen));
      if (value.length > MAX_ARRAY) out.push(`…[+${value.length - MAX_ARRAY} more]`);
      return out;
    }

    if (value instanceof Date) return value.toISOString();
    if (value instanceof Error) return serializeError(value);

    if (isPlainObject(value) || value.constructor) {
      const out = {};
      for (const k of Object.keys(value)) {
        if (isKeySensitive(k)) { out[k] = REDACTED; continue; }
        try {
          out[k] = redact(value[k], depth + 1, seen);
        } catch (_) {
          out[k] = '[Unserializable]';
        }
      }
      return out;
    }
  }

  return String(value);
}

function serializeError(err) {
  if (!err || typeof err !== 'object') return { message: String(err) };
  const out = {
    name: err.name || 'Error',
    message: err.message || String(err),
  };
  if (err.code !== undefined) out.code = err.code;
  if (err.statusCode !== undefined) out.statusCode = err.statusCode;
  if (err.requestId !== undefined) out.requestId = err.requestId;
  // Stack traces are developer diagnostics only.
  if (!config.isProduction && typeof err.stack === 'string') {
    out.stack = err.stack;
  }
  if (err.cause) out.cause = serializeError(err.cause);
  return out;
}

/* ------------------------------------------------------------------ */
/* formatting                                                          */
/* ------------------------------------------------------------------ */

const COLORS = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  debug: '\x1b[36m', // cyan
  info: '\x1b[32m',  // green
  warn: '\x1b[33m',  // yellow
  error: '\x1b[31m', // red
};

function formatDev(record) {
  const { timestamp, level, module: mod, event, message, ...rest } = record;
  const color = COLORS[level.toLowerCase()] || COLORS.reset;
  const time = timestamp.slice(11, 23); // HH:MM:SS.mmm
  const label = LEVEL_LABEL[LEVELS[level]] || level.toUpperCase();

  const ctxParts = [];
  if (mod) ctxParts.push(`${COLORS.dim}${mod}${COLORS.reset}`);
  if (event) ctxParts.push(`${COLORS.dim}${event}${COLORS.reset}`);

  // Extra context keys (requestId, symbol, code, error, ...)
  const extras = [];
  for (const [k, v] of Object.entries(rest)) {
    if (v === undefined) continue;
    if (typeof v === 'object') {
      extras.push(`${COLORS.dim}${k}=${COLORS.reset}${JSON.stringify(v)}`);
    } else {
      extras.push(`${COLORS.dim}${k}=${COLORS.reset}${v}`);
    }
  }

  const head = `${COLORS.dim}${time}${COLORS.reset} ${color}${label}${COLORS.reset}`;
  const ctx = ctxParts.length ? ` ${ctxParts.join(' ')}` : '';
  const body = message ? ` ${message}` : '';
  const tail = extras.length ? `  ${extras.join(' ')}` : '';

  return `${head}${ctx}${body}${tail}`;
}

function formatProd(record) {
  return JSON.stringify(record);
}

/* ------------------------------------------------------------------ */
/* logger                                                              */
/* ------------------------------------------------------------------ */

class Logger {
  /**
   * @param {string} moduleName  short module tag, e.g. 'derivGateway'
   * @param {object} [baseContext]  context merged into every record
   */
  constructor(moduleName = 'app', baseContext = {}) {
    this.moduleName = moduleName;
    this.baseContext = baseContext;
  }

  /**
   * Create a child logger with additional bound context.
   *   const log = logger.with({ requestId: 'REQ-1' });
   *   log.info('order.accepted', 'accepted by Deriv');
   */
  with(context = {}) {
    return new Logger(this.moduleName, { ...this.baseContext, ...context });
  }

  /** Rename the module tag (rarely needed). */
  module(name) {
    return new Logger(name, this.baseContext);
  }

  /* -------------------------------------------------------------- */

  debug(event, message, context) { this._write('debug', event, message, context); }
  info(event, message, context)  { this._write('info',  event, message, context); }
  warn(event, message, context)  { this._write('warn',  event, message, context); }
  error(event, message, context) { this._write('error', event, message, context); }

  /**
   * Hot-path tick logger. Honours `config.deriv.logTicks` globally so a
   * misbehaving module cannot flood production logs by mistake.
   */
  tick(event, message, context) {
    if (!config.deriv.logTicks) return;
    this._write('debug', event, message, context);
  }

  /** Log an Error instance with a structured event tag. */
  exception(event, err, context = {}) {
    const serialized = serializeError(err);
    this._write('error', event, serialized.message, {
      ...context,
      error: serialized,
    });
  }

  /* -------------------------------------------------------------- */

  _write(level, event, message, context) {
    if (LEVELS[level] < ACTIVE_LEVEL) return;

    // Normalise argument shapes:
    //   log.info('event', 'message', { ...context })
    //   log.info('event', { ...context })
    //   log.info('event')
    if (message && typeof message === 'object' && context === undefined) {
      context = message;
      message = undefined;
    }

    const record = {
      timestamp: new Date().toISOString(),
      level,
      module: this.moduleName,
      ...(event ? { event } : {}),
      ...(message !== undefined && message !== null ? { message: String(message) } : {}),
      ...redact(this.baseContext),
      ...redact(context || {}),
    };

    const line = config.isProduction ? formatProd(record) : formatDev(record);

    // Warnings and errors go to stderr so they can be filtered by log sinks.
    if (level === 'warn' || level === 'error') {
      process.stderr.write(line + '\n');
    } else {
      process.stdout.write(line + '\n');
    }
  }
}

/* ------------------------------------------------------------------ */
/* singleton                                                           */
/* ------------------------------------------------------------------ */

const root = new Logger('app');

module.exports = root;
module.exports.Logger = Logger;
module.exports.serializeError = serializeError;
module.exports.redact = redact;
