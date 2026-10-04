'use strict';

/**
 * Ray Trading Hub — central configuration.
 *
 * Rules:
 *  - This is the ONLY module allowed to read process.env directly.
 *  - Every other module imports `config` from here.
 *  - Required values fail fast at boot. Optional values have sane defaults.
 *  - No operational constant is hard-coded anywhere else in the codebase.
 *  - Secrets never leave the server. They are never serialized to the client.
 */

require('dotenv').config();

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

const errors = [];

function required(key, { min = 0, max = Infinity } = {}) {
  const raw = process.env[key];
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    errors.push(`Missing required environment variable: ${key}`);
    return null;
  }
  const v = String(raw).trim();
  if (v.length < min) {
    errors.push(`${key} is shorter than minimum length ${min}`);
  }
  if (v.length > max) {
    errors.push(`${key} is longer than maximum length ${max}`);
  }
  return v;
}

function optional(key, fallback) {
  const raw = process.env[key];
  if (raw === undefined || raw === null || String(raw).trim() === '') return fallback;
  return String(raw).trim();
}

function int(key, fallback, { min = -Infinity, max = Infinity } = {}) {
  const raw = process.env[key];
  if (raw === undefined || raw === null || String(raw).trim() === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n)) {
    errors.push(`${key} must be an integer (got: ${raw})`);
    return fallback;
  }
  if (n < min || n > max) {
    errors.push(`${key} must be between ${min} and ${max} (got: ${n})`);
    return fallback;
  }
  return n;
}

function bool(key, fallback) {
  const raw = process.env[key];
  if (raw === undefined || raw === null || String(raw).trim() === '') return fallback;
  const v = String(raw).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(v)) return true;
  if (['0', 'false', 'no', 'off'].includes(v)) return false;
  errors.push(`${key} must be a boolean-like value (got: ${raw})`);
  return fallback;
}

function list(key, fallback = []) {
  const raw = process.env[key];
  if (raw === undefined || raw === null || String(raw).trim() === '') return fallback;
  return String(raw)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

/* ------------------------------------------------------------------ */
/* environment                                                         */
/* ------------------------------------------------------------------ */

const NODE_ENV = optional('NODE_ENV', 'development');
const isProduction = NODE_ENV === 'production';
const isDevelopment = NODE_ENV === 'development';
const isTest = NODE_ENV === 'test';

/* ------------------------------------------------------------------ */
/* server                                                              */
/* ------------------------------------------------------------------ */

const PORT = int('PORT', 3000, { min: 1, max: 65535 });
const FRONTEND_URL = optional('FRONTEND_URL', '');
const TRUST_PROXY = bool('TRUST_PROXY', isProduction);

/* ------------------------------------------------------------------ */
/* database                                                            */
/* ------------------------------------------------------------------ */

const MONGODB_URI = required('MONGODB_URI', { min: 10 });

/* ------------------------------------------------------------------ */
/* auth / session                                                      */
/* ------------------------------------------------------------------ */

const SESSION_SECRET = required('SESSION_SECRET', { min: 32, max: 512 });
const OPERATOR_PASSWORD = required('OPERATOR_PASSWORD', { min: 8, max: 256 });
const SESSION_TTL_MS = int('SESSION_TTL_MS', 12 * 60 * 60 * 1000, { min: 60_000, max: 30 * 24 * 60 * 60 * 1000 });

/* ------------------------------------------------------------------ */
/* Deriv                                                               */
/* ------------------------------------------------------------------ */

const DERIV_APP_ID = required('DERIV_APP_ID', { min: 1, max: 64 });
const DERIV_API_TOKEN = required('DERIV_API_TOKEN', { min: 8, max: 512 });
const DERIV_ENDPOINT = optional('DERIV_ENDPOINT', 'wss://ws.derivws.com/websockets/v3');
const DERIV_ACCOUNT_MODE = (() => {
  const v = optional('DERIV_ACCOUNT_MODE', 'demo').toLowerCase();
  if (v !== 'demo' && v !== 'real') {
    errors.push(`DERIV_ACCOUNT_MODE must be "demo" or "real" (got: ${v})`);
    return 'demo';
  }
  return v;
})();

const DERIV_HEARTBEAT_MS = int('DERIV_HEARTBEAT_MS', 30_000, { min: 5_000, max: 120_000 });
const DERIV_REQUEST_TIMEOUT_MS = int('DERIV_REQUEST_TIMEOUT_MS', 20_000, { min: 2_000, max: 120_000 });
const DERIV_MAX_RECONNECT_DELAY_MS = int('DERIV_MAX_RECONNECT_DELAY_MS', 30_000, { min: 1_000, max: 300_000 });
const DERIV_LOG_TICKS = bool('DERIV_LOG_TICKS', false);

/* ------------------------------------------------------------------ */
/* market / watchlist                                                  */
/* ------------------------------------------------------------------ */

// Symbols are configurable. The architecture never hard-codes instruments.
// These defaults are Deriv's standard forex majors; override via env.
const WATCHLIST = list('WATCHLIST', ['frxEURUSD', 'frxGBPUSD', 'frxUSDJPY', 'frxAUDUSD']);

// Internal symbol normalization table. Different Deriv representations of the
// same instrument must collapse to one internal symbol so we never create
// duplicate instruments. Extend via env as needed.
const SYMBOL_ALIASES = {
  frxEURUSD: 'frxEURUSD',
  EURUSD: 'frxEURUSD',
  frxGBPUSD: 'frxGBPUSD',
  GBPUSD: 'frxGBPUSD',
  frxUSDJPY: 'frxUSDJPY',
  USDJPY: 'frxUSDJPY',
  frxAUDUSD: 'frxAUDUSD',
  AUDUSD: 'frxAUDUSD',
};

const CANDLE_MAX_HISTORY = int('CANDLE_MAX_HISTORY', 5000, { min: 100, max: 100_000 });
const CANDLE_PERSIST_CLOSED = bool('CANDLE_PERSIST_CLOSED', true);

/* ------------------------------------------------------------------ */
/* trading                                                             */
/* ------------------------------------------------------------------ */

const ORDER_MAX_STAKE = Number(optional('ORDER_MAX_STAKE', '10000'));
const ORDER_MIN_STAKE = Number(optional('ORDER_MIN_STAKE', '0.35'));
const IDEMPOTENCY_TTL_MS = int('IDEMPOTENCY_TTL_MS', 10 * 60 * 1000, { min: 30_000, max: 24 * 60 * 60 * 1000 });

if (!Number.isFinite(ORDER_MAX_STAKE) || ORDER_MAX_STAKE <= 0) {
  errors.push(`ORDER_MAX_STAKE must be a positive number (got: ${process.env.ORDER_MAX_STAKE})`);
}
if (!Number.isFinite(ORDER_MIN_STAKE) || ORDER_MIN_STAKE <= 0) {
  errors.push(`ORDER_MIN_STAKE must be a positive number (got: ${process.env.ORDER_MIN_STAKE})`);
}
if (Number.isFinite(ORDER_MAX_STAKE) && Number.isFinite(ORDER_MIN_STAKE) && ORDER_MIN_STAKE > ORDER_MAX_STAKE) {
  errors.push('ORDER_MIN_STAKE cannot exceed ORDER_MAX_STAKE');
}

/* ------------------------------------------------------------------ */
/* logging / rate limiting                                             */
/* ------------------------------------------------------------------ */

const LOG_LEVEL = (() => {
  const v = optional('LOG_LEVEL', isProduction ? 'info' : 'debug').toLowerCase();
  const allowed = ['debug', 'info', 'warn', 'error'];
  if (!allowed.includes(v)) {
    errors.push(`LOG_LEVEL must be one of ${allowed.join(', ')} (got: ${v})`);
    return 'info';
  }
  return v;
})();

const RATE_LIMIT_AUTH_WINDOW_MS = int('RATE_LIMIT_AUTH_WINDOW_MS', 15 * 60 * 1000, { min: 1_000 });
const RATE_LIMIT_AUTH_MAX = int('RATE_LIMIT_AUTH_MAX', 10, { min: 1 });
const RATE_LIMIT_ORDER_WINDOW_MS = int('RATE_LIMIT_ORDER_WINDOW_MS', 60 * 1000, { min: 1_000 });
const RATE_LIMIT_ORDER_MAX = int('RATE_LIMIT_ORDER_MAX', 30, { min: 1 });

/* ------------------------------------------------------------------ */
/* audit / persistence policy                                          */
/* ------------------------------------------------------------------ */

// Transient events (tick, candle.updated, heartbeat) are NEVER persisted.
// Only events listed here can be written to MongoDB.
const PERSISTED_EVENTS = list('PERSISTED_EVENTS', [
  'order.created',
  'order.submitted',
  'order.accepted',
  'order.rejected',
  'order.failed',
  'contract.opened',
  'contract.closed',
  'trade.completed',
  'account.transaction',
  'deriv.error',
  'system.error',
]);

/* ------------------------------------------------------------------ */
/* fail fast                                                           */
/* ------------------------------------------------------------------ */

if (errors.length) {
  const banner = '='.repeat(72);
  // eslint-disable-next-line no-console
  console.error(`\n${banner}\nRAY TRADING HUB — CONFIGURATION ERROR\n${banner}`);
  for (const e of errors) {
    // eslint-disable-next-line no-console
    console.error(`  • ${e}`);
  }
  // eslint-disable-next-line no-console
  console.error(`${banner}\nFix your .env (see .env.example) and restart.\n`);
  process.exit(1);
}

/* ------------------------------------------------------------------ */
/* frozen config object                                                */
/* ------------------------------------------------------------------ */

const config = Object.freeze({
  env: NODE_ENV,
  isProduction,
  isDevelopment,
  isTest,

  server: Object.freeze({
    port: PORT,
    frontendUrl: FRONTEND_URL,
    trustProxy: TRUST_PROXY,
  }),

  database: Object.freeze({
    uri: MONGODB_URI,
  }),

  auth: Object.freeze({
    sessionSecret: SESSION_SECRET,
    operatorPassword: OPERATOR_PASSWORD,
    sessionTtlMs: SESSION_TTL_MS,
    cookieName: 'rth_session',
  }),

  deriv: Object.freeze({
    appId: DERIV_APP_ID,
    apiToken: DERIV_API_TOKEN,
    endpoint: DERIV_ENDPOINT,
    accountMode: DERIV_ACCOUNT_MODE,
    heartbeatMs: DERIV_HEARTBEAT_MS,
    requestTimeoutMs: DERIV_REQUEST_TIMEOUT_MS,
    maxReconnectDelayMs: DERIV_MAX_RECONNECT_DELAY_MS,
    logTicks: DERIV_LOG_TICKS,
  }),

  market: Object.freeze({
    watchlist: Object.freeze([...WATCHLIST]),
    symbolAliases: Object.freeze({ ...SYMBOL_ALIASES }),
    candleMaxHistory: CANDLE_MAX_HISTORY,
    candlePersistClosed: CANDLE_PERSIST_CLOSED,
  }),

  trading: Object.freeze({
    minStake: ORDER_MIN_STAKE,
    maxStake: ORDER_MAX_STAKE,
    idempotencyTtlMs: IDEMPOTENCY_TTL_MS,
  }),

  logging: Object.freeze({
    level: LOG_LEVEL,
  }),

  rateLimit: Object.freeze({
    authWindowMs: RATE_LIMIT_AUTH_WINDOW_MS,
    authMax: RATE_LIMIT_AUTH_MAX,
    orderWindowMs: RATE_LIMIT_ORDER_WINDOW_MS,
    orderMax: RATE_LIMIT_ORDER_MAX,
  }),

  persistence: Object.freeze({
    persistedEvents: Object.freeze([...PERSISTED_EVENTS]),
  }),
});

/**
 * Normalize an external symbol (as returned by Deriv or entered by the user)
 * to its internal canonical form. Prevents duplicate instruments.
 */
function normalizeSymbol(raw) {
  if (!raw) return null;
  const key = String(raw).trim();
  return config.market.symbolAliases[key] || key;
}

/**
 * Safe subset of config that may be sent to the browser.
 * Secrets are NEVER included.
 */
function publicConfig() {
  return {
    env: config.env,
    accountMode: config.deriv.accountMode,
    watchlist: config.market.watchlist,
    minStake: config.trading.minStake,
    maxStake: config.trading.maxStake,
  };
}

module.exports = config;
module.exports.normalizeSymbol = normalizeSymbol;
module.exports.publicConfig = publicConfig;
