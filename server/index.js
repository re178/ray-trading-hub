'use strict';

/**
 * Ray Trading Hub — application entry point.
 *
 * Boot sequence (strict order):
 *   1. Config (already validated on require; will exit on error)
 *   2. Logger
 *   3. Event Bus (required by everything)
 *   4. Database connection + persistence dispatcher
 *   5. Deriv gateway start + authorization
 *   6. Market manager start (instruments, ticks, candle engines)
 *   7. Trading service start
 *   8. HTTP server (Express + REST + static)
 *   9. WebSocket gateway attach
 *
 * Graceful shutdown reverses the order and waits for each step.
 *
 * This file is the ONLY place that boots the process. It exposes nothing
 * else. All business logic lives in the modules it starts.
 */

const http = require('http');
const path = require('path');
const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');

const config = require('./config');
const { Logger } = require('./logger');
const bus = require('./eventBus');
const { EVENTS } = require('./eventBus');
const database = require('./database');
const gateway = require('./derivGateway');
const market = require('./market');
const trading = require('./trading');
const wsGateway = require('./websocket');
const session = require('./session');
const { Account, Order, Contract, Trade, Instrument, Transaction } = require('./models');
const { enums } = require('./models');

const log = new Logger('server');

/* ------------------------------------------------------------------ */
/* constants                                                           */
/* ------------------------------------------------------------------ */

const CLIENT_DIR = path.join(__dirname, '..', 'client');
const PUBLIC_DIR = path.join(__dirname, '..', 'public');

/* ------------------------------------------------------------------ */
/* application state                                                   */
/* ------------------------------------------------------------------ */

const state = {
  started: false,
  shuttingDown: false,
  startedAt: null,
  httpServer: null,
  expressApp: null,
};

/* ------------------------------------------------------------------ */
/* Express app                                                         */
/* ------------------------------------------------------------------ */

function createApp() {
  const app = express();

  // Trust proxy when behind Render's load balancer.
  if (config.server.trustProxy) app.set('trust proxy', 1);

  app.disable('x-powered-by');
  app.set('etag', false);

  // ---- security headers -----------------------------------------
  app.use(helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'"], // inline boot script in index.html
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", 'data:'],
        connectSrc: ["'self'", 'ws:', 'wss:'],
        fontSrc: ["'self'"],
        objectSrc: ["'none'"],
        frameAncestors: ["'none'"],
        baseUri: ["'self'"],
        formAction: ["'self'"],
      },
    },
    crossOriginEmbedderPolicy: false, // we use same-origin WS upgrades
    crossOriginResourcePolicy: { policy: 'same-origin' },
    referrerPolicy: { policy: 'no-referrer' },
    hsts: config.isProduction ? { maxAge: 15552000, includeSubDomains: true, preload: false } : false,
  }));

  // ---- body parsing (small, JSON only) --------------------------
  app.use(express.json({ limit: '32kb', strict: true }));
  app.use(express.urlencoded({ extended: false, limit: '32kb' }));

  // ---- request logging (dev only; production keeps structured logs) --
  if (!config.isProduction) {
    app.use((req, _res, next) => {
      log.debug('http.request', `${req.method} ${req.path}`);
      next();
    });
  }

  // ---- rate limiters --------------------------------------------
  const authLimiter = rateLimit({
    windowMs: config.rateLimit.authWindowMs,
    max: config.rateLimit.authMax,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: { code: 'RATE_LIMITED', message: 'Too many attempts. Try again later.' } },
  });

  const orderLimiter = rateLimit({
    windowMs: config.rateLimit.orderWindowMs,
    max: config.rateLimit.orderMax,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: { code: 'RATE_LIMITED', message: 'Too many orders. Slow down.' } },
  });

  // ---- auth gate -------------------------------------------------
  function requireAuth(req, res, next) {
    const sessionInfo = session.readSessionFromRequest(req);
    if (!sessionInfo) {
      return sendError(res, 401, 'NOT_AUTHENTICATED', 'Authentication required');
    }
    req.session = sessionInfo;
    next();
  }

  // ================================================================
  // AUTH
  // ================================================================

  app.post('/api/auth/login', authLimiter, (req, res) => {
    const provided = req.body && typeof req.body.password === 'string' ? req.body.password : '';
    if (!session.checkOperatorPassword(provided)) {
      log.warn('auth.login.failed', 'Invalid operator password');
      bus.emit(EVENTS.SYSTEM_ERROR, {
        source: 'auth',
        code: 'LOGIN_FAILURE',
        message: 'Invalid operator password',
      });
      return sendError(res, 401, 'INVALID_CREDENTIALS', 'Invalid password');
    }

    const issued = session.issueSession({ userId: 'operator' });
    session.attachCookieToResponse(res, issued.cookie);

    log.info('auth.login.success', 'Operator authenticated');
    bus.emit(EVENTS.SYSTEM_STATUS, { auth: { event: 'login', at: Date.now() } });

    return res.json({
      session: {
        userId: 'operator',
        expiresAt: issued.expiresAt,
      },
    });
  });

  app.post('/api/auth/logout', (req, res) => {
    const revoked = session.revokeSession();
    session.attachCookieToResponse(res, revoked.cookie);
    log.info('auth.logout', 'Operator logged out');
    return res.json({ ok: true });
  });

  app.get('/api/auth/session', requireAuth, (req, res) => {
    return res.json({
      session: {
        userId: req.session.userId,
        createdAt: req.session.iat,
        expiresAt: req.session.exp,
      },
    });
  });

  // ================================================================
  // ACCOUNT
  // ================================================================

  app.get('/api/account', requireAuth, async (_req, res) => {
    const doc = await Account.findOne({}, { _id: 0, __v: 0 }).lean();
    return res.json({ account: doc || null });
  });

  app.get('/api/account/balance', requireAuth, async (_req, res) => {
    const doc = await Account.findOne({}, { balance: 1, currency: 1, mode: 1, lastSyncedAt: 1, _id: 0 }).lean();
    return res.json({ balance: doc || null });
  });

  app.get('/api/account/summary', requireAuth, async (_req, res) => {
    try {
      const summary = await buildAccountSummary();
      return res.json({ summary });
    } catch (err) {
      log.error('api.account.summary_failed', err.message);
      return sendError(res, 500, 'SUMMARY_FAILED', 'Could not build account summary');
    }
  });

  app.get('/api/account/transactions', requireAuth, async (req, res) => {
    const limit = clampInt(req.query.limit, 1, 500, 100);
    const rows = await Transaction.find({}, { _id: 0, __v: 0 })
      .sort({ transactionTime: -1 })
      .limit(limit)
      .lean();
    return res.json({ transactions: rows });
  });

  // ================================================================
  // INSTRUMENTS
  // ================================================================

  app.get('/api/instruments', requireAuth, (_req, res) => {
    return res.json({ instruments: market.listInstruments() });
  });

  app.get('/api/instruments/:symbol', requireAuth, (req, res) => {
    const symbol = config.normalizeSymbol(req.params.symbol);
    const inst = market.getInstrument(symbol);
    if (!inst) return sendError(res, 404, 'UNKNOWN_INSTRUMENT', `Instrument ${req.params.symbol} not found`);
    return res.json({ instrument: inst });
  });

  app.get('/api/instruments/:symbol/capabilities', requireAuth, async (req, res) => {
    const symbol = config.normalizeSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, 'INVALID_SYMBOL', 'Symbol is required');
    try {
      const capabilities = await trading.getCapabilities(symbol);
      return res.json({ symbol, capabilities });
    } catch (err) {
      const code = err.code || 'CAPABILITIES_FAILED';
      const status = code === 'UNKNOWN_INSTRUMENT' ? 404 : 502;
      return sendError(res, status, code, err.message);
    }
  });

  // ================================================================
  // CANDLES
  // ================================================================

  app.get('/api/candles/:symbol', requireAuth, async (req, res) => {
    const symbol = config.normalizeSymbol(req.params.symbol);
    const granularity = Number(req.query.granularity || 60);
    const count = clampInt(req.query.count, 50, 5000, 400);

    if (!symbol) return sendError(res, 400, 'INVALID_SYMBOL', 'Symbol is required');
    if (!market.isSupportedGranularity(granularity)) {
      return sendError(res, 400, 'UNSUPPORTED_GRANULARITY', `Granularity ${granularity} is not supported`);
    }
    try {
      const candles = await market.getCandles(symbol, granularity, count);
      return res.json({ symbol, granularity, candles });
    } catch (err) {
      const code = err.code || 'CANDLES_FAILED';
      const status = code === 'UNKNOWN_INSTRUMENT' ? 404 : 502;
      return sendError(res, status, code, err.message);
    }
  });

  // ================================================================
  // ORDERS
  // ================================================================

  app.post('/api/orders', requireAuth, orderLimiter, async (req, res) => {
    const body = req.body || {};

    // The client MUST supply a clientOrderId. If not, generate one so the
    // idempotency guard still applies, but log the anomaly.
    let clientOrderId = typeof body.clientOrderId === 'string' ? body.clientOrderId.trim() : '';
    if (!clientOrderId) {
      clientOrderId = `srv-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
      log.warn('api.order.client_id_missing', `Generated fallback clientOrderId=${clientOrderId}`);
    }

    try {
      const result = await trading.submitOrder({
        clientOrderId,
        symbol: body.symbol,
        contractType: body.contractType,
        stake: body.stake,
        currency: body.currency,
        multiplier: body.multiplier,
        duration: body.duration,
        durationUnit: body.durationUnit,
        barrier: body.barrier,
      });

      return res.status(201).json({
        order: result.order,
        duplicate: !!result.duplicate,
      });
    } catch (err) {
      const code = err.code || 'ORDER_FAILED';
      const status = mapOrderErrorToStatus(code);
      log.warn('api.order.rejected', `${code}: ${err.message}`, {
        clientOrderId,
        derivRequestId: err.requestId,
      });
      return sendError(res, status, code, err.message, err.requestId);
    }
  });

  app.get('/api/orders', requireAuth, async (req, res) => {
    const limit = clampInt(req.query.limit, 1, 500, 100);
    const filter = {};
    if (req.query.state) filter.state = String(req.query.state).toUpperCase();
    if (req.query.symbol) filter.symbol = config.normalizeSymbol(req.query.symbol);

    const rows = await Order.find(filter, { _id: 0, __v: 0, derivResponse: 0 })
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean();
    return res.json({ orders: rows });
  });

  app.get('/api/orders/:clientOrderId', requireAuth, async (req, res) => {
    const doc = await Order.findOne(
      { clientOrderId: req.params.clientOrderId },
      { _id: 0, __v: 0 }
    ).lean();
    if (!doc) return sendError(res, 404, 'ORDER_NOT_FOUND', 'Order not found');
    return res.json({ order: doc });
  });

  // ================================================================
  // CONTRACTS
  // ================================================================

  app.get('/api/contracts', requireAuth, async (req, res) => {
    const filter = {};
    if (req.query.status === 'open') filter.isSold = false;
    else if (req.query.status === 'closed') filter.isSold = true;
    if (req.query.symbol) filter.symbol = config.normalizeSymbol(req.query.symbol);

    const limit = clampInt(req.query.limit, 1, 500, 200);
    const rows = await Contract.find(filter, { _id: 0, __v: 0, lastDerivPayload: 0 })
      .sort({ dateStart: -1 })
      .limit(limit)
      .lean();
    return res.json({ contracts: rows });
  });

  app.get('/api/contracts/:contractId', requireAuth, async (req, res) => {
    const doc = await Contract.findOne(
      { contractId: req.params.contractId },
      { _id: 0, __v: 0 }
    ).lean();
    if (!doc) return sendError(res, 404, 'CONTRACT_NOT_FOUND', 'Contract not found');
    return res.json({ contract: doc });
  });

  // ================================================================
  // TRADES
  // ================================================================

  app.get('/api/trades', requireAuth, async (req, res) => {
    const filter = {};
    if (req.query.symbol) filter.symbol = config.normalizeSymbol(req.query.symbol);
    if (req.query.result) filter.result = String(req.query.result).toUpperCase();
    if (req.query.from || req.query.to) {
      filter.closeTime = {};
      if (req.query.from) filter.closeTime.$gte = Math.floor(new Date(req.query.from).getTime() / 1000);
      if (req.query.to) filter.closeTime.$lte = Math.floor(new Date(req.query.to).getTime() / 1000);
    }

    const limit = clampInt(req.query.limit, 1, 1000, 100);
    const rows = await Trade.find(filter, { _id: 0, __v: 0 })
      .sort({ closeTime: -1 })
      .limit(limit)
      .lean();
    return res.json({ trades: rows });
  });

  app.get('/api/trades/:tradeId', requireAuth, async (req, res) => {
    const doc = await Trade.findOne({ tradeId: req.params.tradeId }, { _id: 0, __v: 0 }).lean();
    if (!doc) return sendError(res, 404, 'TRADE_NOT_FOUND', 'Trade not found');
    return res.json({ trade: doc });
  });

  // ================================================================
  // SYSTEM
  // ================================================================

  app.get('/api/system/status', requireAuth, async (_req, res) => {
    return res.json(buildSystemStatus());
  });

  app.get('/api/system/events', requireAuth, (req, res) => {
    const limit = clampInt(req.query.limit, 1, 200, 50);
    return res.json({ events: bus.history(limit) });
  });

  app.get('/api/system/config', requireAuth, (_req, res) => {
    return res.json(config.publicConfig());
  });

  // ================================================================
  // HEALTH (public — used by Render)
  // ================================================================

  app.get('/health', (_req, res) => {
    const status = buildSystemStatus();
    const ok = status.ok;
    return res.status(ok ? 200 : 503).json(status);
  });

  // ================================================================
  // STATIC
  // ================================================================

  // Serve the terminal shell.
  app.use('/', express.static(CLIENT_DIR, {
    index: 'index.html',
    extensions: false,
    maxAge: config.isProduction ? '5m' : 0,
    setHeaders(res) {
      // The HTML shell must never be cached aggressively — it references
      // the asset version and needs to be re-fetched on deploy.
      res.setHeader('Cache-Control', 'no-cache');
    },
  }));

  // Optional: any future static assets in /public
  app.use('/public', express.static(PUBLIC_DIR, {
    maxAge: config.isProduction ? '1h' : 0,
    immutable: false,
  }));

  // SPA fallback: any GET that is not /api, /ws, /health, or /public returns
  // the shell so client-side routing works.
  app.get(/^\/(?!api\/|ws$|health$|public\/).*/, (req, res, next) => {
    if (req.method !== 'GET') return next();
    res.setHeader('Cache-Control', 'no-cache');
    res.sendFile(path.join(CLIENT_DIR, 'index.html'), (err) => {
      if (err) next();
    });
  });

  // ---- 404 for unmatched API routes ------------------------------
  app.use('/api', (_req, res) => sendError(res, 404, 'NOT_FOUND', 'Endpoint not found'));

  // ---- error handler ---------------------------------------------
  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, _next) => {
    const isBodyError = err && (err.type === 'entity.parse.failed' || err.type === 'entity.too.large');
    if (isBodyError) {
      return sendError(res, 400, 'INVALID_BODY', 'Request body was invalid or too large');
    }
    log.exception('http.unhandled', err, { path: req.path, method: req.method });
    return sendError(res, 500, 'INTERNAL_ERROR', 'Internal server error');
  });

  return app;
}

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

function sendError(res, status, code, message, requestId) {
  const body = { error: { code, message } };
  if (requestId != null) body.error.requestId = String(requestId);
  return res.status(status).json(body);
}

function clampInt(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  const i = Math.floor(n);
  return Math.max(min, Math.min(max, i));
}

function mapOrderErrorToStatus(code) {
  switch (code) {
    case 'NOT_AUTHORIZED': return 503;
    case 'UNKNOWN_INSTRUMENT':
    case 'CONTRACT_NOT_AVAILABLE': return 404;
    case 'MARKET_CLOSED': return 409;
    case 'MISSING_CLIENT_ORDER_ID':
    case 'INVALID_INPUT':
    case 'INVALID_SYMBOL':
    case 'INVALID_CONTRACT_TYPE':
    case 'INVALID_STAKE':
    case 'INVALID_CURRENCY':
    case 'INVALID_MULTIPLIER':
    case 'INVALID_DURATION':
    case 'INVALID_DURATION_UNIT':
    case 'MISSING_MULTIPLIER':
    case 'MISSING_BARRIER':
    case 'MULTIPLIER_NOT_SUPPORTED':
    case 'DURATION_NOT_SUPPORTED':
    case 'BARRIER_NOT_SUPPORTED':
      return 400;
    case 'STAKE_BELOW_MIN':
    case 'STAKE_ABOVE_MAX':
    case 'STAKE_BELOW_DERIV_MIN':
    case 'STAKE_ABOVE_DERIV_MAX':
    case 'DURATION_BELOW_MIN':
    case 'DURATION_ABOVE_MAX':
      return 422;
    case 'TOO_MANY_OPEN_POSITIONS': return 429;
    default: return 502;
  }
}

function buildSystemStatus() {
  const deriv = gateway.status();
  const db = database.status();
  const ws = wsGateway.stats();
  const mk = market.listInstruments().length;
  const td = trading.stats();

  const dbOk = db.state === 'connected';
  const derivOk = deriv.state === 'authorized';

  return {
    ok: dbOk && derivOk,
    startedAt: state.startedAt,
    uptimeMs: state.startedAt ? (Date.now() - state.startedAt) : 0,
    env: config.env,
    accountMode: config.deriv.accountMode,
    deriv: {
      state: deriv.state,
      authorized: deriv.authorized,
      accountId: deriv.accountId,
      reconnectAttempt: deriv.reconnectAttempt,
      subscriptions: deriv.subscriptions,
      pendingRequests: deriv.pendingRequests,
      lastError: deriv.lastError ? { code: deriv.lastError.code, at: deriv.lastError.at } : null,
    },
    database: {
      state: db.state,
      readyState: db.readyState,
      attempt: db.attempt,
    },
    websocket: {
      state: ws.started ? 'listening' : 'stopped',
      clients: ws.clients,
    },
    market: {
      instruments: mk,
    },
    trading: {
      started: td.started,
      openContractSubscriptions: td.openContractSubscriptions,
      capabilityCacheSize: td.capabilityCacheSize,
    },
    eventBus: {
      metrics: bus.metrics(),
      patterns: bus.patterns(),
    },
  };
}

async function buildAccountSummary() {
  const open = await Contract.find({ isSold: false }, { profit: 1, stake: 1 }).lean();
  const unrealizedPl = open.reduce((sum, c) => sum + (Number(c.profit) || 0), 0);
  const openExposure = open.reduce((sum, c) => sum + (Number(c.stake) || 0), 0);

  const [totalTrades, winningTrades, losingTrades] = await Promise.all([
    Trade.countDocuments({}),
    Trade.countDocuments({ result: 'WIN' }),
    Trade.countDocuments({ result: 'LOSS' }),
  ]);

  const startOfDay = new Date();
  startOfDay.setHours(0, 0, 0, 0);
  const startOfDaySec = Math.floor(startOfDay.getTime() / 1000);

  const todayAgg = await Trade.aggregate([
    { $match: { closeTime: { $gte: startOfDaySec } } },
    { $group: { _id: null, pl: { $sum: '$profit' } } },
  ]);
  const todayPl = todayAgg.length ? todayAgg[0].pl : 0;

  const sessionSince = Math.floor((Date.now() - 24 * 3600 * 1000) / 1000);
  const sessionAgg = await Trade.aggregate([
    { $match: { closeTime: { $gte: sessionSince } } },
    { $group: { _id: null, pl: { $sum: '$profit' } } },
  ]);
  const sessionPl = sessionAgg.length ? sessionAgg[0].pl : 0;

  const accountDoc = await Account.findOne({}, { balance: 1 }).lean();
  const balance = accountDoc ? Number(accountDoc.balance) : null;
  const equity = Number.isFinite(balance) ? balance + unrealizedPl : null;

  return {
    unrealizedPl: roundTo(unrealizedPl, 2),
    openExposure: roundTo(openExposure, 2),
    openContracts: open.length,
    todayPl: roundTo(todayPl, 2),
    sessionPl: roundTo(sessionPl, 2),
    totalTrades,
    winningTrades,
    losingTrades,
    equity: equity != null ? roundTo(equity, 2) : null,
  };
}

function roundTo(n, places) {
  const p = Math.pow(10, places);
  return Math.round(n * p) / p;
}

/* ------------------------------------------------------------------ */
/* boot                                                                */
/* ------------------------------------------------------------------ */

async function boot() {
  if (state.started) return;
  state.startedAt = Date.now();

  log.info('boot.start', `Ray Trading Hub booting`, {
    env: config.env,
    node: process.version,
    accountMode: config.deriv.accountMode,
    watchlist: config.market.watchlist.join(','),
  });

  // 1. Database (non-fatal if unavailable — persistence will queue).
  await database.init().catch((err) => {
    log.error('boot.database_failed', `Database init failed: ${err.message}`);
  });

  // 2. Deriv gateway (must succeed to authorize; failure triggers reconnect).
  try {
    await gateway.start();
    log.info('boot.deriv_ready', `Deriv gateway state=${gateway.state}`);
  } catch (err) {
    log.error('boot.deriv_failed', `Deriv gateway start failed: ${err.message}`);
    // Continue booting — the gateway will keep retrying in the background.
  }

  // 3. Market manager (instruments, tick subscriptions, candle engines).
  try {
    await market.start();
  } catch (err) {
    log.error('boot.market_failed', `Market manager start failed: ${err.message}`);
  }

  // 4. Trading service.
  try {
    trading.start();
  } catch (err) {
    log.error('boot.trading_failed', `Trading service start failed: ${err.message}`);
  }

  // 5. Express + HTTP server.
  const app = createApp();
  state.expressApp = app;
  const server = http.createServer(app);
  state.httpServer = server;

  // 6. WebSocket gateway.
  wsGateway.start(server);

  // 7. Listen.
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(config.server.port, () => {
      server.removeListener('error', reject);
      resolve();
    });
  });

  log.info('boot.ready', `Ray Trading Hub listening on :${config.server.port}`);

  bus.emit(EVENTS.SYSTEM_STARTED, {
    port: config.server.port,
    env: config.env,
    accountMode: config.deriv.accountMode,
    at: state.startedAt,
  });

  state.started = true;
}

/* ------------------------------------------------------------------ */
/* shutdown                                                            */
/* ------------------------------------------------------------------ */

async function shutdown(reason, exitCode = 0) {
  if (state.shuttingDown) return;
  state.shuttingDown = true;

  log.info('shutdown.start', `Shutting down (${reason})`);
  bus.emit(EVENTS.SYSTEM_STOPPED, { reason, at: Date.now() });

  const steps = [
    ['websocket', () => wsGateway.stop()],
    ['http', () => new Promise((resolve) => {
      if (!state.httpServer) return resolve();
      state.httpServer.close(() => resolve());
      // Force-close idle keep-alive connections.
      try { state.httpServer.closeIdleConnections?.(); } catch (_) {}
      // Hard cap so a hung socket cannot block shutdown.
      setTimeout(resolve, 5_000);
    })],
    ['trading', () => trading.stop()],
    ['market', () => market.stop()],
    ['deriv', () => gateway.stop()],
    ['database', () => database.shutdown()],
  ];

  for (const [name, fn] of steps) {
    try {
      await fn();
      log.info('shutdown.step', `stopped ${name}`);
    } catch (err) {
      log.warn('shutdown.step_failed', `${name}: ${err.message}`);
    }
  }

  log.info('shutdown.complete', `Bye`);
  process.exit(exitCode);
}

/* ------------------------------------------------------------------ */
/* process-level wiring                                                */
/* ------------------------------------------------------------------ */

process.on('SIGINT', () => { shutdown('SIGINT', 0); });
process.on('SIGTERM', () => { shutdown('SIGTERM', 0); });

process.on('unhandledRejection', (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  log.error('process.unhandled_rejection', msg);
  bus.emit(EVENTS.SYSTEM_ERROR, {
    source: 'process',
    code: 'UNHANDLED_REJECTION',
    message: msg,
  });
});

process.on('uncaughtException', (err) => {
  log.exception('process.uncaught_exception', err);
  bus.emit(EVENTS.SYSTEM_ERROR, {
    source: 'process',
    code: 'UNCAUGHT_EXCEPTION',
    message: err.message,
  });
  // Give logging a chance, then exit — state may be corrupt after an
  // uncaught exception.
  setTimeout(() => shutdown('uncaughtException', 1), 500);
});

/* ------------------------------------------------------------------ */
/* start                                                               */
/* ------------------------------------------------------------------ */

if (require.main === module) {
  boot().catch((err) => {
    log.exception('boot.fatal', err);
    process.exit(1);
  });
}

module.exports = {
  boot,
  shutdown,
  createApp,
  state,
};
