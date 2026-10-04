'use strict';

/**
 * Ray Trading Hub — client WebSocket gateway.
 *
 * This is the boundary between the internal Event Bus and the browsers.
 * It is the ONLY module that talks to client sockets.
 *
 * Responsibilities:
 *   - Authenticate every upgrade (session cookie) before accepting
 *   - Track per-client subscriptions (symbols, chart symbol+granularity)
 *   - Forward a CONTROLLED subset of bus events to each client, filtered
 *     by that client's subscriptions
 *   - Throttle high-frequency events (ticks, candle updates) so a slow
 *     client cannot be flooded or flood the server
 *   - Reply to client control messages (subscribe, chart.subscribe, ping)
 *   - Send a `hello` snapshot on connect (account, connection status,
 *     initial candles for the active chart)
 *   - Emit clean disconnect accounting
 *
 * It NEVER:
 *   - Exposes raw bus payloads (every outgoing payload is built here)
 *   - Sends anything with a Deriv API token or session secret
 *   - Allows a client to trigger a Deriv request directly (that flows
 *     through REST)
 *   - Implements a prediction or strategy
 */

const { WebSocketServer, WebSocket } = require('ws');
const config = require('./config');
const bus = require('./eventBus');
const { EVENTS } = require('./eventBus');
const { Logger } = require('./logger');
const market = require('./market');
const gateway = require('./derivGateway');
const database = require('./database');
const { Account, Contract } = require('./models');
const auth = require('./auth');

const log = new Logger('websocket');

/* ------------------------------------------------------------------ */
/* constants                                                           */
/* ------------------------------------------------------------------ */

// Throttle intervals for high-frequency forwards, per client.
const TICK_THROTTLE_MS = 150;          // ~6-7 Hz per symbol — plenty for UI
const CANDLE_UPDATE_THROTTLE_MS = 250;
const CONTRACT_UPDATE_THROTTLE_MS = 500;
const MAX_CLIENT_BUFFERED_BYTES = 1_000_000; // 1 MB — disconnect if backed up
const PING_INTERVAL_MS = 30_000;
const PING_TIMEOUT_MS = 60_000;        // must be > PING_INTERVAL_MS
const MAX_SUBSCRIPTIONS_PER_CLIENT = 50;
const HELLO_CANDLE_COUNT = 400;

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

function safeSend(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    if (ws.bufferedAmount > MAX_CLIENT_BUFFERED_BYTES) {
      log.warn('ws.client.buffer_overflow', `Closing slow client (buffered=${ws.bufferedAmount})`);
      try { ws.close(1013, 'Slow consumer'); } catch (_) {}
      return false;
    }
    ws.send(JSON.stringify(payload));
    return true;
  } catch (err) {
    log.warn('ws.client.send_failed', err.message);
    return false;
  }
}

/* ------------------------------------------------------------------ */
/* client wrapper                                                      */
/* ------------------------------------------------------------------ */

class Client {
  constructor(ws, session) {
    this.id = `ws-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    this.ws = ws;
    this.session = session;

    // Subscriptions
    this.tickSymbols = new Set();          // symbols whose ticks the client wants
    this.chart = null;                     // { symbol, granularity }

    // Throttle bookkeeping
    this._lastTickAt = new Map();          // symbol -> timestamp
    this._lastCandleUpdateAt = new Map();  // `${symbol}|${g}` -> timestamp
    this._lastContractUpdateAt = new Map();// contractId -> timestamp

    // Liveness
    this.connectedAt = Date.now();
    this.lastPongAt = Date.now();
    this.isAlive = true;

    // Unsubscribers from the bus
    this.unsubscribers = [];
  }

  /** Attach bus subscriptions tailored to this client's needs. */
  attach() {
    const add = (pattern, handler, label) => {
      this.unsubscribers.push(bus.on(pattern, handler, { label: `ws:${this.id}:${label}` }));
    };

    add(EVENTS.MARKET_TICK, (tick) => this._forwardTick(tick), 'tick');
    add(EVENTS.MARKET_CANDLE_UPDATED, (candle) => this._forwardCandleUpdated(candle), 'candleUpdated');
    add(EVENTS.MARKET_CANDLE_CLOSED, (candle) => this._forwardCandleClosed(candle), 'candleClosed');
    add(EVENTS.ACCOUNT_UPDATED, (acct) => this._forwardAccount(acct), 'accountUpdated');
    add(EVENTS.ACCOUNT_BALANCE_UPDATED, (bal) => this._forwardBalance(bal), 'balanceUpdated');
    add(EVENTS.CONTRACT_UPDATED, (c) => this._forwardContractUpdate(c, 'contract.updated'), 'contractUpdated');
    add(EVENTS.CONTRACT_OPENED, (c) => this._forwardContractUpdate(c, 'position.opened', true), 'contractOpened');
    add(EVENTS.POSITION_OPENED, (c) => this._forwardContractUpdate(c, 'position.opened', true), 'positionOpened');
    add(EVENTS.POSITION_UPDATED, (c) => this._forwardContractUpdate(c, 'position.updated'), 'positionUpdated');
    add(EVENTS.POSITION_CLOSED, (c) => this._forwardContractUpdate(c, 'position.closed', true), 'positionClosed');
    add(EVENTS.CONTRACT_CLOSED, (c) => this._forwardContractUpdate(c, 'contract.closed', true), 'contractClosed');
    add(EVENTS.TRADE_COMPLETED, (t) => this._forwardTradeCompleted(t), 'tradeCompleted');
    add(EVENTS.ORDER_ACCEPTED, (o) => this._forwardOrderEvent('order.accepted', o), 'orderAccepted');
    add(EVENTS.ORDER_REJECTED, (o) => this._forwardOrderEvent('order.rejected', o), 'orderRejected');
    add(EVENTS.ORDER_FAILED, (o) => this._forwardOrderEvent('order.failed', o), 'orderFailed');
    add(EVENTS.ORDER_SUBMITTED, (o) => this._forwardOrderEvent('order.submitted', o), 'orderSubmitted');
    add(EVENTS.DERIV_CONNECTED, (d) => this._forwardConnStatus('deriv', 'connected', d), 'derivConnected');
    add(EVENTS.DERIV_AUTHORIZED, () => this._forwardConnStatus('deriv', 'authorized'), 'derivAuthorized');
    add(EVENTS.DERIV_DISCONNECTED, (d) => this._forwardConnStatus('deriv', 'disconnected', d), 'derivDisconnected');
    add(EVENTS.DERIV_RECONNECTING, (d) => this._forwardConnStatus('deriv', 'reconnecting', d), 'derivReconnecting');
    add(EVENTS.DERIV_ERROR, (d) => this._forwardConnStatus('deriv', 'error', d), 'derivError');
    add(EVENTS.SYSTEM_STATUS, (s) => this._forwardSystemStatus(s), 'systemStatus');
    add(EVENTS.SYSTEM_ERROR, (e) => this._forwardSystemError(e), 'systemError');
  }

  detach() {
    for (const unsub of this.unsubscribers) {
      try { unsub(); } catch (_) {}
    }
    this.unsubscribers = [];
  }

  /* -------------------------------------------------------------- */
  /* forwarding                                                      */
  /* -------------------------------------------------------------- */

  _forwardTick(tick) {
    if (!tick || !this.tickSymbols.has(tick.symbol)) return;
    const now = Date.now();
    const last = this._lastTickAt.get(tick.symbol) || 0;
    if (now - last < TICK_THROTTLE_MS) return;
    this._lastTickAt.set(tick.symbol, now);

    safeSend(this.ws, {
      type: 'event',
      event: 'market.tick',
      data: {
        symbol: tick.symbol,
        quote: tick.quote,
        bid: tick.bid,
        ask: tick.ask,
        epoch: tick.epoch,
      },
    });
  }

  _forwardCandleUpdated(candle) {
    if (!candle || !this.chart) return;
    if (candle.symbol !== this.chart.symbol || candle.granularity !== this.chart.granularity) return;

    const key = `${candle.symbol}|${candle.granularity}`;
    const now = Date.now();
    const last = this._lastCandleUpdateAt.get(key) || 0;
    if (now - last < CANDLE_UPDATE_THROTTLE_MS) return;
    this._lastCandleUpdateAt.set(key, now);

    safeSend(this.ws, {
      type: 'event',
      event: 'market.candle.updated',
      data: {
        symbol: candle.symbol,
        granularity: candle.granularity,
        epoch: candle.epoch,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume,
        closed: false,
      },
    });
  }

  _forwardCandleClosed(candle) {
    if (!candle || !this.chart) return;
    if (candle.symbol !== this.chart.symbol || candle.granularity !== this.chart.granularity) return;

    // Closed candles are never throttled — dropping one would desync the chart.
    safeSend(this.ws, {
      type: 'event',
      event: 'market.candle.closed',
      data: {
        symbol: candle.symbol,
        granularity: candle.granularity,
        epoch: candle.epoch,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume,
        closed: true,
      },
    });
  }

  _forwardAccount(acct) {
    if (!acct) return;
    safeSend(this.ws, {
      type: 'event',
      event: 'account.updated',
      data: {
        accountId: acct.accountId,
        mode: acct.mode,
        currency: acct.currency,
        balance: acct.balance,
        isVirtual: acct.isVirtual,
        email: acct.email,
        country: acct.country,
        loginid: acct.loginid,
      },
    });
  }

  _forwardBalance(bal) {
    if (!bal) return;
    safeSend(this.ws, {
      type: 'event',
      event: 'account.balance.updated',
      data: {
        accountId: bal.accountId,
        currency: bal.currency,
        balance: bal.balance,
        epoch: bal.epoch,
      },
    });
  }

  _forwardContractUpdate(c, eventName, force = false) {
    if (!c || !c.contractId) return;
    const key = String(c.contractId);
    if (!force) {
      const now = Date.now();
      const last = this._lastContractUpdateAt.get(key) || 0;
      if (now - last < CONTRACT_UPDATE_THROTTLE_MS) return;
      this._lastContractUpdateAt.set(key, now);
    }

    safeSend(this.ws, {
      type: 'event',
      event: eventName,
      data: {
        contractId: String(c.contractId),
        clientOrderId: c.clientOrderId,
        symbol: c.symbol,
        contractType: c.contractType,
        currency: c.currency,
        stake: c.stake,
        buyPrice: c.buyPrice,
        payout: c.payout,
        entrySpot: c.entrySpot,
        currentSpot: c.currentSpot,
        exitSpot: c.exitSpot,
        profit: c.profit,
        multiplier: c.multiplier,
        status: c.status,
        isSold: c.isSold,
        dateStart: c.dateStart,
        dateExpiry: c.dateExpiry,
        duration: c.duration,
        durationUnit: c.durationUnit,
        sellTime: c.sellTime,
        exitTickTime: c.exitTickTime,
      },
    });
  }

  _forwardTradeCompleted(t) {
    if (!t || !t.contractId) return;
    safeSend(this.ws, {
      type: 'event',
      event: 'trade.completed',
      data: {
        tradeId: t.tradeId || `TRD-${t.contractId}`,
        contractId: String(t.contractId),
        clientOrderId: t.clientOrderId,
        symbol: t.symbol,
        contractType: t.contractType,
        currency: t.currency,
        stake: t.stake,
        payout: t.payout,
        entrySpot: t.entrySpot,
        exitSpot: t.exitSpot,
        profit: t.profit,
        result: t.result,
        closeTime: t.closeTime,
      },
    });
  }

  _forwardOrderEvent(eventName, o) {
    if (!o || !o.clientOrderId) return;
    safeSend(this.ws, {
      type: 'event',
      event: eventName,
      data: {
        clientOrderId: o.clientOrderId,
        symbol: o.symbol,
        contractType: o.contractType,
        stake: o.stake,
        currency: o.currency,
        contractId: o.contractId,
        code: o.code,
        message: o.message,
        derivRequestId: o.derivRequestId,
      },
    });
  }

  _forwardConnStatus(target, state, detail) {
    safeSend(this.ws, {
      type: 'event',
      event: 'connection.status',
      data: { target, state, detail: detail ? sanitizeConnDetail(detail) : undefined },
    });
  }

  _forwardSystemStatus(s) {
    if (!s || typeof s !== 'object') return;
    // Only forward the connection-shaped parts of system.status.
    const out = {};
    if (s.deriv) out.deriv = { state: s.deriv.state };
    if (s.database) out.database = { state: s.database.state };
    if (Object.keys(out).length === 0) return;
    safeSend(this.ws, { type: 'event', event: 'system.status', data: out });
  }

  _forwardSystemError(e) {
    if (!e) return;
    safeSend(this.ws, {
      type: 'event',
      event: 'system.error',
      data: {
        source: e.source,
        code: e.code,
        message: e.message,
      },
    });
  }
}

/* ------------------------------------------------------------------ */
/* gateway                                                             */
/* ------------------------------------------------------------------ */

class WebSocketGateway {
  constructor() {
    this._wss = null;
    this._clients = new Map();    // id -> Client
    this._pingTimer = null;
    this._started = false;
  }

  /**
   * Attach the WebSocket server to an existing HTTP server.
   * @param {import('http').Server} httpServer
   */
  start(httpServer) {
    if (this._started) return;
    this._started = true;

    this._wss = new WebSocketServer({
      noServer: true,
      perMessageDeflate: false,
      maxPayload: 64 * 1024,     // client messages are small control frames
      clientTracking: false,
    });

    // Manual upgrade handling so we can authenticate before completing
    // the handshake.
    httpServer.on('upgrade', (req, socket, head) => {
      let pathname = '/';
      try {
        pathname = new URL(req.url, 'http://localhost').pathname;
      } catch (_) {
        socket.destroy();
        return;
      }
      if (pathname !== '/ws') {
        socket.destroy();
        return;
      }

      // Origin check for cross-site protection.
      if (!this._originAllowed(req)) {
        log.warn('ws.upgrade.origin_rejected', `Rejected upgrade from origin=${req.headers.origin || '(none)'}`);
        socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
        socket.destroy();
        return;
      }

      let session;
      try {
        session = auth.readSessionFromCookieHeader(req.headers.cookie || '');
      } catch (err) {
        session = null;
      }
      if (!session) {
        log.warn('ws.upgrade.unauthenticated', 'Rejected unauthenticated upgrade');
        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
        socket.destroy();
        return;
      }

      this._wss.handleUpgrade(req, socket, head, (ws) => {
        this._wss.emit('connection', ws, req, session);
      });
    });

    this._wss.on('connection', (ws, _req, session) => this._onConnection(ws, session));

    this._pingTimer = setInterval(() => this._pingSweep(), PING_INTERVAL_MS);

    log.info('ws.started', 'WebSocket gateway started', { path: '/ws' });
  }

  async stop() {
    if (!this._started) return;
    this._started = false;

    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

    for (const client of this._clients.values()) {
      try { client.ws.close(1001, 'Server shutting down'); } catch (_) {}
      client.detach();
    }
    this._clients.clear();

    if (this._wss) {
      try { this._wss.close(); } catch (_) {}
      this._wss = null;
    }
    log.info('ws.stopped', 'WebSocket gateway stopped');
  }

  stats() {
    return {
      started: this._started,
      clients: this._clients.size,
    };
  }

  /* -------------------------------------------------------------- */

  _originAllowed(req) {
    const origin = req.headers.origin;
    if (!origin) {
      // Same-origin non-browser clients (curl, tests) may omit Origin.
      // In production we still require it when FRONTEND_URL is set.
      return !config.isProduction || !config.server.frontendUrl;
    }
    if (config.server.frontendUrl) {
      try {
        const o = new URL(origin);
        const expected = new URL(config.server.frontendUrl);
        return o.host === expected.host && o.protocol === expected.protocol;
      } catch (_) {
        return false;
      }
    }
    // In dev with no FRONTEND_URL, accept same-host origin.
    try {
      const o = new URL(origin);
      const host = req.headers.host || '';
      return o.host === host;
    } catch (_) {
      return false;
    }
  }

  async _onConnection(ws, session) {
    const client = new Client(ws, session);
    this._clients.set(client.id, client);
    client.attach();

    log.info('ws.client.connected', `Client ${client.id} connected`, { clients: this._clients.size });

    // Liveness plumbing — rely on ws's built-in pong reply to our ping().
    ws.on('pong', () => { client.isAlive = true; client.lastPongAt = Date.now(); });

    ws.on('message', (raw) => this._onMessage(client, raw));
    ws.on('error', (err) => {
      log.warn('ws.client.error', `Client ${client.id}: ${err.message}`);
    });
    ws.on('close', (code, reasonBuf) => {
      const reason = reasonBuf ? reasonBuf.toString().slice(0, 100) : '';
      log.info('ws.client.closed', `Client ${client.id} closed`, { code, reason });
      client.detach();
      this._clients.delete(client.id);
    });

    // Send initial snapshot.
    try {
      await this._sendHello(client);
    } catch (err) {
      log.warn('ws.client.hello_failed', `Client ${client.id}: ${err.message}`);
      safeSend(ws, { type: 'error', data: { code: 'HELLO_FAILED', message: 'Initial snapshot failed' } });
      try { ws.close(1011, 'Hello failed'); } catch (_) {}
    }
  }

  async _sendHello(client) {
    const [accountDoc, openContracts, accountStats] = await Promise.all([
      Account.findOne({}, { _id: 0, __v: 0 }).lean(),
      Contract.find({ isSold: false }, { _id: 0, __v: 0 }).lean(),
      this._accountSummary(),
    ]);

    const instruments = market.listInstruments();

    const hello = {
      type: 'hello',
      data: {
        serverTime: Date.now(),
        session: { userId: client.session.userId, createdAt: client.session.createdAt },
        connection: {
          deriv: gateway.state,
          database: database.state,
          ws: 'connected',
        },
        account: accountDoc ? publicAccount(accountDoc) : null,
        summary: accountStats,
        instruments,
        openContracts: openContracts.map(publicContract),
        subscriptions: {
          tickSymbols: [...client.tickSymbols],
          chart: client.chart,
        },
      },
    };

    safeSend(client.ws, hello);
  }

  async _accountSummary() {
    // Derive a small summary from Deriv-sourced data. Fields that cannot be
    // derived accurately are omitted, never faked.
    try {
      const open = await Contract.find({ isSold: false }, {
        profit: 1, stake: 1, currency: 1,
      }).lean();

      const unrealized = open.reduce((sum, c) => {
        const p = Number(c.profit);
        return sum + (Number.isFinite(p) ? p : 0);
      }, 0);

      const openExposure = open.reduce((sum, c) => {
        const s = Number(c.stake);
        return sum + (Number.isFinite(s) ? s : 0);
      }, 0);

      const [totalTrades, wins, losses] = await Promise.all([
        Contract.countDocuments({ isSold: true }),
        Contract.countDocuments({ isSold: true, profit: { $gt: 0 } }),
        Contract.countDocuments({ isSold: true, profit: { $lt: 0 } }),
      ]);

      const startOfDay = new Date();
      startOfDay.setHours(0, 0, 0, 0);
      const todayAgg = await Contract.aggregate([
        { $match: { isSold: true, sellTime: { $gte: Math.floor(startOfDay.getTime() / 1000) } } },
        { $group: { _id: null, pl: { $sum: '$profit' } } },
      ]);
      const todayPl = todayAgg.length ? todayAgg[0].pl : 0;

      // Session = since process start (or last 24h as a reasonable default).
      const since = Date.now() / 1000 - 24 * 3600;
      const sessionAgg = await Contract.aggregate([
        { $match: { isSold: true, sellTime: { $gte: since } } },
        { $group: { _id: null, pl: { $sum: '$profit' } } },
      ]);
      const sessionPl = sessionAgg.length ? sessionAgg[0].pl : 0;

      const accountDoc = await Account.findOne({}, { balance: 1, currency: 1 }).lean();
      const balance = accountDoc ? Number(accountDoc.balance) : null;
      const equity = Number.isFinite(balance) ? balance + unrealized : null;

      return {
        unrealizedPl: roundTo(unrealized, 2),
        openExposure: roundTo(openExposure, 2),
        openContracts: open.length,
        todayPl: roundTo(todayPl, 2),
        sessionPl: roundTo(sessionPl, 2),
        totalTrades,
        winningTrades: wins,
        losingTrades: losses,
        equity: equity != null ? roundTo(equity, 2) : null,
      };
    } catch (err) {
      log.warn('ws.account_summary_failed', err.message);
      return {};
    }
  }

  /* -------------------------------------------------------------- */
  /* client messages                                                 */
  /* -------------------------------------------------------------- */

  async _onMessage(client, raw) {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (_) {
      return; // ignore non-JSON frames
    }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'ping':
        return safeSend(client.ws, { type: 'pong', data: { at: Date.now() } });

      case 'subscribe':
        return this._handleSubscribe(client, msg);

      case 'unsubscribe':
        return this._handleUnsubscribe(client, msg);

      case 'chart.subscribe':
        return this._handleChartSubscribe(client, msg).catch((err) => {
          safeSend(client.ws, {
            type: 'error',
            data: { code: err.code || 'CHART_SUBSCRIBE_FAILED', message: err.message },
          });
        });

      default:
        return; // unknown types are ignored
    }
  }

  _handleSubscribe(client, msg) {
    const symbols = Array.isArray(msg.symbols) ? msg.symbols : (msg.symbol ? [msg.symbol] : []);
    for (const rawSym of symbols) {
      const sym = config.normalizeSymbol(rawSym);
      if (!sym) continue;
      if (client.tickSymbols.size >= MAX_SUBSCRIPTIONS_PER_CLIENT) break;
      client.tickSymbols.add(sym);
    }
    safeSend(client.ws, {
      type: 'subscribed',
      data: { tickSymbols: [...client.tickSymbols] },
    });
  }

  _handleUnsubscribe(client, msg) {
    const symbols = Array.isArray(msg.symbols) ? msg.symbols : (msg.symbol ? [msg.symbol] : []);
    for (const rawSym of symbols) {
      const sym = config.normalizeSymbol(rawSym);
      if (sym) client.tickSymbols.delete(sym);
    }
    safeSend(client.ws, {
      type: 'unsubscribed',
      data: { tickSymbols: [...client.tickSymbols] },
    });
  }

  async _handleChartSubscribe(client, msg) {
    const symbol = config.normalizeSymbol(msg.symbol);
    const granularity = Number(msg.granularity);

    if (!symbol) throw wsError('INVALID_SYMBOL', 'symbol is required');
    if (!market.isSupportedGranularity(granularity)) {
      throw wsError('UNSUPPORTED_GRANULARITY', `granularity ${granularity} is not supported`);
    }
    if (!market.getInstrument(symbol)) {
      throw wsError('UNKNOWN_INSTRUMENT', `instrument ${symbol} is not tracked`);
    }

    client.chart = { symbol, granularity };

    // Send a fresh snapshot of candles so the client is immediately in sync.
    const candles = await market.getCandles(symbol, granularity, HELLO_CANDLE_COUNT);

    safeSend(client.ws, {
      type: 'chart.snapshot',
      data: { symbol, granularity, candles },
    });

    // Also ensure the client is receiving the tick stream for this symbol.
    if (!client.tickSymbols.has(symbol)) client.tickSymbols.add(symbol);
  }

  /* -------------------------------------------------------------- */
  /* ping sweep                                                      */
  /* -------------------------------------------------------------- */

  _pingSweep() {
    const now = Date.now();
    for (const [id, client] of this._clients) {
      if (!client.isAlive || now - client.lastPongAt > PING_TIMEOUT_MS) {
        log.warn('ws.client.timeout', `Client ${id} unresponsive, closing`);
        try { client.ws.terminate(); } catch (_) {}
        client.detach();
        this._clients.delete(id);
        continue;
      }
      client.isAlive = false;
      try { client.ws.ping(); } catch (_) {}
    }
  }
}

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

function sanitizeConnDetail(d) {
  if (!d || typeof d !== 'object') return undefined;
  return {
    code: d.code,
    reason: d.reason,
    attempt: d.attempt,
    delayMs: d.delayMs,
    message: d.message,
  };
}

function publicAccount(doc) {
  return {
    accountId: doc.accountId,
    mode: doc.mode,
    currency: doc.currency,
    balance: doc.balance,
    isVirtual: doc.isVirtual,
    email: doc.email,
    country: doc.country,
    loginid: doc.loginid,
    lastSyncedAt: doc.lastSyncedAt,
  };
}

function publicContract(doc) {
  return {
    contractId: doc.contractId,
    clientOrderId: doc.clientOrderId,
    symbol: doc.symbol,
    contractType: doc.contractType,
    currency: doc.currency,
    stake: doc.stake,
    buyPrice: doc.buyPrice,
    payout: doc.payout,
    entrySpot: doc.entrySpot,
    currentSpot: doc.currentSpot,
    exitSpot: doc.exitSpot,
    profit: doc.profit,
    multiplier: doc.multiplier,
    status: doc.status,
    isSold: doc.isSold,
    dateStart: doc.dateStart,
    dateExpiry: doc.dateExpiry,
    duration: doc.duration,
    durationUnit: doc.durationUnit,
    sellTime: doc.sellTime,
    exitTickTime: doc.exitTickTime,
  };
}

function roundTo(n, places) {
  const p = Math.pow(10, places);
  return Math.round(n * p) / p;
}

function wsError(code, message) {
  const err = new Error(message);
  err.name = 'WebSocketError';
  err.code = code;
  return err;
}

/* ------------------------------------------------------------------ */
/* singleton                                                           */
/* ------------------------------------------------------------------ */

const wsGateway = new WebSocketGateway();

module.exports = wsGateway;
module.exports.WebSocketGateway = WebSocketGateway;
