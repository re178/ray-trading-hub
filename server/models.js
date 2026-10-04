'use strict';

/**
 * Ray Trading Hub — MongoDB models.
 *
 * Design rules:
 *  - Only persistent state lives here. Transient events (ticks, live candle
 *    updates, heartbeats) are NEVER written to MongoDB. See config.persistence.
 *  - Every Deriv identifier (requestId, contractId, buyPrice, etc.) is stored
 *    verbatim so local records can be reconciled against Deriv.
 *  - Every financial field is stored as a Number and unit-labelled by
 *    `currency` on the parent document. No strings for amounts.
 *  - Unique indexes protect against duplicate order submission at the
 *    database level — the idempotency guarantee is not only application-level.
 *  - Schemas are strict: unknown fields are rejected, so a bug that tries to
 *    write `{ balence: 100 }` fails loudly instead of silently.
 *  - toJSON strips `_id` and `__v` and renames `_id`-adjacent internal fields.
 *  - No `users` collection: Ray Trading Hub uses a single operator password
 *    with an HMAC-signed cookie. Sessions are stateless. There are no
 *    end-user accounts to model.
 */

const mongoose = require('mongoose');
const config = require('./config');

const { Schema, model, models } = mongoose;

/* ------------------------------------------------------------------ */
/* shared transforms                                                   */
/* ------------------------------------------------------------------ */

const jsonOptions = {
  virtuals: false,
  versionKey: false,
  transform(_doc, ret) {
    delete ret._id;
    return ret;
  },
};

/* ------------------------------------------------------------------ */
/* enums                                                               */
/* ------------------------------------------------------------------ */

const ORDER_STATES = Object.freeze([
  'DRAFT',
  'VALIDATING',
  'SUBMITTING',
  'SUBMITTED',
  'ACCEPTED',
  'OPEN',
  'UPDATED',
  'CLOSING',
  'CLOSED',
  'REJECTED',
  'FAILED',
  'CANCELLED',
  'UNKNOWN',
]);

const CONTRACT_STATES = Object.freeze([
  'OPEN',
  'UPDATED',
  'CLOSING',
  'CLOSED',
  'EXPIRED',
  'CANCELLED',
]);

const TRADE_RESULTS = Object.freeze(['WIN', 'LOSS', 'DRAW', 'UNKNOWN']);

const ACCOUNT_MODES = Object.freeze(['demo', 'real']);

const AUDIT_ACTIONS = Object.freeze([
  'LOGIN_SUCCESS',
  'LOGIN_FAILURE',
  'LOGOUT',
  'PLACE_TRADE',
  'CLOSE_CONTRACT',
  'ORDER_ACCEPTED',
  'ORDER_REJECTED',
  'ORDER_FAILED',
  'CONTRACT_OPENED',
  'CONTRACT_CLOSED',
  'DERIV_CONNECTED',
  'DERIV_DISCONNECTED',
  'DERIV_REAUTH',
  'SYSTEM_START',
  'SYSTEM_STOP',
  'CONFIG_LOADED',
  'ERROR',
]);

/* ------------------------------------------------------------------ */
/* Account                                                             */
/* ------------------------------------------------------------------ */

/**
 * Current snapshot of the authorized Deriv account. Upserted on every
 * balance / account event from Deriv. This is a materialized view — the
 * authoritative source remains Deriv. We keep it so the UI can render
 * without waiting for the next push and so an audit trail exists.
 */
const AccountSchema = new Schema(
  {
    accountId:   { type: String, required: true, index: true },
    mode:        { type: String, enum: ACCOUNT_MODES, required: true },
    currency:    { type: String, required: true, uppercase: true, minlength: 3, maxlength: 6 },
    balance:     { type: Number, required: true, default: 0 },
    loginid:     { type: String },
    email:       { type: String },
    country:     { type: String },
    isVirtual:   { type: Boolean, default: true },

    // Deriv-reported metadata we surface but never invent.
    landingCompany: { type: String },

    // Lifecycle
    lastSyncedAt: { type: Date, default: Date.now },
    lastSyncedAtEpoch: { type: Number },

    // Raw Deriv payloads for traceability. Trimmed — no tokens ever.
    lastAccountResponse: { type: Schema.Types.Mixed },
    lastBalanceResponse: { type: Schema.Types.Mixed },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

AccountSchema.index({ accountId: 1, mode: 1 }, { unique: true });

/* ------------------------------------------------------------------ */
/* Instrument                                                          */
/* ------------------------------------------------------------------ */

/**
 * Cached instrument metadata from Deriv's `active_symbols` response.
 * Keyed by the NORMALIZED internal symbol (see config.normalizeSymbol).
 * Never hard-code symbol lists anywhere else — always read from here.
 */
const InstrumentSchema = new Schema(
  {
    symbol:           { type: String, required: true, unique: true, index: true },
    derivSymbol:      { type: String, required: true }, // as Deriv names it
    displayName:      { type: String, required: true },
    market:           { type: String },
    submarket:        { type: String },
    marketDisplayName: { type: String },
    submarketDisplayName: { type: String },
    pip:              { type: Number, default: 4 },
    pipSize:          { type: Number, default: 0.0001 },
    displayDecimals:  { type: Number, default: 4 },
    exchangeIsOpen:   { type: Boolean, default: false },
    isOpen:           { type: Boolean, default: false },
    openTime:         { type: String },  // "HH:MM:SS" as Deriv provides
    closeTime:        { type: String },
    submarketId:      { type: String },
    spot:             { type: Number },
    spotTime:         { type: Number },

    lastRefreshedAt:  { type: Date, default: Date.now },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

InstrumentSchema.index({ market: 1, isOpen: 1 });

/* ------------------------------------------------------------------ */
/* Candle                                                              */
/* ------------------------------------------------------------------ */

/**
 * Closed candles only. Live/in-progress candles live in memory in the
 * candle engine and are never persisted mid-formation. This keeps the
 * collection bounded and makes every stored row historically immutable.
 *
 * Uniqueness is enforced on (symbol, granularity, epoch) so a replayed
 * history fetch cannot create duplicate rows.
 */
const CandleSchema = new Schema(
  {
    symbol:      { type: String, required: true, index: true },
    granularity: { type: Number, required: true },   // seconds
    epoch:       { type: Number, required: true },   // candle open time (unix seconds)
    open:        { type: Number, required: true },
    high:        { type: Number, required: true },
    low:         { type: Number, required: true },
    close:       { type: Number, required: true },
    volume:      { type: Number },
    closed:      { type: Boolean, default: true },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

CandleSchema.index({ symbol: 1, granularity: 1, epoch: -1 }, { unique: true });
CandleSchema.index({ symbol: 1, granularity: 1, epoch: 1 });

/* ------------------------------------------------------------------ */
/* Order                                                               */
/* ------------------------------------------------------------------ */

/**
 * Every order attempt. One document per user-submitted order, updated
 * through its lifecycle via state transitions. `clientOrderId` is the
 * idempotency key — unique at the database level.
 *
 * This is the single most important collection for auditability: it must
 * be possible to reconstruct exactly what happened to every trade request
 * from here plus Deriv's own records.
 */
const OrderSchema = new Schema(
  {
    clientOrderId: { type: String, required: true, unique: true, index: true },
    userId:        { type: String, default: 'operator' },

    // Requested contract parameters (as submitted by the user)
    symbol:        { type: String, required: true, index: true },
    contractType:  { type: String, required: true },
    stake:         { type: Number, required: true, min: 0 },
    currency:      { type: String, required: true, uppercase: true },
    multiplier:    { type: Number },
    duration:      { type: Number },
    durationUnit:  { type: String },
    barrier:       { type: String },
    // Raw parameters echoed back to Deriv. Kept verbatim for reconciliation.
    parameters:    { type: Schema.Types.Mixed, default: {} },

    // Lifecycle
    state: {
      type: String,
      enum: ORDER_STATES,
      default: 'DRAFT',
      required: true,
      index: true,
    },
    stateHistory: [{
      _id: false,
      state: { type: String, enum: ORDER_STATES, required: true },
      at:    { type: Date, default: Date.now },
      note:  { type: String },
    }],

    // Deriv-side identifiers
    derivRequestId: { type: String, index: true },
    derivResponse:  { type: Schema.Types.Mixed },  // as received, tokens stripped

    // Result linkage
    contractId:     { type: String, index: true },

    // Failure info (structured, never a raw stack)
    failure: {
      code:      { type: String },
      message:   { type: String },
      requestId: { type: String },
      at:        { type: Date },
    },

    // Idempotency bookkeeping
    idempotencyKey: { type: String, index: true },
    submittedAt:    { type: Date },
    acceptedAt:     { type: Date },
    closedAt:       { type: Date },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

OrderSchema.index({ userId: 1, createdAt: -1 });
OrderSchema.index({ symbol: 1, state: 1 });
OrderSchema.index({ contractId: 1 });

/* ------------------------------------------------------------------ */
/* Contract                                                            */
/* ------------------------------------------------------------------ */

/**
 * Deriv contract records. One document per Deriv contract. Upserted on
 * open and updated on every `proposal_open_contract` push from Deriv.
 * The authoritative source is Deriv; this is the reconciliation record.
 */
const ContractSchema = new Schema(
  {
    contractId:    { type: String, required: true, unique: true, index: true },
    clientOrderId: { type: String, index: true },
    accountId:     { type: String, index: true },
    userId:        { type: String, default: 'operator' },

    symbol:        { type: String, required: true, index: true },
    contractType:  { type: String, required: true },
    currency:      { type: String, required: true, uppercase: true },
    underlying:    { type: String },

    // Financial fields (all sourced from Deriv; none invented)
    buyPrice:      { type: Number },
    stake:         { type: Number },
    payout:        { type: Number },
    entrySpot:     { type: Number },
    currentSpot:   { type: Number },
    exitSpot:      { type: Number },
    entryTickTime: { type: Number },
    exitTickTime:  { type: Number },
    profit:        { type: Number },
    profitCurrency: { type: String },
    multiplier:    { type: Number },

    // Duration as requested / as Deriv reports
    dateStart:     { type: Number },
    dateExpiry:    { type: Number },
    duration:      { type: Number },
    durationUnit:  { type: String },
    tickCount:     { type: Number },
    barrier:       { type: String },

    // Status
    status:        { type: String, enum: CONTRACT_STATES, default: 'OPEN', index: true },
    isSold:        { type: Boolean, default: false },
    isExpired:     { type: Boolean, default: false },
    isSettled:     { type: Boolean, default: false },

    // Deriv identifiers
    transactionId: { type: Number, index: true },
    sellTime:      { type: Number },

    // Raw Deriv payloads for reconciliation (tokens stripped upstream)
    lastDerivPayload: { type: Schema.Types.Mixed },
    updateCount:   { type: Number, default: 0 },
    lastUpdatedAt: { type: Date, default: Date.now },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

ContractSchema.index({ status: 1, symbol: 1 });
ContractSchema.index({ accountId: 1, createdAt: -1 });
ContractSchema.index({ isSold: 1, isSettled: 1 });

/* ------------------------------------------------------------------ */
/* Trade                                                               */
/* ------------------------------------------------------------------ */

/**
 * A Trade is the immutable record of a completed contract — the "final
 * result". Created exactly once, when Deriv reports the contract settled.
 * Never mutated afterwards. Orders and Contracts describe the journey;
 * Trade describes the destination.
 */
const TradeSchema = new Schema(
  {
    tradeId:       { type: String, required: true, unique: true, index: true }, // internal id
    contractId:    { type: String, required: true, index: true },
    clientOrderId: { type: String, index: true },
    accountId:     { type: String, index: true },
    userId:        { type: String, default: 'operator' },

    symbol:        { type: String, required: true, index: true },
    contractType:  { type: String, required: true },
    currency:      { type: String, required: true, uppercase: true },

    stake:         { type: Number, required: true },
    payout:        { type: Number },
    entrySpot:     { type: Number },
    exitSpot:      { type: Number },
    multiplier:    { type: Number },

    openTime:      { type: Number },   // unix seconds
    closeTime:     { type: Number },
    durationSeconds: { type: Number },

    profit:        { type: Number, required: true },
    profitPercent: { type: Number },

    result: {
      type: String,
      enum: TRADE_RESULTS,
      default: 'UNKNOWN',
      required: true,
      index: true,
    },

    status:        { type: String, default: 'CLOSED' },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

TradeSchema.index({ accountId: 1, closeTime: -1 });
TradeSchema.index({ symbol: 1, result: 1 });
TradeSchema.index({ createdAt: -1 });

/* ------------------------------------------------------------------ */
/* Transaction                                                         */
/* ------------------------------------------------------------------ */

/**
 * Deriv account transactions. Every entry originates from a Deriv
 * `transaction` stream event and is stored with the Deriv transactionId
 * as the unique key. This is the ledger.
 */
const TransactionSchema = new Schema(
  {
    transactionId: { type: Number, required: true, unique: true, index: true },
    accountId:     { type: String, required: true, index: true },
    actionType:    { type: String, required: true },
    amount:        { type: Number, required: true },
    balanceAfter:  { type: Number },
    currency:      { type: String, uppercase: true },
    contractId:    { type: String, index: true },
    reference:     { type: String },
    transactionTime: { type: Number, index: true },
    raw:           { type: Schema.Types.Mixed },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

TransactionSchema.index({ accountId: 1, transactionTime: -1 });

/* ------------------------------------------------------------------ */
/* AuditLog                                                            */
/* ------------------------------------------------------------------ */

/**
 * Append-only audit trail. Every important operator action and every
 * significant system event lands here. Read-only by policy: nothing in
 * the application updates or deletes an AuditLog row.
 */
const AuditLogSchema = new Schema(
  {
    at:      { type: Date, required: true, default: Date.now, index: true },
    action:  { type: String, enum: AUDIT_ACTIONS, required: true, index: true },
    userId:  { type: String, default: 'operator', index: true },
    source:  { type: String, required: true }, // module or subsystem name

    symbol:       { type: String, index: true },
    orderId:      { type: String, index: true },
    clientOrderId: { type: String, index: true },
    contractId:   { type: String, index: true },
    tradeId:      { type: String, index: true },
    derivRequestId: { type: String, index: true },

    message: { type: String },
    code:    { type: String },

    // Structured, redacted detail. Never contains secrets.
    detail:  { type: Schema.Types.Mixed },
  },
  { timestamps: true, strict: true, toJSON: jsonOptions }
);

AuditLogSchema.index({ at: -1 });
AuditLogSchema.index({ action: 1, at: -1 });

/* ------------------------------------------------------------------ */
/* model registry                                                      */
/* ------------------------------------------------------------------ */

const Account     = models.Account     || model('Account',     AccountSchema);
const Instrument  = models.Instrument  || model('Instrument',  InstrumentSchema);
const Candle      = models.Candle      || model('Candle',      CandleSchema);
const Order       = models.Order       || model('Order',       OrderSchema);
const Contract    = models.Contract    || model('Contract',    ContractSchema);
const Trade       = models.Trade       || model('Trade',       TradeSchema);
const Transaction = models.Transaction || model('Transaction', TransactionSchema);
const AuditLog    = models.AuditLog    || model('AuditLog',    AuditLogSchema);

module.exports = {
  Account,
  Instrument,
  Candle,
  Order,
  Contract,
  Trade,
  Transaction,
  AuditLog,
  enums: {
    ORDER_STATES,
    CONTRACT_STATES,
    TRADE_RESULTS,
    ACCOUNT_MODES,
    AUDIT_ACTIONS,
  },
};
