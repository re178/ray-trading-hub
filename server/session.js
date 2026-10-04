'use strict';

/**
 * Ray Trading Hub — session helpers.
 *
 * Stateless HMAC-signed cookie sessions.
 *
 * Design rules:
 *  - No session store. No Redis. No express-session. The cookie IS the state.
 *  - Every cookie carries: { userId, iat, exp, nonce } signed with
 *    SESSION_SECRET using HMAC-SHA256.
 *  - Verification is constant-time (`crypto.timingSafeEqual`) to defeat
 *    timing oracles on the signature check.
 *  - The session payload is base64url(JSON) so it is inspectable for
 *    debugging but tamper-proof without the secret.
 *  - Expiration is enforced both from the payload's `exp` field AND from
 *    the cookie's `Max-Age` attribute.
 *  - Revocation in this single-operator deployment is done by rotating
 *    SESSION_SECRET. The `nonce` field is included so future per-session
 *    revocation lists remain possible without changing the wire format.
 *
 * This module has no dependencies beyond Node's crypto and the config
 * module. It knows nothing about Express, WebSocket, or Deriv.
 */

const crypto = require('crypto');
const config = require('./config');

const COOKIE_NAME = config.auth.cookieName;
const SECRET = config.auth.sessionSecret;
const TTL_MS = config.auth.sessionTtlMs;

/* ------------------------------------------------------------------ */
/* encoding primitives                                                 */
/* ------------------------------------------------------------------ */

function b64urlEncode(buf) {
  return Buffer.from(buf)
    .toString('base64')
    .replace(/=+$/, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function b64urlDecode(str) {
  if (typeof str !== 'string') return null;
  const pad = str.length % 4 === 0 ? '' : '='.repeat(4 - (str.length % 4));
  const normalized = str.replace(/-/g, '+').replace(/_/g, '/') + pad;
  try {
    return Buffer.from(normalized, 'base64');
  } catch (_) {
    return null;
  }
}

function sign(data) {
  return crypto.createHmac('sha256', SECRET).update(data).digest();
}

function constantTimeEqual(aBuf, bBuf) {
  if (!aBuf || !bBuf) return false;
  if (aBuf.length !== bBuf.length) return false;
  try {
    return crypto.timingSafeEqual(aBuf, bBuf);
  } catch (_) {
    return false;
  }
}

/* ------------------------------------------------------------------ */
/* session issue / verify / revoke                                     */
/* ------------------------------------------------------------------ */

/**
 * Issue a new signed session token.
 *
 * @param {object} [claims]   additional non-sensitive claims (userId, role)
 * @returns {{ token: string, cookie: string, expiresAt: number }}
 */
function issueSession(claims = {}) {
  const now = Date.now();
  const exp = now + TTL_MS;

  const payload = {
    userId: typeof claims.userId === 'string' && claims.userId ? claims.userId : 'operator',
    iat: now,
    exp,
    nonce: crypto.randomBytes(12).toString('hex'),
  };

  const payloadB64 = b64urlEncode(JSON.stringify(payload));
  const signatureB64 = b64urlEncode(sign(payloadB64));
  const token = `${payloadB64}.${signatureB64}`;

  const cookie = buildSetCookie(token, Math.floor(TTL_MS / 1000));

  return { token, cookie, expiresAt: exp };
}

/**
 * Verify a session token string.
 *
 * @param {string} token
 * @returns {{ userId: string, iat: number, exp: number, nonce: string } | null}
 *          null if malformed, tampered, or expired.
 */
function verifySessionToken(token) {
  if (typeof token !== 'string' || token.length === 0) return null;

  const dot = token.indexOf('.');
  if (dot <= 0 || dot >= token.length - 1) return null;

  const payloadB64 = token.slice(0, dot);
  const signatureB64 = token.slice(dot + 1);

  const providedSig = b64urlDecode(signatureB64);
  const expectedSig = sign(payloadB64);
  if (!providedSig || !constantTimeEqual(providedSig, expectedSig)) return null;

  const payloadBuf = b64urlDecode(payloadB64);
  if (!payloadBuf) return null;

  let payload;
  try {
    payload = JSON.parse(payloadBuf.toString('utf8'));
  } catch (_) {
    return null;
  }

  if (!payload || typeof payload !== 'object') return null;
  if (typeof payload.exp !== 'number' || payload.exp <= Date.now()) return null;
  if (typeof payload.iat !== 'number') return null;
  if (typeof payload.userId !== 'string' || !payload.userId) return null;

  return {
    userId: payload.userId,
    iat: payload.iat,
    exp: payload.exp,
    nonce: typeof payload.nonce === 'string' ? payload.nonce : '',
  };
}

/**
 * Revoke a session. In this stateless design, revocation is a no-op on the
 * server (the cookie is dropped client-side by issuing a new empty one).
 * The function exists so the REST layer has a single clear call site, and
 * so a future per-session revocation list can be added without changing
 * callers.
 */
function revokeSession(/* token */) {
  return { cookie: buildClearCookie() };
}

/* ------------------------------------------------------------------ */
/* cookie plumbing                                                     */
/* ------------------------------------------------------------------ */

function cookieAttributes({ maxAgeSeconds }) {
  const attrs = [
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
  ];
  if (config.isProduction) attrs.push('Secure');
  if (Number.isFinite(maxAgeSeconds)) attrs.push(`Max-Age=${Math.floor(maxAgeSeconds)}`);
  return attrs.join('; ');
}

function buildSetCookie(token, maxAgeSeconds) {
  return `${COOKIE_NAME}=${token}; ${cookieAttributes({ maxAgeSeconds })}`;
}

function buildClearCookie() {
  return `${COOKIE_NAME}=; ${cookieAttributes({ maxAgeSeconds: 0 })}`;
}

/**
 * Read the session cookie from a raw `Cookie:` header string.
 * Returns the verified session object or null.
 */
function readSessionFromCookieHeader(cookieHeader) {
  const raw = parseCookieHeader(cookieHeader);
  const token = raw[COOKIE_NAME];
  if (!token) return null;
  return verifySessionToken(token);
}

/**
 * Read the session cookie from an Express request object.
 * Returns the verified session object or null.
 */
function readSessionFromRequest(req) {
  if (!req || !req.headers) return null;
  return readSessionFromCookieHeader(req.headers.cookie || '');
}

/**
 * Set the session cookie on an Express response.
 */
function attachCookieToResponse(res, cookieString) {
  if (!res || typeof res.setHeader !== 'function') return;
  const existing = res.getHeader('Set-Cookie');
  const list = Array.isArray(existing) ? [...existing] : (existing ? [existing] : []);
  list.push(cookieString);
  res.setHeader('Set-Cookie', list);
}

/* ------------------------------------------------------------------ */
/* cookie parsing (RFC 6265 minimal)                                   */
/* ------------------------------------------------------------------ */

function parseCookieHeader(header) {
  const out = {};
  if (typeof header !== 'string' || header.length === 0) return out;

  const parts = header.split(';');
  for (const part of parts) {
    const eq = part.indexOf('=');
    if (eq <= 0) continue;
    const name = part.slice(0, eq).trim();
    if (!name) continue;
    let value = part.slice(eq + 1).trim();
    if (value.startsWith('"') && value.endsWith('"') && value.length >= 2) {
      value = value.slice(1, -1);
    }
    // First occurrence wins — do not let a duplicate shadow a real cookie.
    if (!(name in out)) out[name] = value;
  }
  return out;
}

/* ------------------------------------------------------------------ */
/* constant-time password check                                        */
/* ------------------------------------------------------------------ */

/**
 * Verify the operator password from a login attempt in constant time.
 * Returns true iff the provided value matches OPERATOR_PASSWORD.
 */
function checkOperatorPassword(provided) {
  if (typeof provided !== 'string' || provided.length === 0) return false;
  const a = Buffer.from(provided, 'utf8');
  const b = Buffer.from(config.auth.operatorPassword, 'utf8');
  // Both must be the same length for timingSafeEqual. Pad the shorter to
  // avoid leaking length via early-return by comparing on a fixed-size
  // digest of each.
  const aDigest = crypto.createHash('sha256').update(a).digest();
  const bDigest = crypto.createHash('sha256').update(b).digest();
  return constantTimeEqual(aDigest, bDigest);
}

/* ------------------------------------------------------------------ */
/* exports                                                             */
/* ------------------------------------------------------------------ */

module.exports = {
  COOKIE_NAME,
  issueSession,
  verifySessionToken,
  revokeSession,
  readSessionFromCookieHeader,
  readSessionFromRequest,
  attachCookieToResponse,
  parseCookieHeader,
  checkOperatorPassword,
  buildSetCookie,
  buildClearCookie,
};
