/**
 * r2-client.cjs
 *
 * Minimal Cloudflare R2 client for the fearless-scout-worker.
 * Wraps aws4fetch (ESM-only; loaded via dynamic import so this stays CJS)
 * — no AWS SDK bloat. Only the operations Phase 7 needs:
 * putObject (with immutable cache metadata), headObject, deleteObject.
 *
 * Environment variables:
 *   R2_ACCOUNT_ID        — Cloudflare account ID (R2 overview page, top right)
 *   R2_ACCESS_KEY_ID     — R2 API token access key (Object Read & Write,
 *                          scoped to the events bucket only)
 *   R2_SECRET_ACCESS_KEY — R2 API token secret
 *   R2_BUCKET            — bucket name (default: fearless-events)
 *
 * Bucket objects are written with:
 *   Content-Type: application/x-ndjson
 *   Content-Encoding: gzip
 *   Cache-Control: public, max-age=31536000, immutable
 * so the replays-worker CDN (replays-worker/src/index.js) can serve them
 * edge-cached straight to the browser. The browser fetch() transparently
 * decompresses `Content-Encoding: gzip` and sees the JSONL text.
 *
 * Usage:
 *   const r2 = require('./r2-client.cjs');
 *   await r2.putObject('events/{ws}/{series}/{game}.jsonl.gz', gzBuffer);
 */

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || "";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || "";
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || "";
const R2_BUCKET = process.env.R2_BUCKET || "fearless-events";

const R2_URL = `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
const DEFAULT_CACHE_CONTROL = "public, max-age=31536000, immutable";

let awsClientPromise = null;

function getAwsClient() {
  if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
    throw new Error(
      "[R2] Missing env vars: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY",
    );
  }
  if (!awsClientPromise) {
    awsClientPromise = import("aws4fetch").then(
      ({ AwsClient }) =>
        new AwsClient({
          service: "s3",
          region: "auto",
          accessKeyId: R2_ACCESS_KEY_ID,
          secretAccessKey: R2_SECRET_ACCESS_KEY,
        }),
    );
  }
  return awsClientPromise;
}

/**
 * Encode a key for use in a URL path while preserving the `/` separators.
 * Keys are `events/{workspaceId}/{seriesId}/{gameNumber}.jsonl.gz`.
 */
function encodeKey(key) {
  return key
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function objectUrl(key) {
  return `${R2_URL}/${R2_BUCKET}/${encodeKey(key)}`;
}

function errorMessage(res, operation, key) {
  return res.text().then(
    (body) =>
      `[R2] ${operation} "${key}" failed: ${res.status} ${res.statusText}${
        body ? ` — ${body.slice(0, 300)}` : ""
      }`,
  );
}

/**
 * Upload an object. Defaults target the gutted-JSONL contract.
 *
 * @param {string} key              e.g. `events/{ws}/{series}/{game}.jsonl.gz`
 * @param {Buffer|Uint8Array} body  gzipped bytes
 * @param {object} [opts]
 * @param {string} [opts.contentType]       default `application/x-ndjson`
 * @param {string} [opts.contentEncoding]   default `gzip`
 * @param {string} [opts.cacheControl]      default immutable public
 * @returns {Promise<{status: number, etag: string|null}>}
 */
async function putObject(key, body, opts = {}) {
  const client = await getAwsClient();
  const headers = {
    "Content-Type": opts.contentType || "application/x-ndjson",
    "Cache-Control": opts.cacheControl || DEFAULT_CACHE_CONTROL,
  };
  if ((opts.contentEncoding ?? "gzip") !== "") {
    headers["Content-Encoding"] = opts.contentEncoding ?? "gzip";
  }
  const res = await client.fetch(objectUrl(key), {
    method: "PUT",
    headers,
    body,
  });
  if (!res.ok) throw new Error(await errorMessage(res, "putObject", key));
  return { status: res.status, etag: res.headers.get("etag") };
}

/**
 * @param {string} key
 * @returns {Promise<{status: number, etag: string|null, contentType: string|null,
 *                    contentEncoding: string|null, cacheControl: string|null}|null>}
 */
async function headObject(key) {
  const client = await getAwsClient();
  const res = await client.fetch(objectUrl(key), { method: "HEAD" });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(await errorMessage(res, "headObject", key));
  return {
    status: res.status,
    etag: res.headers.get("etag"),
    contentType: res.headers.get("content-type"),
    contentEncoding: res.headers.get("content-encoding"),
    cacheControl: res.headers.get("cache-control"),
    contentLength: parseInt(res.headers.get("content-length") || "0", 10) || null,
  };
}

/**
 * @param {string} key
 * @returns {Promise<boolean>} true when the object was deleted
 */
async function deleteObject(key) {
  const client = await getAwsClient();
  const res = await client.fetch(objectUrl(key), { method: "DELETE" });
  if (!res.ok) throw new Error(await errorMessage(res, "deleteObject", key));
  return true;
}

module.exports = { putObject, headObject, deleteObject, R2_BUCKET };
