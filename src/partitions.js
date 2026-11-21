'use strict';
// Partition access for B7. The archive is shipped as gzipped NDJSON
// partitions (data/pp-YYYY.ndjson.gz) plus a manifest (data/index.json).
// Most partitions are one per transfer year; oversized years are split
// into half-year sub-partitions (data/pp-YYYY-h1.ndjson.gz / -h2) so no
// single file exceeds GitHub's 100 MiB limit — see
// scripts/split-oversized-partitions.js. ~29M rows cannot be loaded — or
// shipped — as one JSON array the way B6's ~1M rows could, so partitions
// are streamed line by line and only the partitions a query's month range
// touches are opened (see yearsTouched in lib.js and entryTouched below).
//
// Resolution order for a partition file:
//   1. local data/ directory next to the Actor (bundled or mounted snapshot);
//   2. download from UKPP_SNAPSHOT_BASE_URL (env var, set at publish time to
//      the hosted snapshot location — the B2 hosted-snapshot pattern) into a
//      per-run cache directory, then stream from disk.
// The manifest is resolved the same way (index.json).
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const readline = require('readline');
const zlib = require('zlib');

const LOCAL_DATA_DIR = path.join(__dirname, '..', 'data');

function cacheDir() {
  const d = process.env.UKPP_CACHE_DIR || path.join(require('os').tmpdir(), 'ukpp-snapshot-cache');
  fs.mkdirSync(d, { recursive: true });
  return d;
}

function download(url, dest) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https:') ? https : http;
    client.get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return download(res.headers.location, dest).then(resolve, reject);
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`download ${url}: HTTP ${res.statusCode}`)); }
      const tmp = `${dest}.tmp`;
      const ws = fs.createWriteStream(tmp);
      res.pipe(ws);
      ws.on('finish', () => ws.close(() => { fs.renameSync(tmp, dest); resolve(dest); }));
      ws.on('error', reject);
    }).on('error', reject);
  });
}

async function resolveFile(fileName) {
  const local = path.join(LOCAL_DATA_DIR, fileName);
  if (fs.existsSync(local)) return local;
  const base = process.env.UKPP_SNAPSHOT_BASE_URL;
  if (!base) throw new Error(`partition ${fileName} not found locally and UKPP_SNAPSHOT_BASE_URL is not set`);
  const dest = path.join(cacheDir(), fileName);
  if (!fs.existsSync(dest)) await download(`${base.replace(/\/$/, '')}/${fileName}`, dest);
  return dest;
}

async function loadManifest() {
  const p = await resolveFile('index.json');
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

let manifestCache = null;
function getManifest() {
  if (!manifestCache) manifestCache = loadManifest();
  return manifestCache;
}

function partitionFileName(year) { return `pp-${year}.ndjson.gz`; }

// All manifest partition entries for one transfer year, in stream order.
// Whole-year entries come back as a single entry; split years return their
// sub-partitions (each carrying month_from / month_to) in month order.
async function partitionEntriesFor(year) {
  const manifest = await getManifest();
  return manifest.partitions
    .filter((p) => p.year === year)
    .sort((a, b) => String(a.month_from || a.min_date || '').localeCompare(String(b.month_from || b.min_date || '')));
}

// Does a partition entry's month range intersect the input's month range?
// Sub-partitions declare month_from / month_to in the manifest; whole-year
// entries fall back to their min/max transfer dates. No input (or no month
// range in it) touches everything.
function entryTouched(entry, input) {
  if (!input) return true;
  const from = entry.month_from || (entry.min_date ? entry.min_date.slice(0, 7) : null);
  const to = entry.month_to || (entry.max_date ? entry.max_date.slice(0, 7) : null);
  if (input.monthFrom && to && to < input.monthFrom) return false;
  if (input.monthTo && from && from > input.monthTo) return false;
  return true;
}

async function* streamFile(fileName) {
  const file = await resolveFile(fileName);
  const rl = readline.createInterface({
    input: fs.createReadStream(file).pipe(zlib.createGunzip()),
    crlfDelay: Infinity,
  });
  for await (const line of rl) {
    if (line) yield JSON.parse(line);
  }
}

// Async generator: yields normalised rows for one transfer year, streamed
// (gunzip -> line split -> JSON.parse), never materialising the archive.
// Resolution is transparent to the caller: the year's manifest entries are
// streamed in order, and any sub-partition whose month range does not
// intersect the input's month range is skipped entirely — so a run streams
// exactly the partitions its month range touches, whether they are
// whole-year files or half-year splits.
async function* streamPartition(year, input) {
  const entries = await partitionEntriesFor(year);
  const list = entries.length ? entries : [{ file: partitionFileName(year) }];
  for (const entry of list) {
    if (!entryTouched(entry, input)) continue;
    yield* streamFile(entry.file);
  }
}

module.exports = { loadManifest, streamPartition, partitionFileName, partitionEntriesFor, entryTouched, resolveFile, LOCAL_DATA_DIR };
