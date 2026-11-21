'use strict';
// Split oversized year partitions into half-year sub-partitions.
//
// Why: GitHub rejects files over 100 MiB, and two shipped partitions
// exceeded that (pp-2002: 105.1 MiB, pp-2006: 103.4 MiB), which blocked
// B6's plain public-repo hosting route for B7. This script splits ONLY
// partitions whose file on disk exceeds 100 MiB into two sub-partitions
// by transfer month (H1 = Jan–Jun, H2 = Jul–Dec), rewrites the manifest
// to describe the sub-partitions (part / month_from / month_to / rows),
// and removes the oversized originals. Row lines are copied byte-for-byte
// in original order — no re-serialisation — and per-year totals are
// asserted against the pre-split manifest before anything is deleted.
//
// Re-runnable after scripts/ingest.js / refresh.js (which regenerate the
// whole-year layout): run this again to re-apply the splits. It refuses
// to run if a partition is already split or a sub-partition file exists.
//
// Usage: node scripts/split-oversized-partitions.js
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');

const DATA_DIR = path.join(__dirname, '..', 'data');
const LIMIT_BYTES = 100 * 1024 * 1024; // GitHub per-file hard limit
const TARGET_BYTES = 95 * 1024 * 1024; // comfort target for split outputs

function writeLine(ws, line) {
  return new Promise((resolve, reject) => {
    if (ws.write(line)) return resolve();
    ws.once('drain', resolve);
  });
}

async function splitPartition(entry) {
  const srcPath = path.join(DATA_DIR, entry.file);
  const year = entry.year;
  const halves = [
    { part: 'H1', monthFrom: `${year}-01`, monthTo: `${year}-06`, file: `pp-${year}-h1.ndjson.gz` },
    { part: 'H2', monthFrom: `${year}-07`, monthTo: `${year}-12`, file: `pp-${year}-h2.ndjson.gz` },
  ];
  for (const h of halves) {
    if (fs.existsSync(path.join(DATA_DIR, h.file))) {
      throw new Error(`${h.file} already exists — refusing to overwrite`);
    }
    h.rows = 0; h.category_a = 0; h.category_b = 0;
    h.min_date = null; h.max_date = null; h.max_price = 0;
    h.gz = zlib.createGzip({ level: 6 }); // same level as scripts/ingest.js
    h.ws = fs.createWriteStream(path.join(DATA_DIR, h.file));
    h.gz.on('error', (e) => { throw e; });
    h.ws.on('error', (e) => { throw e; });
    h.gz.pipe(h.ws);
  }
  const rl = readline.createInterface({
    input: fs.createReadStream(srcPath).pipe(zlib.createGunzip()),
    crlfDelay: Infinity,
  });
  for await (const line of rl) {
    if (!line) continue;
    const r = JSON.parse(line);
    if (r.year !== year) throw new Error(`row year ${r.year} found in ${entry.file} (expected ${year})`);
    const h = r.month <= `${year}-06` ? halves[0] : halves[1];
    if (r.month < h.monthFrom || r.month > h.monthTo) throw new Error(`row month ${r.month} outside ${h.part} range`);
    await writeLine(h.gz, line + '\n');
    h.rows += 1;
    if (r.ppd_category === 'A') h.category_a += 1;
    else if (r.ppd_category === 'B') h.category_b += 1;
    else throw new Error(`unexpected ppd_category ${r.ppd_category}`);
    if (h.min_date === null || r.date < h.min_date) h.min_date = r.date;
    if (h.max_date === null || r.date > h.max_date) h.max_date = r.date;
    if (r.price !== null && r.price > h.max_price) h.max_price = r.price;
  }
  const out = [];
  const sizes = new Map();
  for (const h of halves) {
    await new Promise((resolve, reject) => { h.gz.end(); h.ws.on('finish', resolve); h.ws.on('error', reject); });
    const size = fs.statSync(path.join(DATA_DIR, h.file)).size;
    if (size > TARGET_BYTES) throw new Error(`${h.file} is ${(size / 1048576).toFixed(1)} MiB — still above the 95 MiB target`);
    sizes.set(h.file, size);
    out.push({
      year,
      part: h.part,
      file: h.file,
      month_from: h.monthFrom,
      month_to: h.monthTo,
      raw_rows: h.rows,
      rows: h.rows,
      dupes_within_year: 0,
      category_a: h.category_a,
      category_b: h.category_b,
      min_date: h.min_date,
      max_date: h.max_date,
      max_price: h.max_price,
    });
  }
  out.sizes = sizes;
  // Reconcile against the pre-split manifest entry before replacing it.
  const sum = (k) => out.reduce((s, e) => s + e[k], 0);
  if (sum('rows') !== entry.rows || sum('raw_rows') !== entry.raw_rows
      || sum('category_a') !== entry.category_a || sum('category_b') !== entry.category_b) {
    throw new Error(`split of ${entry.file} does not reconcile with manifest entry — aborting, original kept`);
  }
  if (out[0].min_date !== entry.min_date || out[1].max_date !== entry.max_date) {
    throw new Error(`split of ${entry.file}: date range drift (${out[0].min_date}..${out[1].max_date} vs ${entry.min_date}..${entry.max_date})`);
  }
  return out;
}

(async () => {
  const manifestPath = path.join(DATA_DIR, 'index.json');
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  const oversized = manifest.partitions.filter((p) => {
    if (p.part) return false;
    return fs.statSync(path.join(DATA_DIR, p.file)).size > LIMIT_BYTES;
  });
  if (!oversized.length) { console.log('no oversized partitions — nothing to do'); return; }
  const replacements = new Map(); // year -> [subEntries]
  for (const entry of oversized) {
    console.log(`splitting ${entry.file} (${(fs.statSync(path.join(DATA_DIR, entry.file)).size / 1048576).toFixed(1)} MiB, ${entry.rows} rows)`);
    replacements.set(entry.year, await splitPartition(entry));
  }
  const rebuild = (list) => list.flatMap((p) => replacements.get(p.year) || [p]);
  manifest.partitions = rebuild(manifest.partitions);
  manifest.meta.partitions = rebuild(manifest.meta.partitions);
  manifest.meta.coverage = manifest.meta.coverage.replace(
    'Partitions are by transfer year.',
    'Partitions are by transfer year; oversized years are split into half-year sub-partitions (see part / month_from / month_to on each partition entry).',
  );
  const total = manifest.partitions.reduce((s, p) => s + p.rows, 0);
  if (total !== manifest.total_rows) throw new Error(`manifest total drift: ${total} vs ${manifest.total_rows}`);
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  for (const entry of oversized) {
    fs.unlinkSync(path.join(DATA_DIR, entry.file));
    console.log(`removed oversized original ${entry.file}`);
  }
  for (const subs of replacements.values()) {
    for (const s of subs) console.log(`${s.file}: ${s.rows} rows, ${s.month_from}..${s.month_to}, ${(subs.sizes.get(s.file) / 1048576).toFixed(1)} MiB`);
  }
  console.log(`manifest rewritten: ${manifest.partitions.length} partition entries, total_rows still ${manifest.total_rows}`);
})().catch((e) => { console.error(e); process.exit(1); });
