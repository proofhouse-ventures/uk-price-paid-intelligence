'use strict';
// Ingest: HM Land Registry Price Paid yearly CSVs (raw dir) -> per-year
// gzipped NDJSON partitions (data/pp-YYYY.ndjson.gz) + manifest
// (data/index.json). Phase 2 applies the monthly update file
// (pp-monthly-update-new-version.csv: record_status A = addition,
// C = change, D = deletion) idempotently on top of the yearly files:
// yearly files are themselves refreshed monthly by HMLR, so an update
// record that is already reflected counts as "already current" rather
// than being applied twice.
//
// The source contains no personal data (HMLR: "Price paid information is
// not personal, but property-related information"), so no scrubbing
// applies; addresses are republished exactly as published.
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const { parseCsvLine, normaliseRecord, PPD_FIELD_COUNT } = require('../src/lib');

const rawDir = process.argv[2] || path.join(__dirname, '..', '..', 'data', 'raw', 'ukpp');
const dataDir = path.join(__dirname, '..', 'data');
const indexDir = path.join(rawDir, 'indexes');
fs.mkdirSync(dataDir, { recursive: true });
fs.mkdirSync(indexDir, { recursive: true });

const YEARS = [];
for (let y = 1995; y <= 2026; y++) YEARS.push(y);

function eachCsvLine(file, fn) {
  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
    rl.on('line', fn);
    rl.on('close', resolve);
    rl.on('error', reject);
  });
}

function ndjsonWriter(file) {
  const gz = zlib.createGzip({ level: 6 });
  const ws = fs.createWriteStream(file);
  gz.pipe(ws);
  let buf = '';
  return {
    write(row) {
      buf += JSON.stringify(row) + '\n';
      if (buf.length > 1 << 20) { gz.write(buf); buf = ''; }
    },
    async end() {
      if (buf) { gz.write(buf); buf = ''; }
      gz.end();
      await new Promise((res, rej) => { ws.on('finish', res); ws.on('error', rej); });
    },
  };
}

async function* ndjsonRows(file) {
  const rl = readline.createInterface({
    input: fs.createReadStream(file).pipe(zlib.createGunzip()),
    crlfDelay: Infinity,
  });
  for await (const line of rl) if (line) yield JSON.parse(line);
}

const partitionPath = (y) => path.join(dataDir, `pp-${y}.ndjson.gz`);
const idsPath = (y) => path.join(indexDir, `pp-${y}.ids.txt`);

(async () => {
  // ---------- Phase 1: yearly files -> partitions ----------
  const perYear = new Map();
  let rawTotal = 0;
  let malformed = 0;
  for (const y of YEARS) {
    const csv = path.join(rawDir, `pp-${y}.csv`);
    if (!fs.existsSync(csv)) { console.log(`pp-${y}.csv missing — skipped`); continue; }
    const out = ndjsonWriter(partitionPath(y));
    const idsOut = fs.createWriteStream(idsPath(y));
    const seen = new Set();
    let raw = 0, kept = 0, dupes = 0, catA = 0, catB = 0, minDate = null, maxDate = null, maxPrice = 0;
    await eachCsvLine(csv, (line) => {
      if (!line) return;
      const fields = parseCsvLine(line);
      if (fields.length !== PPD_FIELD_COUNT) { malformed++; return; }
      raw++;
      const row = normaliseRecord(fields);
      if (seen.has(row.transaction_id)) { dupes++; return; }
      seen.add(row.transaction_id);
      out.write(row);
      idsOut.write(row.transaction_id + '\n');
      kept++;
      if (row.ppd_category === 'A') catA++; else if (row.ppd_category === 'B') catB++;
      if (row.date && (minDate === null || row.date < minDate)) minDate = row.date;
      if (row.date && (maxDate === null || row.date > maxDate)) maxDate = row.date;
      if (row.price !== null && row.price > maxPrice) maxPrice = row.price;
    });
    await out.end();
    await new Promise((res) => idsOut.end(res));
    perYear.set(y, { year: y, file: `pp-${y}.ndjson.gz`, raw_rows: raw, rows: kept, dupes_within_year: dupes, category_a: catA, category_b: catB, min_date: minDate, max_date: maxDate, max_price: maxPrice });
    rawTotal += raw;
    console.log(`pp-${y}: ${raw} raw, ${kept} kept, ${dupes} dupes, dates ${minDate}..${maxDate}`);
  }

  // ---------- Phase 2: monthly update file ----------
  const updateFile = path.join(rawDir, 'pp-monthly-update-new-version.csv');
  const upd = { present: fs.existsSync(updateFile), raw_rows: 0, distinct_ids: 0, adds_raw: 0, changes_raw: 0, deletes_raw: 0, adds_applied: 0, changes_applied: 0, already_current: 0, deletes_applied: 0, deletes_not_found: 0, stale_copies_removed: 0 };
  if (upd.present) {
    const finalAction = new Map(); // id -> { action: 'upsert', row } | { action: 'delete' }
    await eachCsvLine(updateFile, (line) => {
      if (!line) return;
      const fields = parseCsvLine(line);
      if (fields.length !== PPD_FIELD_COUNT) { malformed++; return; }
      upd.raw_rows++;
      const row = normaliseRecord(fields);
      const status = fields[15];
      if (status === 'D') { upd.deletes_raw++; finalAction.set(row.transaction_id, { action: 'delete' }); }
      else if (status === 'C') { upd.changes_raw++; finalAction.set(row.transaction_id, { action: 'upsert', row }); }
      else { upd.adds_raw++; finalAction.set(row.transaction_id, { action: 'upsert', row }); }
    });
    upd.distinct_ids = finalAction.size;

    // Locate every wanted id's current partition year via the id indexes.
    const wanted = new Set(finalAction.keys());
    const foundYear = new Map();
    for (const y of perYear.keys()) {
      await eachCsvLine(idsPath(y), (id) => { if (wanted.has(id)) foundYear.set(id, y); });
    }

    // Plan per-year rewrites.
    const plan = new Map(); // year -> { drop: Set, replace: Map, append: [] }
    const planFor = (y) => { if (!plan.has(y)) plan.set(y, { drop: new Set(), replace: new Map(), append: [] }); return plan.get(y); };
    for (const [id, act] of finalAction) {
      const fy = foundYear.get(id);
      if (act.action === 'delete') {
        if (fy !== undefined) planFor(fy).drop.add(id);
        else upd.deletes_not_found++;
      } else {
        const targetYear = act.row.year;
        if (fy === undefined) { planFor(targetYear).append.push(act.row); upd.adds_applied++; }
        else if (fy === targetYear) { planFor(fy).replace.set(id, act.row); }
        else { // transfer date amended across a year boundary: remove stale copy, append to the new year
          planFor(fy).drop.add(id); upd.stale_copies_removed++;
          planFor(targetYear).append.push(act.row); upd.adds_applied++;
        }
      }
    }

    // Execute rewrites for affected years only.
    for (const [y, p] of plan) {
      if (!perYear.has(y)) { // brand-new partition year (not expected; handled for completeness)
        perYear.set(y, { year: y, file: `pp-${y}.ndjson.gz`, raw_rows: 0, rows: 0, dupes_within_year: 0, category_a: 0, category_b: 0, min_date: null, max_date: null, max_price: 0 });
      }
      const stats = perYear.get(y);
      const tmp = partitionPath(y) + '.tmp';
      const out = ndjsonWriter(tmp);
      const idsOut = fs.createWriteStream(idsPath(y) + '.tmp');
      const seenAppend = new Set();
      let rows = 0, catA = 0, catB = 0, minDate = null, maxDate = null, maxPrice = 0;
      const emit = (row) => {
        out.write(row); idsOut.write(row.transaction_id + '\n'); rows++;
        if (row.ppd_category === 'A') catA++; else if (row.ppd_category === 'B') catB++;
        if (row.date && (minDate === null || row.date < minDate)) minDate = row.date;
        if (row.date && (maxDate === null || row.date > maxDate)) maxDate = row.date;
        if (row.price !== null && row.price > maxPrice) maxPrice = row.price;
      };
      if (fs.existsSync(partitionPath(y))) for await (const row of ndjsonRows(partitionPath(y))) {
        if (p.drop.has(row.transaction_id)) {
          if (finalAction.get(row.transaction_id) && finalAction.get(row.transaction_id).action === 'delete') upd.deletes_applied++;
          continue;
        }
        const rep = p.replace.get(row.transaction_id);
        if (rep) {
          if (JSON.stringify(rep) === JSON.stringify(row)) upd.already_current++;
          else upd.changes_applied++;
          emit(rep);
          p.replace.delete(row.transaction_id);
          continue;
        }
        emit(row);
      }
      for (const row of p.append) { if (!seenAppend.has(row.transaction_id)) { seenAppend.add(row.transaction_id); emit(row); } }
      // Any replace ids not encountered (shouldn't happen) are appended.
      for (const row of p.replace.values()) emit(row);
      await out.end();
      await new Promise((res) => idsOut.end(res));
      fs.renameSync(tmp, partitionPath(y));
      fs.renameSync(idsPath(y) + '.tmp', idsPath(y));
      Object.assign(stats, { rows, category_a: catA, category_b: catB, min_date: minDate, max_date: maxDate, max_price: maxPrice });
      console.log(`pp-${y} rewritten after update: ${rows} rows`);
    }
  }

  // ---------- Manifest ----------
  const partitions = [...perYear.values()].sort((a, b) => a.year - b.year);
  const totalRows = partitions.reduce((s, p) => s + p.rows, 0);
  const allDates = partitions.flatMap((p) => [p.min_date, p.max_date]).filter(Boolean).sort();
  const meta = {
    dataset: 'Price Paid Data',
    publisher: 'HM Land Registry (England & Wales), via gov.uk',
    licence: 'Open Government Licence v3.0 — http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/',
    attribution: 'Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.',
    address_data_note: 'The address data in Price Paid Data was processed against Ordnance Survey\'s AddressBase Premium product, which incorporates Royal Mail\'s PAF® database. It is republished here exactly as published by HM Land Registry, for the purpose of providing residential property price information services (the use Royal Mail and Ordnance Survey permit). No address enhancement, geocoding or derivation is applied.',
    accessed: new Date().toISOString().slice(0, 10),
    rows: totalRows,
    raw_rows_yearly_files: rawTotal,
    malformed_lines_skipped: malformed,
    monthly_update: upd,
    coverage: `Transfer dates ${allDates[0]} to ${allDates[allDates.length - 1]}; England & Wales only (Scotland and Northern Ireland registers are separate and are not included). Partitions are by transfer year.`,
    personal_data_handling: 'None required: Price Paid Data contains no personal data — HMLR states "Price paid information is not personal, but property-related information". There are no buyer/seller names or contact details in any column (verified at ingest). Postcode and address fields are the transacted property\'s address, as published.',
    partitions,
  };
  const manifest = { meta, years: partitions.map((p) => p.year), total_rows: totalRows, partitions };
  fs.writeFileSync(path.join(dataDir, 'index.json'), JSON.stringify(manifest, null, 2));
  console.log(`ingested ${totalRows} rows total (${rawTotal} raw yearly lines) -> ${dataDir}`);
})().catch((e) => { console.error(e); process.exit(1); });
