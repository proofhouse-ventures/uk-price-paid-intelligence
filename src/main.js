'use strict';
// B7 — UK Price Paid Intelligence Actor (dataset-only v1).
// Runs on Apify (uses the `apify` SDK if present) or standalone for local
// testing (no account needed): `node src/main.js` reads input from
// ./input.json or APIFY_INPUT env, writes ./output.json.
//
// ARCHITECTURE (differs from B6, deliberately): the archive is ~31.7M rows,
// so it is NOT bundled as a single gzipped JSON array. It ships as gzipped
// NDJSON partitions keyed by transfer year (oversized years split into
// half-year sub-partitions — see partitions.js); a run opens only the
// partitions its month range touches and streams them row by row. A run
// with no month range touches every partition — sequential streaming,
// bounded memory, linear time (see README "Architecture and run
// behaviour"). Analytics run in TWO streaming passes (one per accumulator
// family — see lib.js) so exact medians never require holding a price
// array for every grouping at once; a single-pass version OOM-killed at
// the default ~2 GB V8 heap on a full-archive pass during local testing.
//
// PUSH PATH (hard platform lesson from B6's root-cause runs, 2026-11-21):
// the Apify SDK's pushData silently drops ~99.6% of bulk items on-platform
// past ~4,24x stored items while reporting success — batching SDK calls
// does NOT fix it. The verified fix (B6 build 0.0.5, run
// dhOVOUSUX5SnKkxps, which stored all 989,625 items) is to bypass the SDK
// write path entirely: on-platform, rows are pushed in 1,000-item chunks
// straight to the public dataset API (POST /v2/datasets/<id>/items) with
// per-chunk HTTP status checks and 100 ms pacing, then the stored item
// count is verified via GET /v2/datasets/<id> and the run fails loudly on
// any mismatch. The local standalone path keeps using the local push stub.
// Do not revert to SDK pushData for on-platform pushes.
const fs = require('fs');
const path = require('path');
const { matchesFilters, yearsTouched, createAnalyticsAccumulator, mergeAnalytics } = require('./lib');
const { loadManifest, streamPartition } = require('./partitions');

const PUSH_BATCH = 1000;

async function getRuntime() {
  try {
    const { Actor } = require('apify');
    await Actor.init();
    return {
      getInput: () => Actor.getInput(),
      pushData: (items) => Actor.pushData(items),
      setValue: (k, v) => Actor.setValue(k, v),
      exit: () => Actor.exit(),
      onPlatform: true,
    };
  } catch (e) {
    return {
      getInput: async () => {
        if (process.env.APIFY_INPUT) return JSON.parse(process.env.APIFY_INPUT);
        const p = path.join(process.cwd(), 'input.json');
        return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : {};
      },
      pushData: async (items) => { module.exports._localDataset = (module.exports._localDataset || []).concat(items); },
      setValue: async (k, v) => { module.exports._localKV = module.exports._localKV || {}; module.exports._localKV[k] = v; },
      exit: async () => {},
      onPlatform: false,
    };
  }
}

async function main() {
  const rt = await getRuntime();
  const input = (await rt.getInput()) || {};
  const mode = input.mode || 'both'; // rows | analytics | both
  const wantRows = mode === 'rows' || mode === 'both';
  const wantAnalytics = mode === 'analytics' || mode === 'both';
  const limit = Number.isInteger(input.limit) && input.limit > 0 ? input.limit : Infinity;

  const manifest = await loadManifest();
  const years = yearsTouched(input, manifest.years);
  const acc1 = wantAnalytics ? createAnalyticsAccumulator(1) : null;
  const acc2 = wantAnalytics ? createAnalyticsAccumulator(2) : null;

  let pushed = 0;
  let matched = 0;
  let batch = [];
  let batchCount = 0;

  // On-platform direct dataset-API push state (see PUSH PATH note above).
  // Local standalone runs keep using rt.pushData (the local stub).
  const apiToken = rt.onPlatform ? process.env.APIFY_TOKEN : null;
  const apiDatasetId = rt.onPlatform ? process.env.APIFY_DEFAULT_DATASET_ID : null;
  if (wantRows && rt.onPlatform && (!apiToken || !apiDatasetId)) {
    throw new Error('Missing APIFY_TOKEN or APIFY_DEFAULT_DATASET_ID in run env');
  }
  const pushBatchDirect = async (items) => {
    const res = await fetch('https://api.apify.com/v2/datasets/' + apiDatasetId + '/items', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + apiToken },
      body: JSON.stringify(items),
    });
    if (!res.ok) throw new Error('Direct push failed at item ' + pushed + ': HTTP ' + res.status + ' ' + (await res.text()).slice(0, 200));
    await new Promise((r) => setTimeout(r, 100));
  };
  const flush = async () => {
    if (!batch.length) return;
    const items = batch;
    batch = [];
    if (rt.onPlatform) {
      await pushBatchDirect(items);
    } else {
      await rt.pushData(items);
    }
    pushed += items.length;
    batchCount += 1;
    if (rt.onPlatform && batchCount % 100 === 1) console.log('DIAG pushed_up_to=' + pushed);
  };

  // Pass 1: rows (if wanted) + analytics family 1.
  outer:
  for (const year of years) {
    for await (const row of streamPartition(year, input)) {
      if (!matchesFilters(row, input)) continue;
      matched += 1;
      if (acc1) acc1.add(row);
      if (wantRows && pushed + batch.length < limit) {
        batch.push(row);
        if (batch.length >= PUSH_BATCH) await flush();
      }
      // Early exit only when rows are the sole output and the limit is met;
      // analytics must always see the full filtered set.
      if (wantRows && !wantAnalytics && pushed + batch.length >= limit) break outer;
    }
  }
  await flush();

  if (wantRows && rt.onPlatform) {
    // Stored-count verification (B6 pattern): direct pushes return HTTP
    // success per chunk, but the run must not be trusted until the dataset
    // service's stored count equals the total pushed. Fail loudly on any
    // mismatch — never exit silently on a short count.
    console.log('DIAG push_loop_complete total=' + pushed);
    let landed = 0;
    for (let p = 0; p < 24; p++) {
      const infoRes = await fetch('https://api.apify.com/v2/datasets/' + apiDatasetId, { headers: { Authorization: 'Bearer ' + apiToken } });
      const info = await infoRes.json();
      landed = info && info.data ? info.data.itemCount : 0;
      console.log('DIAG dataset_items=' + landed + ' target=' + pushed);
      if (landed >= pushed) break;
      await new Promise((r) => setTimeout(r, 5000));
    }
    if (landed < pushed) {
      throw new Error('Dataset write incomplete after direct pushes: ' + landed + ' of ' + pushed + ' items stored');
    }
    console.log('DIAG dataset_write_landing_complete items=' + landed);
  }

  // Pass 2: analytics family 2 (second streaming pass over the same
  // partitions — the price of bounded memory; see lib.js).
  if (acc2) {
    for (const year of years) {
      for await (const row of streamPartition(year, input)) {
        if (matchesFilters(row, input)) acc2.add(row);
      }
    }
  }

  let analytics = null;
  if (wantAnalytics) {
    analytics = mergeAnalytics(acc1.finalize(), acc2.finalize());
    analytics.meta = manifest.meta;
    await rt.setValue('ANALYTICS', analytics);
  }
  if (!rt.onPlatform) {
    fs.writeFileSync(path.join(process.cwd(), 'output.json'), JSON.stringify({ dataset: module.exports._localDataset || [], kv: module.exports._localKV || {} }, null, 2));
    console.log(`standalone run: ${pushed} rows pushed, analytics=${analytics ? 'yes' : 'no'}, matched=${matched}, years touched=${years.length} (${years[0]}..${years[years.length - 1]})`);
  }
  await rt.exit();
  return { pushed, matched, analytics, years };
}

if (require.main === module) main().catch((e) => { console.error(e); process.exit(1); });
module.exports = { main };
