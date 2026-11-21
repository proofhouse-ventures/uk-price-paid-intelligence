'use strict';
// Tests for B7. Run: npm test
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const zlib = require('zlib');
const readline = require('readline');
const { parseCsvLine, normaliseRecord, filterTransactions, computeAnalytics, median, postcodeParts, yearsTouched } = require('../src/lib');

const fx = (n) => JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', n), 'utf8'));
let passed = 0;
function check(name, fn) { fn(); passed += 1; console.log(`ok - ${name}`); }
async function acheck(name, fn) { await fn(); passed += 1; console.log(`ok - ${name}`); }

const raw = fx('raw_extract.json');
const rows = raw.map(normaliseRecord);

check('normalisation: types converted, braces stripped, date sliced, labels added', () => {
  assert.strictEqual(rows[0].transaction_id, 'AAA-0001');
  assert.strictEqual(rows[0].price, 320000);
  assert.strictEqual(rows[0].date, '2024-03-20');
  assert.strictEqual(rows[0].month, '2024-03');
  assert.strictEqual(rows[0].year, 2024);
  assert.strictEqual(rows[0].property_type_label, 'Detached');
  assert.strictEqual(rows[0].duration_label, 'Freehold');
  assert.strictEqual(rows[2].saon, 'FLAT 1');
  assert.strictEqual(rows[4].postcode, null); // empty postcode stays null, not ""
});

check('postcode parts derived; missing/malformed postcodes give nulls', () => {
  assert.deepStrictEqual(postcodeParts('NR9 5FP'), { postcode_area: 'NR', postcode_district: 'NR9', postcode_sector: 'NR9 5' });
  assert.deepStrictEqual(postcodeParts('SW1A 1AA'), { postcode_area: 'SW', postcode_district: 'SW1A', postcode_sector: 'SW1A 1' });
  assert.deepStrictEqual(postcodeParts(''), { postcode_area: null, postcode_district: null, postcode_sector: null });
  assert.deepStrictEqual(postcodeParts(null), { postcode_area: null, postcode_district: null, postcode_sector: null });
  assert.strictEqual(rows[0].postcode_district, 'NR9');
  assert.strictEqual(rows[1].postcode_district, 'NR20');
});

check('parseCsvLine handles quoted commas and escaped quotes', () => {
  const line = '"{X1}","100000","2020-01-01 00:00","L1 2AB","F","N","L","FLAT 1, BLOCK A","","STREET ""Q""","","TOWN","DIST","COUNTY","A","A"';
  const f = parseCsvLine(line);
  assert.strictEqual(f.length, 16);
  assert.strictEqual(f[7], 'FLAT 1, BLOCK A');
  assert.strictEqual(f[9], 'STREET "Q"');
});

check('median unit cases', () => {
  assert.strictEqual(median([3, 1, 2]), 2);
  assert.strictEqual(median([4, 1, 3, 2]), 2.5);
  assert.strictEqual(median([]), null);
});

check('filters compose: county + property type (code and label) + category', () => {
  assert.strictEqual(filterTransactions(rows, { county: 'norfolk' }).length, 5);
  assert.strictEqual(filterTransactions(rows, { county: 'NORFOLK', propertyType: 'D' }).length, 3);
  assert.strictEqual(filterTransactions(rows, { county: 'NORFOLK', propertyType: 'detached' }).length, 3);
  assert.strictEqual(filterTransactions(rows, { county: 'NORFOLK', propertyType: 'D', ppdCategory: 'A' }).length, 2);
  assert.strictEqual(filterTransactions(rows, { tenure: 'leasehold' }).length, 2);
  assert.strictEqual(filterTransactions(rows, { tenure: 'L' }).length, 2);
  assert.strictEqual(filterTransactions(rows, { oldNew: 'Y' }).length, 2);
});

check('filters: month range, price range, postcode parts are exact-part matches', () => {
  assert.strictEqual(filterTransactions(rows, { monthFrom: '2024-01', monthTo: '2024-12' }).length, 4);
  assert.strictEqual(filterTransactions(rows, { priceMin: 300000 }).length, 3);
  assert.strictEqual(filterTransactions(rows, { postcodeArea: 'NR' }).length, 5);
  assert.strictEqual(filterTransactions(rows, { postcodeDistrict: 'NR9' }).length, 3);
  assert.strictEqual(filterTransactions(rows, { postcodeDistrict: 'NR2' }).length, 0); // must not substring-match NR20
  assert.strictEqual(filterTransactions(rows, { postcodeSector: 'NR9 5' }).length, 3); // 5FP x2 + 5AA x1
});

check('analytics: medians and shares hand-verified on fixture', () => {
  const an = computeAnalytics(rows);
  assert.strictEqual(an.rows_analysed, 6);
  assert.strictEqual(an.overall.median_price, 305500); // (291000+320000)/2
  assert.strictEqual(an.overall.mean_price, 310166.67);
  assert.strictEqual(an.overall.new_build_share, 0.33);
  const norf = an.by_county.find((c) => c.county === 'NORFOLK');
  assert.strictEqual(norf.transactions, 5);
  assert.strictEqual(norf.median_price, 320000);
  const det = an.by_property_type.find((t) => t.property_type === 'D');
  assert.strictEqual(det.median_price, 320000);
  const nr9 = an.by_postcode_district.find((d) => d.postcode_district === 'NR9');
  assert.strictEqual(nr9.transactions, 3);
  assert.strictEqual(an.volume_by_year['2024'], 4);
  assert.strictEqual(an.volume_by_month['2024-03'], 2);
  const catB = an.by_ppd_category.find((c) => c.ppd_category === 'B');
  assert.strictEqual(catB.transactions, 1);
});

check('yearsTouched selects partitions from the month range only', () => {
  const all = [1995, 2000, 2020, 2024, 2026];
  assert.deepStrictEqual(yearsTouched({}, all), all);
  assert.deepStrictEqual(yearsTouched({ monthFrom: '2020-06', monthTo: '2024-02' }, all), [2020, 2024]);
  assert.deepStrictEqual(yearsTouched({ monthFrom: '2023-01' }, all), [2024, 2026]);
});

(async () => {
  await acheck('shipped snapshot: manifest totals reconcile and one partition stream-counts exactly', async () => {
    const dataDir = path.join(__dirname, '..', 'data');
    const manifest = JSON.parse(fs.readFileSync(path.join(dataDir, 'index.json'), 'utf8'));
    const EXPECTED_TOTAL = 31702685; // from the 2026-11-21 ingest — see build report
    assert.strictEqual(manifest.total_rows, EXPECTED_TOTAL);
    // PriceBag invariant (lib.js): prices are stored in Uint32Arrays, so the
    // archive maximum must stay below 2^32 — assert it from the manifest.
    const globalMaxPrice = Math.max(...manifest.partitions.map((p) => p.max_price));
    assert.ok(globalMaxPrice < 4294967296, `max price ${globalMaxPrice} exceeds Uint32 range`);
    const sum = manifest.partitions.reduce((s, p) => s + p.rows, 0);
    assert.strictEqual(sum, manifest.total_rows);
    assert.ok(manifest.meta.attribution.includes('Crown copyright and database right 2021'));
    // Stream-count the smallest partition and compare with the manifest.
    const smallest = [...manifest.partitions].sort((a, b) => a.rows - b.rows)[0];
    const rl = readline.createInterface({
      input: fs.createReadStream(path.join(dataDir, smallest.file)).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    });
    let n = 0;
    let first = null;
    for await (const line of rl) { if (line) { n++; if (!first) first = JSON.parse(line); } }
    assert.strictEqual(n, smallest.rows);
    const expectedKeys = ['transaction_id', 'price', 'date', 'month', 'year', 'postcode', 'postcode_area', 'postcode_district', 'postcode_sector', 'property_type', 'property_type_label', 'old_new', 'duration', 'duration_label', 'paon', 'saon', 'street', 'locality', 'town_city', 'district', 'county', 'ppd_category', 'record_status', 'source_dataset'].sort();
    assert.deepStrictEqual(Object.keys(first).sort(), expectedKeys);
    const blob = JSON.stringify(first);
    assert.ok(!/[\w.+-]+@[\w-]+\.[\w.]+/.test(blob), 'email-like string in snapshot row');
  });
  console.log(`\n${passed} tests passed`);
})().catch((e) => { console.error(e); process.exit(1); });
