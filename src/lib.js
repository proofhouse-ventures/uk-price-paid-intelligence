'use strict';
// B7 core logic — pure functions, no Apify dependency. Shared by main.js,
// partitions.js consumers and tests.
// Scope: HM Land Registry "Price Paid Data" (England & Wales), Open Government
// Licence v3.0. The dataset carries NO personal data: HMLR states "Price paid
// information is not personal, but property-related information." Columns are
// transaction + property-address attributes only (price, date, postcode,
// PAON/SAON, street, locality, town, district, county, property type, tenure).
// There are no buyer/seller names, no IDs of persons, no contact details.
//
// Attribution (required by HMLR under OGL v3.0):
//   Contains HM Land Registry data © Crown copyright and database right 2021.
//   This data is licensed under the Open Government Licence v3.0.
// Address data in PPD was processed against Ordnance Survey AddressBase
// Premium / Royal Mail PAF; it is republished here exactly as published by
// HMLR, for the purpose of providing residential property price information
// services (the use OS/Royal Mail permit). No address enhancement is applied.

// Parse one CSV line with RFC4180 quoting. PPD fields are double-quoted and
// may contain commas inside quotes (e.g. PAON "FLAT 1, 10 HIGH STREET" style
// values); embedded newlines do not occur in PPD files.
function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } else inQ = false;
      } else cur += c;
    } else if (c === '"') inQ = true;
    else if (c === ',') { out.push(cur); cur = ''; }
    else cur += c;
  }
  out.push(cur);
  return out;
}

// PPD column order (16 fields, headerless files):
//  0 transaction_id  1 price        2 transfer_date  3 postcode
//  4 property_type   5 old_new      6 duration       7 paon
//  8 saon            9 street      10 locality      11 town_city
// 12 district       13 county      14 ppd_category 15 record_status
const PPD_FIELD_COUNT = 16;

const PROPERTY_TYPE_LABELS = { D: 'Detached', S: 'Semi-detached', T: 'Terraced', F: 'Flats/Maisonettes', O: 'Other' };
const DURATION_LABELS = { F: 'Freehold', L: 'Leasehold' };

function toIntOrNull(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

// Split a UK postcode into area / district (outward) / sector parts.
// "NR9 5FP" -> { area: "NR", district: "NR9", sector: "NR9 5" }.
// Missing or malformed postcodes (they occur, mostly in early years) -> nulls.
function postcodeParts(postcode) {
  if (!postcode) return { postcode_area: null, postcode_district: null, postcode_sector: null };
  const pc = String(postcode).toUpperCase().trim().replace(/\s+/g, ' ');
  const m = pc.match(/^([A-Z]{1,2})(\d[A-Z\d]?)\s*(\d)?/);
  if (!m) return { postcode_area: null, postcode_district: null, postcode_sector: null };
  const area = m[1];
  const district = m[1] + m[2];
  const sector = m[3] ? `${district} ${m[3]}` : null;
  return { postcode_area: area, postcode_district: district, postcode_sector: sector };
}

// Normalise one raw PPD record (array of 16 strings, or an object keyed by
// the raw column names) into the clean store shape.
function normaliseRecord(r) {
  const f = Array.isArray(r) ? r : [
    r.transaction_id, r.price, r.transfer_date, r.postcode, r.property_type,
    r.old_new, r.duration, r.paon, r.saon, r.street, r.locality, r.town_city,
    r.district, r.county, r.ppd_category, r.record_status,
  ];
  const date = f[2] ? String(f[2]).slice(0, 10) : null; // "2024-03-20 00:00" -> "2024-03-20"
  const postcode = f[3] ? String(f[3]).toUpperCase().trim() : null;
  const parts = postcodeParts(postcode);
  const propertyType = f[4] || null;
  const duration = f[6] || null;
  return {
    transaction_id: f[0] ? String(f[0]).replace(/^\{|\}$/g, '') : null, // braces are a file-format artefact; the bare GUID is HMLR's linked-data form
    price: toIntOrNull(f[1]), // GBP, whole pounds as published
    date,
    month: date ? date.slice(0, 7) : null, // YYYY-MM
    year: date ? Number(date.slice(0, 4)) : null,
    postcode,
    postcode_area: parts.postcode_area,
    postcode_district: parts.postcode_district,
    postcode_sector: parts.postcode_sector,
    property_type: propertyType, // D/S/T/F/O as published
    property_type_label: PROPERTY_TYPE_LABELS[propertyType] || null,
    old_new: f[5] || null, // Y = new build, N = established dwelling
    duration, // F = Freehold, L = Leasehold (the tenure field)
    duration_label: DURATION_LABELS[duration] || null,
    paon: f[7] || '',
    saon: f[8] || '',
    street: f[9] || '',
    locality: f[10] || '',
    town_city: f[11] || '',
    district: f[12] || '',
    county: f[13] || '',
    ppd_category: f[14] || null, // A = Standard, B = Additional (see README)
    record_status: f[15] || 'A', // in the shipped snapshot every row is a live record ('A'); deletions/changes are applied at ingest
    source_dataset: 'Price Paid Data (HM Land Registry)',
  };
}

// Does one normalised row match the input filters? (Single-row predicate so
// the Actor can stream partitions without materialising the archive.)
function matchesFilters(r, input) {
  const ci = (v) => (v ? String(v).toLowerCase() : null);
  const county = ci(input.county);
  const district = ci(input.district);
  const townCity = ci(input.townCity);
  if (county && !r.county.toLowerCase().includes(county)) return false;
  if (district && !r.district.toLowerCase().includes(district)) return false;
  if (townCity && !r.town_city.toLowerCase().includes(townCity)) return false;
  // Postcode parts are exact-part matches (case-insensitive), not substrings:
  // area "NR" must not bleed into district logic, district "NR9" != "NR9x".
  if (input.postcodeArea && (r.postcode_area || '') !== String(input.postcodeArea).toUpperCase().trim()) return false;
  if (input.postcodeDistrict && (r.postcode_district || '') !== String(input.postcodeDistrict).toUpperCase().trim().replace(/\s+/g, ' ')) return false;
  if (input.postcodeSector && (r.postcode_sector || '') !== String(input.postcodeSector).toUpperCase().trim().replace(/\s+/g, ' ')) return false;
  if (input.propertyType) {
    const want = String(input.propertyType).toUpperCase().trim();
    const codeHit = r.property_type === want;
    // Label match is exact (case-insensitive): the label set is closed
    // (Detached / Semi-detached / Terraced / Flats/Maisonettes / Other) and
    // substring matching false-fires — "DETACHED" is a substring of
    // "SEMI-DETACHED", and the code "D" is too.
    const labelHit = (r.property_type_label || '').toUpperCase() === want;
    if (!codeHit && !labelHit) return false;
  }
  if (input.tenure) {
    const want = String(input.tenure).toUpperCase().trim();
    const codeHit = r.duration === want;
    // Exact label match, same reason ("L" is a substring of "FREEHOLD").
    const labelHit = (r.duration_label || '').toUpperCase() === want;
    if (!codeHit && !labelHit) return false;
  }
  if (input.oldNew && r.old_new !== String(input.oldNew).toUpperCase().trim()) return false;
  if (input.ppdCategory && r.ppd_category !== String(input.ppdCategory).toUpperCase().trim()) return false;
  if (input.priceMin != null && (r.price === null || r.price < input.priceMin)) return false;
  if (input.priceMax != null && (r.price === null || r.price > input.priceMax)) return false;
  if (input.monthFrom && (r.month === null || r.month < input.monthFrom)) return false;
  if (input.monthTo && (r.month === null || r.month > input.monthTo)) return false;
  return true;
}

function filterTransactions(rows, input) {
  return rows.filter((r) => matchesFilters(r, input));
}

// Which partition years can contain rows matching the input? Derived from
// the month range (the primary partition key is the transfer year). With no
// month range, every year is touched — the caller documents that cost.
function yearsTouched(input, availableYears) {
  const from = input.monthFrom ? Number(String(input.monthFrom).slice(0, 4)) : null;
  const to = input.monthTo ? Number(String(input.monthTo).slice(0, 4)) : null;
  return availableYears.filter((y) => (from === null || y >= from) && (to === null || y <= to));
}

const round2 = (n) => Math.round(n * 100) / 100;

function median(nums) {
  if (!nums.length) return null;
  const s = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : round2((s[mid - 1] + s[mid]) / 2);
}

// Incremental analytics accumulator: rows are added one at a time (streamed),
// so an analytics pass never holds the archive itself in memory — only
// per-group price bags (needed for exact medians) and counters.
//
// MEMORY DESIGN (learned the hard way, 2026-11-21): a first version stored
// every price in a plain JS array for EACH of 12 groupings and died with a
// V8 heap OOM (~2 GB) about 3 minutes into a full-archive pass. Two defences
// are now built in:
//  1. Prices live in a PriceBag — a growable Uint32Array (4 bytes/price;
//     PPD prices are whole pounds, archive max £900M, far below 2^32).
//  2. The groupings are split into two FAMILIES and the Actor runs TWO
//     streaming passes, one per family, so peak price storage is the larger
//     family (~7 copies of the filtered set) instead of all 12 at once.
// Family 1: overall, by_year, by_county, by_district, by_property_type,
//   by_tenure, by_county_property_type, volume series.
// Family 2: by_town_city, by_old_new, by_ppd_category, by_postcode_area,
//   by_postcode_district.
// createAnalyticsAccumulator() with no argument accumulates BOTH families
// (tests and small sets); the Actor always passes an explicit family.
class PriceBag {
  constructor() { this.buf = new Uint32Array(256); this.length = 0; }
  push(v) {
    if (this.length === this.buf.length) {
      const nb = new Uint32Array(Math.ceil(this.buf.length * 1.5) + 256);
      nb.set(this.buf);
      this.buf = nb;
    }
    this.buf[this.length++] = v;
  }
  sortedCopy() {
    const a = this.buf.slice(0, this.length);
    a.sort(); // Uint32Array.sort() is numeric
    return a;
  }
}

function medianOfBag(bag) {
  if (!bag || bag.length === 0) return null;
  const s = bag.sortedCopy();
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : round2((s[mid - 1] + s[mid]) / 2);
}

const ANALYTICS_COVERAGE = 'All HM Land Registry Price Paid transactions for England & Wales in the shipped snapshot (year partitions per the manifest), with the monthly update file (additions / changes / deletions) applied at ingest. Prices are as published by HMLR. PPD Category B records (flagged ppd_category = "B") are included in the archive and in analytics over the filtered set; HMLR excludes Category B from its own headline price statistics — filter ppdCategory = "A" to reproduce that convention. Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.';

function createAnalyticsAccumulator(family = 'all') {
  const f1 = family === 'all' || family === 1;
  const f2 = family === 'all' || family === 2;
  const groups = {};
  if (f1) {
    groups.overall = newGroup();
    groups.by_year = new Map();
    groups.by_county = new Map();
    groups.by_district = new Map();
    groups.by_property_type = new Map();
    groups.by_tenure = new Map();
    groups.by_county_property_type = new Map(); // "COUNTY | D" -> group (+parts)
  }
  if (f2) {
    groups.by_town_city = new Map();
    groups.by_old_new = new Map();
    groups.by_ppd_category = new Map();
    groups.by_postcode_area = new Map();
    groups.by_postcode_district = new Map();
  }
  const volumeByYear = {};
  const volumeByMonth = {};
  let rowsAnalysed = 0;

  function newGroup() { return { transactions: 0, prices: new PriceBag(), sum: 0, newBuild: 0 }; }
  function bump(map, key, r) {
    let g = map.get(key);
    if (!g) { g = newGroup(); map.set(key, g); }
    addTo(g, r);
  }
  function addTo(g, r) {
    g.transactions += 1;
    if (r.price !== null) { g.prices.push(r.price); g.sum += r.price; }
    if (r.old_new === 'Y') g.newBuild += 1;
  }

  return {
    add(r) {
      rowsAnalysed += 1;
      if (f1) {
        addTo(groups.overall, r);
        if (r.year !== null) {
          bump(groups.by_year, r.year, r);
          volumeByYear[r.year] = (volumeByYear[r.year] || 0) + 1;
        }
        if (r.month) volumeByMonth[r.month] = (volumeByMonth[r.month] || 0) + 1;
        bump(groups.by_county, r.county, r);
        bump(groups.by_district, r.district, r);
        bump(groups.by_property_type, r.property_type || 'UNKNOWN', r);
        bump(groups.by_tenure, r.duration || 'UNKNOWN', r);
        const ct = groups.by_county_property_type;
        const key = `${r.county} | ${r.property_type || 'UNKNOWN'}`;
        let g = ct.get(key);
        if (!g) { g = { ...newGroup(), county: r.county, property_type: r.property_type || 'UNKNOWN' }; ct.set(key, g); }
        addTo(g, r);
      }
      if (f2) {
        bump(groups.by_town_city, r.town_city, r);
        bump(groups.by_old_new, r.old_new || 'UNKNOWN', r);
        bump(groups.by_ppd_category, r.ppd_category || 'UNKNOWN', r);
        if (r.postcode_area) bump(groups.by_postcode_area, r.postcode_area, r);
        if (r.postcode_district) bump(groups.by_postcode_district, r.postcode_district, r);
      }
    },
    finalize() {
      const sum = (g) => ({
        transactions: g.transactions,
        median_price: medianOfBag(g.prices),
        mean_price: g.transactions && g.prices.length ? round2(g.sum / g.prices.length) : null,
        new_build_share: g.transactions ? round2(g.newBuild / g.transactions) : null,
      });
      const mapOut = (map, keyName) => [...map.entries()]
        .map(([k, g]) => ({ [keyName]: k, ...sum(g) }))
        .sort((a, b) => b.transactions - a.transactions);
      const out = { rows_analysed: rowsAnalysed, coverage: ANALYTICS_COVERAGE };
      if (f1) {
        out.overall = sum(groups.overall);
        out.by_year = [...groups.by_year.entries()]
          .map(([y, g]) => ({ year: y, ...sum(g) }))
          .sort((a, b) => a.year - b.year);
        out.by_county = mapOut(groups.by_county, 'county');
        out.by_district = mapOut(groups.by_district, 'district');
        out.by_property_type = mapOut(groups.by_property_type, 'property_type');
        out.by_tenure = mapOut(groups.by_tenure, 'tenure');
        out.by_county_property_type = [...groups.by_county_property_type.values()]
          .map((g) => ({ county: g.county, property_type: g.property_type, ...sum(g) }))
          .sort((a, b) => (a.county + a.property_type).localeCompare(b.county + b.property_type));
        out.volume_by_year = volumeByYear;
        out.volume_by_month = volumeByMonth;
      }
      if (f2) {
        out.by_town_city = mapOut(groups.by_town_city, 'town_city');
        out.by_old_new = mapOut(groups.by_old_new, 'old_new');
        out.by_ppd_category = mapOut(groups.by_ppd_category, 'ppd_category');
        out.by_postcode_area = mapOut(groups.by_postcode_area, 'postcode_area');
        out.by_postcode_district = mapOut(groups.by_postcode_district, 'postcode_district');
      }
      return out;
    },
  };
}

// Merge the two family partials into the single ANALYTICS record. The key
// sets are disjoint except rows_analysed / coverage, which agree.
function mergeAnalytics(a, b) {
  if (!a) return b;
  if (!b) return a;
  return { ...a, ...b, rows_analysed: a.rows_analysed };
}

// B6-shaped convenience wrapper for tests and small sets.
function computeAnalytics(rows) {
  const acc = createAnalyticsAccumulator('all');
  for (const r of rows) acc.add(r);
  return acc.finalize();
}

module.exports = {
  parseCsvLine, normaliseRecord, matchesFilters, filterTransactions,
  yearsTouched, postcodeParts, median, createAnalyticsAccumulator, computeAnalytics,
  mergeAnalytics, PriceBag,
  PROPERTY_TYPE_LABELS, DURATION_LABELS, PPD_FIELD_COUNT,
};
