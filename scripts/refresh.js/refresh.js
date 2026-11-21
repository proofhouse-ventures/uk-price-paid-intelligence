'use strict';
// Refresh: re-download the Price Paid yearly files and the monthly update
// file from HM Land Registry's public download host (no sign-in / no API
// key), re-ingest, report the new totals. HMLR updates the data on the
// 20th working day of each month; the yearly files are republished with
// amendments and deletions applied, and the monthly update file carries
// the same cycle's additions/changes/deletions (applied idempotently by
// ingest.js). Run manually or from a scheduled job.
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const BASE = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com';
const FILES = [];
for (let y = 1995; y <= 2026; y++) FILES.push(`pp-${y}.csv`);
FILES.push('pp-monthly-update-new-version.csv');

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
      ws.on('finish', () => ws.close(() => { fs.renameSync(tmp, dest); resolve(); }));
      ws.on('error', reject);
    }).on('error', reject);
  });
}

(async () => {
  const rawDir = path.join(__dirname, '..', '..', 'data', 'raw', 'ukpp');
  fs.mkdirSync(rawDir, { recursive: true });
  for (const file of FILES) {
    await download(`${BASE}/${file}`, path.join(rawDir, file));
    console.log(`downloaded ${file}`);
  }
  execFileSync('node', [path.join(__dirname, 'ingest.js'), rawDir], { stdio: 'inherit' });
})().catch((e) => { console.error(e); process.exit(1); });
