# UK Property Sale Prices 1995–2026 — 31.7M Transactions for Investors & Valuation Models

Every recorded residential sale in England & Wales since 1995 — 31,702,685 transactions with price, date, property type, tenure and address — from HM Land Registry's Price Paid Data. Built for investors screening UK markets, valuation and AVM teams training on complete (not sampled) price history, and researchers who need the full register rather than a portal scrape. Crown copyright data published under the Open Government Licence; snapshot-dated, not a live feed.

## What you get

- **Transaction rows** matching your filters (county, district, town/city, postcode area/district/sector, property type, tenure, new-build, PPD category, price range, month range), each with: transaction ID, price paid (GBP), transfer date, postcode and its derived area/district/sector, property type (code and label), new-build flag, tenure, PAON, SAON, street, locality, town/city, district, county, and PPD category.
- **Derived analytics** (key-value store record `ANALYTICS`): transaction count, median and mean price, and new-build share overall, per year, per county, per district, per town/city, per property type, per tenure, per county × property type, and per postcode area and district; volume by year and by month — all computed over the full filtered set, not a sample.

## Coverage — read this first

- **Transfer dates 1995-01-01 to 2026-09-30**, England & Wales only. Scotland (Registers of Scotland) and Northern Ireland maintain separate registers and are **not** included.
- **Deletions and amendments are applied.** HM Land Registry republishes the yearly files monthly and issues a monthly update file carrying additions, changes and deletions. This archive is built from the current yearly files with the current monthly update file applied on top (98,103 update records reconciled at ingest: 93,495 additions were already reflected in the current yearly files, 3,056 amendments were applied in place, and the 1,552 deletions named records already absent from the yearly files — no deleted record is present in this archive), so records HMLR has deleted do not appear and amended records carry their current values.
- **PPD categories.** Category A ("Standard") entries are ordinary residential sales. Category B ("Additional") entries cover other transfers lodged for registration (for example transfers involving companies, repossessions and other non-standard transfers). Both are included and every row is flagged (`ppd_category`); note that **HM Land Registry excludes Category B from its own headline price statistics** — set the `ppdCategory` filter to `A` to reproduce that convention.
- **No floor area exists in this dataset**, so there is no price-per-sqm: area-based analytics are replaced by medians by postcode district/sector, district and county.
- **Addresses are published exactly as published by HM Land Registry** — no enhancement, geocoding or derivation is applied to them. Postcode area/district/sector are mechanical splits of the published postcode.
- The archive is refreshed monthly, on HM Land Registry's publication cycle (the 20th working day of each month). The most recent month is always incomplete — sales are lodged over time — and HMLR itself cautions against reading the latest month in isolation.
- Prices are as recorded at registration; sales not lodged with HM Land Registry, and sales not for value, are excluded by the source itself.

## Data source, licence and attribution

Source dataset: **Price Paid Data**, HM Land Registry, published via gov.uk (yearly files and monthly update file), accessed 21 November 2026.

Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0 (http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

The address data in Price Paid Data was processed against Ordnance Survey's AddressBase Premium product, which incorporates Royal Mail's PAF® database. It is republished here exactly as published by HM Land Registry, for the purpose of providing residential property price information services.

**Independent tool; not affiliated with HM Land Registry, and not endorsed by it.**

Personal data: none. HM Land Registry states that "Price paid information is not personal, but property-related information." The dataset contains no buyer or seller names, no personal identifiers and no contact details in any field; postcode and address fields are the transacted property's address, as published.

## Architecture and run behaviour

The archive is far too large to load as a single file (31.7M rows), so it is partitioned **by transfer year**: a run reads only the year partitions its month range touches and streams them row by row, with bounded memory. Analytics are computed in **two streaming passes** over the touched partitions (the grouping set is split in two), so exact medians never require holding a price array for every grouping at once. Practical consequences:

- A filtered run (a county, a decade, a postcode district) touches few partitions and is fast.
- A run with **no month range reads every partition** — the whole archive — and takes proportionally longer. That is the honest cost of a complete-archive product; use `monthFrom`/`monthTo` and `limit` to scope runs.
- Results are pushed to the dataset in batches of 1,000 rows (large single pushes are truncated by the platform — batching is deliberate).
- **Recommended memory: 4 GB.** Measured locally, a full-archive analytics pass (all 31.7M rows, both passes) peaked at ~1.7 GB RSS and took ~7 minutes on a small shared VM; a single-year analytics pass takes seconds. 4 GB gives comfortable headroom above that peak (1 GB is too little; 2 GB is marginal — the same sizing this product family's Singapore archive required on-platform).

## Pricing

**US$20 per 1,000 results** (US$0.020 per transaction row returned to the dataset), with a free allowance of 250 results. Analytics are delivered to the key-value store and are not metered per row.

## Input

| Field | Meaning |
|---|---|
| `mode` | `rows`, `analytics`, or `both` (default) |
| `county` | case-insensitive substring match on county |
| `district` | case-insensitive substring match on local authority district |
| `townCity` | case-insensitive substring match on town/city |
| `postcodeArea` | exact postcode area match, e.g. `NR` |
| `postcodeDistrict` | exact postcode district (outward code) match, e.g. `NR9` |
| `postcodeSector` | exact postcode sector match, e.g. `NR9 5` |
| `propertyType` | code `D`/`S`/`T`/`F`/`O` or exact label (`Detached`, `Semi-detached`, `Terraced`, `Flats/Maisonettes`, `Other`) |
| `tenure` | code `F` (Freehold) / `L` (Leasehold) or exact label — the source field is named "duration" |
| `oldNew` | `Y` = new build, `N` = established dwelling |
| `ppdCategory` | `A` = Standard, `B` = Additional |
| `priceMin` / `priceMax` | price bounds in GBP |
| `monthFrom` / `monthTo` | transfer month range as `YYYY-MM`; also selects which year partitions are read |
| `limit` | max rows returned (default: all matching) |

## Terms

Outputs are derived from the source dataset named above and are provided as-is for business intelligence. Prices, dates and coverage are exactly as published by the source, subject to the statements in the coverage section above; check it before relying on any aggregate.
