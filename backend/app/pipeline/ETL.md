## ETL Pipeline: Intentional Weirdness Guide

This document explains the deliberate oddities baked into the ETL pipeline.

> **All of these are intentional. Do not “fix” them unless you fully understand the downstream impact.**

The current behavior is tuned to messy, real-world Book Latte data and legacy expectations.

---

## 1. Time Dimension

- `create_time_dimension(_date_series)` intentionally ignores its input.
- Time dimension is generated only for hours `H01`–`H23` (no `H00` rows).
- `time_to_id()` can emit `H00Mxx`, which will not match the time dimension.

**Why this is intentional:** The pipeline is calibrated to how source times appear in practice; adding `H00` or wiring `_date_series` would change joins and historical outputs.

---

## 2. Costing Logic

- Uses only the first costing workbook in `raw_costing/*.xlsx`.
- Matching is heuristic (sheet name vs normalized product name, HOT/ICED, 8/12/16 oz).
- Reads a specific row (row 35) and picks the first numeric cell as the cost.
- Wrapped in `@lru_cache`, so results are stable for a run even if the file is large/weird.

**Why this is intentional:** Mirrors a very specific costing file layout and avoids over‑engineering. Changing it risks breaking existing costing expectations.

---

## 3. Cost Fallback

Example logic:

```python
if cost:
    return cost
# Otherwise, fallback used elsewhere:
# cost = price * 0.60
```

**Why this is intentional:** A zero cost is treated as “no costing found,” and the 60% rule is the accepted default.

---

## 4. Product Dimension / SCD Behavior

- `current_product_dim` uses `groupby('product_id').last()` (order‑dependent).
- `current_product_dim` sets all `record_version = 1` and `is_current = True`.
- `history_product_dim`:
  - Builds versions via `cumcount()` after `drop_duplicates`.
  - Marks one row per product as `is_current = True` inside history as well.

**Why this is intentional:** Pragmatic SCD Type 4‑ish hybrid.  
`current_product_dimension` is the canonical current view.  
`history_product_dimension` is for lineage/debugging.  
The double `is_current` semantics preserve backward compatibility with prior notebooks and reports.

---

## 5. Standardization Rules and “Funny” Spellings

- Hard‑coded normalization rules with apparent typos (e.g., `THE CKUB`, `GALIC BREAD ALA CARTE`, `2024BREads9`).
- Rules trigger remaps based on historical dirty data.

**Why this is intentional:** These strings match legacy exports. “Correcting” them would silently break joins and historical bundle mappings.

---

## 6. Category Classification

- Uses a mix of:
  - Keyword sets (e.g., drink tokens),
  - Regex triggers,
  - Product ID hints (`DRNKS`, `DKS`),
  - Special cases for `OTHERS` / `EXTRA`.

**Why this is intentional:** Heuristic, but tuned. Adjusting could shift many products across categories and distort analytics aligned to this behavior.

---

## 7. Cleaning Logic Coupled to Columns

- Numeric filters (Price, Qty, etc.) are applied only when `Time` exists.
- `Take Out` column mapping:
  - `Y` → `"True"`
  - empty/NaN → `"False"`
  - other values (e.g., `N`) pass through

**Why this is intentional:** Reflects how specific exports behave. The coupling prevents over‑aggressive cleaning on partial/older files.

---

## 8. Product ID Normalization: “First Seen Wins”

- Standardizes Product ID by normalized Product Name.
- First encountered mapping is treated as authoritative.

**Why this is intentional:** Designed to collapse noisy duplicates in legacy data. Consumers rely on the resulting stable IDs.

---

## 9. Join Strategy and Latest Month Exclusion

- Uses an inner join on `Receipt No` for merging header and line data.
- Excludes the latest month from `fact_transaction_dimension` while still using full data for dimensions.

**Why this is intentional:** Inner join ensures only fully linked records are analyzed. Latest month exclusion supports a “stabilized history” pattern for analytics.

---

## 10. Transaction Records and `parent_sku`

- `transaction_records.csv` groups by `Receipt No` and joins `parent_sku`s as comma‑separated values.
- Uses `parent_sku` from the current product mapping; falls back to `Product ID`.

**Why this is intentional:** Optimized for downstream market basket analysis tooling that expects this exact SKU list behavior.

---

## 11. Style and Structure

- Functions print directly to stdout.
- `_date_series` param in `create_time_dimension` is unused.
- Many comments reference “matches original behavior.”

**Why this is intentional:** The script doubles as an operational ETL and a traceable audit log for non‑engineering stakeholders. Refactors are intentionally avoided to keep behavior stable.

---

## Summary

- This ETL is deliberately opinionated and data‑specific.
- It encodes business rules, historical quirks, and legacy compatibility.
- It may look strange as a generic framework—but it is working as designed here.

If you change any of these “weird” parts, assume you might:

- Break historical comparability,
- Break existing dashboards/notebooks,
- Invalidate prior analyses.

No need to fix.