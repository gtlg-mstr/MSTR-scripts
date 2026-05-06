# Scripts — Project Notes

## Auth Method
All scripts **MUST** use `mstrio-py` (`mstrio.connection.Connection`) for authentication and session management. Raw `requests.post("/api/auth/login")` is forbidden. Use raw `requests` only for endpoints that mstrio does not cover (e.g. `/api/aiservice/*`, `/api/dataServer/*`, file uploads with multipart).

Why: mstrio handles token refresh, cookie jars, project ID injection, SSL, and SSL verification. Re-implementing this in raw requests is brittle and error-prone.

### Auth Compliance (2026-05-01 audit)
| Script | Auth | Status |
|--------|------|--------|
| 01_create_mosaic_model.py | mstrio-py | ✅ (deprecated, points to archive v2) |
| 02_add_mosaic_column.py | mstrio-py | ❌ BROKEN — uses classic Attribute.create(), incompatible with Mosaic |
| 03_create_mosaic_metric.py | mstrio-py | ✅ Fixed 2026-05-01 |
| 04_mstr_data_dictionary.py | mstrio-py | ✅ Fixed 2026-05-01 — type normalizer expanded |
| 05_check_mosaic.py | mstrio-py | ✅ |
| 06_diagnose_table.py | mstrio-py | ✅ |
| 07_edit_dossier_visualization.py | mstrio-py | ✅ |
| 08_delete_mosaic_attribute.py | mstrio-py | ✅ Fixed 2026-05-01 |
| 09_delete_mosaic_metric.py | mstrio-py | ✅ |
| add_mosaic_column_v2.py | mstrio-py (classic) | ❌ BROKEN — classic mstrio.modeling API, not Mosaic-compatible |
| add_mosaic_column_v2.1.py | mstrio-py (REST) | ✅ Working — uses Mosaic changeset + batch API |
| archive/create_mosaic_model_v2.py | mstrio-py | ✅ Working — multi-source, folder-targeting |

## Environment
- `.env` lives at `/home/support/dev-projects/Scripts/.env`
- Keys: `MSTR_BASE_URL`, `MSTR_USERNAME`, `MSTR_PASSWORD`, `MSTR_PROJECT_NAME`, `MSTR_LOGIN_MODE`
- All new scripts should load `.env` via a `_load_env()` helper pointing to the absolute path above.

## Target Project
- Default: `Shared Studio` on `https://studio.strategy.com/MicroStrategyLibrary`
- Default model ID: `AC49E200F8E041C1AF38D87B99EF19DC` (may not exist on server — create fresh per test run)
- Default folder ID: `0268E42CB84F8CCCE28909A111004E8F` (Mosaic models)

## Script Versions
- `archive/create_mosaic_model_v2.py` — latest working model creator (multi-source, folder targeting)
- `add_mosaic_column_v2.1.py` — production column adder via Mosaic changeset + batch API
- `03_create_mosaic_metric.py` — calculated metric creation (Sum, Calc (+/-), validated expression)
- `08_delete_mosaic_attribute.py` — attribute deletion via changeset
- `09_delete_mosaic_metric.py` — metric deletion via changeset
- `02_add_mosaic_column.py` / `add_mosaic_column_v2.py` — DEPRECATED/BROKEN (classic mstrio API, not Mosaic)

## Dual-Source Test Config
- `v2_dual_sources.json` — `glagrange - Postgresql` → `public.LRX_pg_orders` + `glagrange - MicroSoft SQL` → `dbo.LRX_mssql_rx`
- Full pipeline test order: create model → create metric → delete attribute → add attribute back

## Convention: mstrio + Raw Hybrid
Pattern used across all working scripts:
```python
from mstrio.connection import Connection
conn = Connection(base_url=..., username=..., password=..., project_name=..., login_mode=1)
# Then use conn.get/post/put/delete(endpoint="/api/model/...", headers={"X-MSTR-MS-Changeset": cs})
```

## Token Parser
The `_formula_to_tokens()` helper in model creator scripts converts AIService natural-language formulas into MSTR expression token trees. Supports: Sum, Count, Avg, Average, Median, Min, Max, RunningSum, RunningAvg, MovingAvg, Percentile, PercentRank, binary ops (+ - * /), and nested ratios.

## Batch API
Bulk attribute/metric creation goes through `/api/model/batch?allowPartialSuccess=true&showChanges=true`.

## Calculated Metrics Workflow
1. `create_metric_shell()` — POST empty metric shell
2. `validate_expression()` — POST tokens to `/metrics/{id}/expression/validate`
3. `update_metric()` — PUT name + validated expression
4. `commit_changeset()` — save

## AI Service Endpoints
mstrio does NOT cover these. Use raw requests with an independently authenticated `requests.Session`:
- `/api/aiservice/model/v2/columns/infer`
- `/api/aiservice/model/objects/metrics/recommendations`
- `/api/aiservice/model/objects/metrics`

## Changeset Lifecycle
1. Create changeset
2. Optional: rebase against model
3. Perform mutations (POST/PATCH/DELETE)
4. Commit changeset
5. On failure: abort changeset

## Column Skips in Metric Creation
Do not create Sum fact-metrics for columns whose lowercase name contains:
- `flag` (boolean-like)
- ` id` or ` id ` (identifier columns)

These stay as attributes only.

## Code Versioning Rule
**Never overwrite existing code files.** Always create a new versioned copy (e.g. `script_v2.py`, `script_v3.py`). Originals stay untouched for reference.

## Type Normalizer (04_mstr_data_dictionary.py)
`_normalize_type()` maps Mosaic's internal type names to canonical forms for cross-source comparison. Key aliases added 2026-05-01:
- `VARIABLE_LENGTH_STRING` → VARCHAR
- `DOUBLE PRECISION` → DOUBLE
- `TIME_STAMP` → TIMESTAMP
- `TIMESTAMP WITHOUT TIME ZONE` → TIMESTAMP
- `DATETIME` → TIMESTAMP (normalized)

Tested against dual-source model (LRX_pg_orders + LRX_mssql_rx): 24/24 MATCH.
