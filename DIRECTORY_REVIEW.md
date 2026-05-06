Scripts Directory Review
====================================
Path: /home/support/dev-projects/Scripts/
Date: 2026-05-01 (updated after auth fix + pipeline test)
Original: 2026-04-27

SUMMARY
-------
A collection of MicroStrategy (Strategy Software) Mosaic model automation scripts.
All scripts target the "Shared Studio" project on https://studio.strategy.com/MicroStrategyLibrary.
They use mstrio-py (Python SDK) for auth and raw REST calls where mstrio lacks coverage (e.g. /api/aiservice, /api/model/batch, /api/dataServer).

2026-05-01 UPDATE
------------------
- Fixed 03_create_mosaic_metric.py: raw requests.post("/api/auth/login") → mstrio.connection.Connection
- Fixed 08_delete_mosaic_attribute.py: raw requests.post("/api/auth/login") → mstrio.connection.Connection
- Created add_mosaic_column_v2.1.py: production Mosaic column adder using changeset + batch API (from archive reference)
- add_mosaic_column_v2.py confirmed BROKEN (classic mstrio.modeling API, not Mosaic-compatible)
- Full 4-step pipeline tested end-to-end against Shared Studio: create model → create metric → delete attribute → add attribute
- Default model AC49E200F8E041C1AF38D87B99EF19DC does NOT exist on server — must create fresh per test
- Dual-source test config: v2_dual_sources.json (Postgres LRX_pg_orders + MSSQL LRX_mssql_rx)
- Auth compliance audit completed — see CLAUDE.md for full table

CONFIGURATION
-------------
.env                Credentials (URL, username, password, project, loginMode)
config.yaml         App settings (model_filter list, output_csv path)
requirements.txt    python-dotenv>=1.0.0

PYTHON SCRIPTS — Working Production
------------------------------------

1. archive/create_mosaic_model_v2.py  (1314 lines)
   PURPOSE: Create multi-source Mosaic model from database tables across different datasources.
   STATUS: ✅ Working. Tested 2026-05-01.
   AUTH: mstrio-py via MSTRSession wrapper.
   FEATURES: Single or multi-source (JSON config), folder targeting, pipeline-based import, auto attribute/fact metric creation, AI calculated metric suggestions, column mappings.
   CLI: --datasource/--table (single) or --source-config <json> (multi), --model-name, --folder-id.

2. add_mosaic_column_v2.1.py  (366 lines)
   PURPOSE: Add a warehouse column as an Attribute to an existing Mosaic model.
   STATUS: ✅ Working. Tested 2026-05-01.
   AUTH: mstrio-py.
   METHOD: AI type inference → changeset → inject column into pipeline JSON → PATCH table via batch API → extract server-assigned column ID → POST attribute → commit → publish.
   CLI: --model-id, --table-name, --column-name, --attr-name, --description.

3. 03_create_mosaic_metric.py  (362 lines)
   PURPOSE: Create a calculated metric inside an existing model using changesets.
   STATUS: ✅ Working. Fixed 2026-05-01 (auth).
   MODES: sum (wrap in Sum()), calc (Metric1 + Metric2 or Metric1 - Metric2).
   FLOW: login → changeset → find metrics → create shell → validate expression → update → commit → publish.

4. 08_delete_mosaic_attribute.py  (186 lines)
   PURPOSE: Remove an attribute by exact name from a Mosaic model.
   STATUS: ✅ Working. Fixed 2026-05-01 (auth).
   FLOW: changeset → resolve attribute → DELETE → commit → verify.
   NOTE: Underlying warehouse pipeline columns are NOT removed; cleaned up on next Studio refresh.

5. 09_delete_mosaic_metric.py  (183 lines)
   PURPOSE: Remove a Metric or FactMetric from a Mosaic model.
   STATUS: ✅ Working. Original already mstrio-py compliant.

6. 04_mstr_data_dictionary.py  (492 lines)
   PURPOSE: Extract Mosaic model metadata, compare to physical DB schema, output CSV.
   STATUS: ✅ Working. mstrio-py.

7. 07_edit_dossier_visualization.py  (253 lines)
   PURPOSE: Edit a Dossier visualization by replacing a metric in its JSON definition.
   STATUS: ✅ Working. mstrio-py.

8-10. 05_check_mosaic.py, 06_diagnose_table.py — diagnostic/read-only. ✅ Working.

BROKEN / DEPRECATED
-------------------
- 01_create_mosaic_model.py: DEPRECATED. Points to archive/create_mosaic_model_v2.py.
- 02_add_mosaic_column.py / add_mosaic_column_v2.py: BROKEN. Uses classic mstrio.modeling.Attribute.create() which is incompatible with Mosaic models. Causes ValueError: 'script' is not a valid DatasourceType. Use add_mosaic_column_v2.1.py instead.

SUPPORTING FILES
----------------
data_dictionary.csv        Output from mstr_data_dictionary.py
mosaic_workflow_diagrams.md  Mermaid workflow diagrams
sources_test_dual.json     Dual-source config (PG + MSSQL)
v2_dual_sources.json       Same as above
config.yaml                App settings for data dictionary

OBSERVATIONS & NOTES
--------------------
- All production scripts now mstrio-py compliant. No raw /api/auth/login calls remain.
- MSTRSession in archive/create_mosaic_model_v2.py is the canonical hybrid pattern: mstrio for auth, raw REST for uncovered endpoints.
- Code versioning rule: never overwrite. Always create new versioned copies.
- Full test pipeline: create model → create metric → delete attribute → add attribute. All green.
- Default model ID in scripts is outdated; create fresh models per test run.
- AI metrics via AIService return suggestions but no workable formulas — all 12 skipped in test.
