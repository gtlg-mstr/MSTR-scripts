# MSTR-scripts

MicroStrategy / Strategy One automation scripts for Mosaic models, reporting, and administration.

## Mosaic Model Operations

| Script | Description |
|--------|-------------|
| `01_create_mosaic_model.py` | Create a Mosaic data model from multiple database tables across different datasources. Supports single and multi-source models. |
| `02_add_mosaic_column.py` | Add a warehouse column as an Attribute or Fact to an existing Mosaic Model. Uses mstrio-py for auth and raw REST for model mutations. |
| `03_create_mosaic_metric.py` | Create calculated metrics in a Mosaic model (Sum, Count, Avg, etc.) with expression validation and batch API support. |

## Data & Diagnostics

| Script | Description |
|--------|-------------|
| `04_mstr_data_dictionary.py` | Generate a data dictionary by extracting Mosaic model metadata and comparing it against actual data source tables. Outputs CSV. |
| `05_check_mosaic.py` | Retrieve and inspect a Mosaic model's object definition. |
| `06_diagnose_table.py` | Diagnose a specific table in a Mosaic model — find table ID and get detailed structure info. |

## Editing & Visualization

| Script | Description |
|--------|-------------|
| `07_edit_dossier_visualization.py` | Edit a visualization within a Dossier (Dashboard) by replacing a metric. Directly manipulates the Dossier JSON definition via REST. |

## Deletion

| Script | Description |
|--------|-------------|
| `08_delete_mosaic_attribute.py` | Remove an Attribute from a Mosaic Model/Changeset. Resolves by name, DELETEs via changeset, and commits. |
| `09_delete_mosaic_metric.py` | Remove a Metric or FactMetric from a Mosaic Model. Resolves against both `/factMetrics` and `/metrics` endpoints. |

## Legacy Scripts

| Script | Description |
|--------|-------------|
| `A01 - Copy visualization data to table.py` | Retrieve visualization data (grid or otherwise) and store it in a PostgreSQL table. |
| `C01 - Combine 2 CSV documents.py` | Combine two CSV files into one. |
| `D01_Distribution_in_body and attachment.py` | Distribute reports via email — body and attachment. |
| `E02-Extract SQL from Cubes and Reports-UpdatedAllProjects.py` | Extract SQL from cubes and reports across all projects. |
| `E03 - Run prompted report and get SQL.py` | Run prompted reports and review the generated data and SQL. |
| `F01 - Create project and basic objects.py` | Create a MicroStrategy project and basic objects. |
| `G01-Export_to_PDF.py` | Export a dossier to PDF. |
| `G02-Export_to_PDF-Filters.py` | Export a dossier to PDF with specific filters applied. |
| `G03-Export_to_PDF-SpecificPage.py` | Export a specific page of a dossier to PDF. Tested on Strategy One (Sept 2025). |
| `S01 - Seek and Disable_2.1.py` | Seek and disable specific objects. |
| `V01_republish list of cubes.py` | Republish a list of Intelligence Server cubes. |
| `X01_Create_simplified_cloud_security_roles (1).py` | Create simplified cloud security roles. |

## Other

| Path | Description |
|------|-------------|
| `api/` | API integration examples (RapidAPI Rugby Live) |
| `Archive/` | Older scripts moved here for reference (AuditUsers, MSTR_usermgmt) |
| `Audit/` | User audit scripts and products.xlsx for license reporting |
| `snippets/` | Utility snippets (mstrio MDI cube create/update, PostgreSQL connection, try-except patterns) |
| `sql/` | SQL utilities (backup_demo_tables) |
| `MuploadJSON/` | MSTR CDFJ upload scripts |

---

### Requirements

Install dependencies with:

```
pip install -r requirements.txt
```

### Authentication

All new scripts (01–09) use **mstrio-py** for connection management. Credentials are loaded from `.env`:

```
MSTR_BASE_URL=https://studio.strategy.com/MicroStrategyLibrary
MSTR_USERNAME=your_user
MSTR_PASSWORD=your_password
MSTR_PROJECT_NAME=Shared Studio
MSTR_LOGIN_MODE=1
```

### Changelog

- **20260428** — Renamed latest scripts with numbered prefixes (01–09) by function group
- **20241022** — Moved older scripts to Archives