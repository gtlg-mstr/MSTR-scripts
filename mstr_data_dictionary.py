"""
MicroStrategy Data Dictionary Generator
=======================================
Connects to MicroStrategy, extracts Mosaic model metadata, compares it
against actual data source tables, and outputs a side-by-side CSV dictionary.

Settings are loaded from:
  - .env          (credentials: URL, username, password, project)
  - config.yaml   (application settings: models, output path)

Usage:
    python mstr_data_dictionary.py
"""
# Tested on Strategy One April 2026 release.


import csv
import os
from pathlib import Path
from typing import Any

from mstrio import connection
from mstrio.modeling import (
    list_namespaces,
    list_datasource_warehouse_tables,
    list_physical_tables,
    list_logical_tables,
)
from mstrio.datasources import list_connected_datasource_instances

# Load .env if python-dotenv is installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _load_yaml_config() -> dict[str, Any]:
    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        import yaml
        return yaml.safe_load(config_path.read_text())
    return {}


def _get_model_to_ds_map(conn: connection.Connection, model_names: list[str]) -> dict[str, str]:
    """Map model names from config to their MicroStrategy datasource IDs."""
    print("Mapping models to datasource IDs...")
    ds_list = list_connected_datasource_instances(conn, to_dictionary=True)

    print(f"  DEBUG: Available Datasources: {[ds.get('name') for ds in ds_list]}")

    model_upper = [m.upper() for m in model_names]

    mapping = {}
    for ds in ds_list:
        name = ds.get("name", "").upper()
        if name in model_upper:
            mapping[name] = ds["id"]
            print(f"  Found: {ds.get('name')} -> {ds['id']}")

    return mapping


# ---------------------------------------------------------------------------
# Load settings
# ---------------------------------------------------------------------------

# --- MicroStrategy credentials (from .env) ---
MSTR_BASE_URL = _env("MSTR_BASE_URL", "https://your-env.microstrategy.com/MicroStrategyLibrary")
MSTR_USERNAME = _env("MSTR_USERNAME", "your_username")
MSTR_PASSWORD = _env("MSTR_PASSWORD", "your_password")
MSTR_PROJECT_NAME = _env("MSTR_PROJECT_NAME", "your_project_name")
MSTR_LOGIN_MODE = int(_env("MSTR_LOGIN_MODE", "1"))

# --- Application settings (from config.yaml) ---
_config = _load_yaml_config()
_mstr_section = _config.get("mstr", {})
MOSAIC_MODELS = _config.get("mosaic_models", _mstr_section.get("mosaic_models", []))
OUTPUT_CSV = _config.get("output_csv", _mstr_section.get("output_csv", "data_dictionary.csv"))

# Env var override: comma-separated list, e.g. "ITT GBQ,aehrlich_pg_db"
_env_models = [m.strip() for m in _env("MOSAIC_MODELS", "").split(",") if m.strip()]
if _env_models:
    MOSAIC_MODELS = _env_models


# ---------------------------------------------------------------------------
# CONNECTION
# ---------------------------------------------------------------------------

def connect() -> connection.Connection:
    """Establish a MicroStrategy connection."""
    print(f"Connecting to {MSTR_BASE_URL} as {MSTR_USERNAME}...")
    conn = connection.Connection(
        base_url=MSTR_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_name=MSTR_PROJECT_NAME,
        login_mode=MSTR_LOGIN_MODE,
    )
    print(f"Connected. I-Server version: {conn.iserver_version}")
    return conn


# ---------------------------------------------------------------------------
# FETCHERS
# ---------------------------------------------------------------------------

def _normalize_type(dtype: str) -> str:
    """Normalize a data type string to a short canonical form for comparison.
    Handles dict-style representations from MicroStrategy and plain strings
    from BigQuery INFORMATION_SCHEMA."""
    import re
    dtype = dtype.upper().strip()
    # If it's a dict-like string (e.g. "{'type': 'int64', ...}"), extract the 'type' field
    m = re.search(r"'type'\s*:\s*'([^']*)'", dtype, re.IGNORECASE)
    if m:
        dtype = m.group(1).upper()
    else:
        dtype = dtype.upper()

    # Normalize semantically equivalent types between Mosaic and BigQuery
    TYPE_ALIASES = {
        # Mosaic string types → canonical VARCHAR
        'FIXED_LENGTH_STRING': 'VARCHAR',
        'LONG_VARCHAR':        'VARCHAR',
        'TEXT':                'VARCHAR',
        'WIDE_CHAR':           'VARCHAR',
        'CHAR':                'CHAR',
        'VARCHAR':             'VARCHAR',
        # BigQuery string type → canonical VARCHAR
        'STRING':              'VARCHAR',
        # Numeric types
        'NUMERIC':             'DECIMAL',
        'DECIMAL':             'DECIMAL',
        'FLOAT':               'FLOAT',
        'DOUBLE':              'DOUBLE',
        'REAL':                'FLOAT',
        # Integer types (all map to BIGINT for cross-DB consistency)
        'INT':                 'BIGINT',
        'INTEGER':             'BIGINT',
        'INT64':               'BIGINT',
        'MEDIUMINT':           'BIGINT',
        'SMALLINT':            'SMALLINT',
        'TINYINT':             'TINYINT',
        'BIGINT':              'BIGINT',
        # Boolean types
        'BOOL':                'BOOLEAN',
        'BOOLEAN':             'BOOLEAN',
        'BIT':                 'BOOLEAN',
        # Date/time types
        'DATE':                'DATE',
        'DATETIME':            'DATETIME',
        'TIMESTAMP':           'TIMESTAMP',
        'TIME':                'TIME',
        # BigQuery array types - normalize to ARRAY
        re.compile(r'^ARRAY<.*>$'): 'ARRAY',
    }
    for key, val in TYPE_ALIASES.items():
        if isinstance(key, re.Pattern) and key.match(dtype):
            return val
    return TYPE_ALIASES.get(dtype, dtype)


def _filter_tables(tables: list[dict], model_filter: list[str]) -> list[dict]:
    if model_filter:
        return [
            t for t in tables
            if t.get("name", "").upper() in [m.upper() for m in model_filter]
        ]
    return tables


def fetch_warehouse_tables(
    conn: connection.Connection,
    ds_map: dict[str, str],
) -> list[dict[str, Any]]:
    print("\nFetching warehouse tables from MicroStrategy...")
    all_rows = []

    for model_name, ds_id in ds_map.items():
        try:
            namespaces = list_namespaces(conn, id=ds_id)
        except Exception as e:
            print(f"  Warning: could not list namespaces for '{model_name}': {e}")
            continue

        for ns in namespaces:
            try:
                tables = list_datasource_warehouse_tables(
                    conn,
                    datasource_id=ds_id,
                    namespace_id=ns["id"],
                )
            except Exception as e:
                print(f"  Warning: could not list tables in namespace '{ns['name']}' ({model_name}): {e}")
                continue

            for tbl in tables:
                col_rows = [
                    {
                        "mosaic_column_name": c.get("name", ""),
                        "mosaic_column_id":   c.get("id", "") or "",
                        "mosaic_data_type":   str(c.get("data_type", "")),
                        "mosaic_description": "",
                    }
                    for c in tbl.list_columns(to_dictionary=True)
                ]
                all_rows.append({
                    "mosaic_table_name":  tbl.name,
                    "datasource_id":     ds_id,
                    "datasource_name":   model_name.capitalize(), # Simplified representation
                    "namespace":         ns["name"],
                    "columns":           col_rows,
                })

    table_count = len(all_rows)
    print(f"  Found {table_count} warehouse table(s).")
    return all_rows


def fetch_physical_tables(
    conn: connection.Connection,
    model_filter: list[str],
) -> list[dict[str, Any]]:
    print("\nFetching physical tables from MicroStrategy...")
    tables = list_physical_tables(conn, to_dictionary=True, include_unassigned_tables=True)
    tables = _filter_tables(tables, model_filter)
    print(f"  Found {len(tables)} physical table(s).")

    rows = []
    for tbl in tables:
        col_rows = [
            {
                "phys_column_name":  c.get("name", ""),
                "phys_column_id":    c.get("id", ""),
                "phys_data_type":    c.get("data_type") or c.get("sub_type", ""),
                "phys_ext_type":     c.get("ext_type", ""),
                "phys_description":  c.get("description", ""),
            }
            for col in (tbl.get("columns") or [])
            for c in ([col] if isinstance(col, dict) else [])
        ]
        rows.append({
            "phys_table_name": tbl.get("name", ""),
            "phys_ext_type":  tbl.get("ext_type", ""),
            "phys_id":        tbl.get("id", ""),
            "columns":        col_rows,
        })
    return rows


def fetch_logical_tables(
    conn: connection.Connection,
    model_filter: list[str],
) -> list[dict[str, Any]]:
    print("\nFetching logical tables...")
    tables = list_logical_tables(conn, to_dictionary=True)
    tables = _filter_tables(tables, model_filter)
    print(f"  Found {len(tables)} logical table(s).")
    return tables


def fetch_physical_schema(
    conn: connection.Connection,
    mosaic_data: list[dict[str, Any]],
    ds_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Query the actual data source (via DatasourceInstance.execute_query)
    to get physical column metadata from INFORMATION_SCHEMA."""
    print("\nQuerying physical schema from data source(s)...")

    from mstrio.datasources import DatasourceInstance

    all_rows = []
    for tbl in mosaic_data:
        ds_name = tbl.get("datasource_name", "").upper()
        ns = tbl.get("namespace", "")
        table_name = tbl.get("mosaic_table_name", "")

        ds_id = ds_map.get(ds_name)
        if not ds_id:
            continue

        proj_id = conn.project_id

        # Resolve DB type to determine INFORMATION_SCHEMA path
        try:
            ds_list = list_connected_datasource_instances(conn, to_dictionary=True)
            ds_meta = next((d for d in ds_list if d["id"] == ds_id), {})
            db_type = ds_meta.get("database_type", "").lower()
        except Exception:
            db_type = "unknown"

        # BigQuery uses dataset.INFORMATION_SCHEMA.COLUMNS syntax
        if "bigquery" in db_type:
            fq_table = f"`{ns}`.INFORMATION_SCHEMA.COLUMNS"
        else:
            fq_table = "INFORMATION_SCHEMA.COLUMNS"

        try:
            ds_obj = DatasourceInstance(connection=conn, id=ds_id)
            result = ds_obj.execute_query(
                project_id=proj_id,
                query=f"""
                    SELECT column_name, data_type, is_nullable
                    FROM {fq_table}
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """,
            )
        except Exception as e:
            print(f"  Warning: query failed for '{ds_name}.{ns}.{table_name}': {e}")
            continue

        results = result.get("results", {})
        data = results.get("data", {})
        if not data:
            continue

        col_names = data.get("column_name", [])
        col_types = data.get("data_type", [])

        col_rows = [
            {
                "phys_column_name": col_names[i] if i < len(col_names) else "",
                "phys_column_id":   "",
                "phys_data_type":   col_types[i] if i < len(col_types) else "",
                "phys_ext_type":    "",
                "phys_description": "",
            }
            for i in range(len(col_names))
        ]

        all_rows.append({
            "phys_table_name": table_name,
            "phys_ext_type":   "",
            "phys_id":         "",
            "columns":         col_rows,
            "datasource_name": ds_name,
            "namespace":       ns,
        })

    total_cols = sum(len(r["columns"]) for r in all_rows)
    print(f"  Fetched physical schema for {len(all_rows)} table(s) ({total_cols} columns).")
    return all_rows


# ---------------------------------------------------------------------------
# BUILD DICTIONARY
# ---------------------------------------------------------------------------

def build_dictionary(
    mosaic_data: list[dict[str, Any]],
    phys_data: list[dict[str, Any]],
    log_data: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    phys_by_name = {t["phys_table_name"].upper(): t for t in phys_data}
    log_by_name = {t.get("name", "").upper(): t for t in log_data}
    dictionary: list[dict[str, Any]] = []

    for mosaic_tbl in mosaic_data:
        mosaic_name = mosaic_tbl["mosaic_table_name"].upper()
        phys_tbl = phys_by_name.get(mosaic_name)

        mosaic_cols = {c["mosaic_column_name"].upper(): c for c in mosaic_tbl["columns"]}
        phys_cols = {
            c["phys_column_name"].upper(): c
            for c in (phys_tbl["columns"] if phys_tbl else [])
        }

        for col_name in sorted(set(mosaic_cols) | set(phys_cols)):
            mosaic_col = mosaic_cols.get(col_name, {})
            phys_col = phys_cols.get(col_name, {})
            mosaic_dtype = _normalize_type(mosaic_col.get("mosaic_data_type", ""))
            phys_dtype = _normalize_type(phys_col.get("phys_data_type", "") or phys_col.get("phys_ext_type", ""))

            if not mosaic_col and phys_col:
                status = "IN_PHYSICAL_ONLY"
            elif mosaic_col and not phys_col:
                status = "IN_MOSAIC_ONLY"
            elif mosaic_dtype.upper() != phys_dtype.upper():
                status = "TYPE_MISMATCH"
            else:
                status = "MATCH"

            dictionary.append({
                "mosaic_table":           mosaic_tbl["mosaic_table_name"],
                "datasource_name":        mosaic_tbl["datasource_name"],
                "namespace":             mosaic_tbl["namespace"],
                "logical_table":          log_by_name.get(mosaic_name, {}).get("name", ""),
                "physical_table":        phys_tbl["phys_table_name"] if phys_tbl else "",
                "phys_ext_type":          phys_tbl["phys_ext_type"] if phys_tbl else "",
                "column_name":           (mosaic_col.get("mosaic_column_name") or phys_col.get("phys_column_name", "")),
                "mosaic_column_id":       mosaic_col.get("mosaic_column_id", ""),
                "phys_column_id":        phys_col.get("phys_column_id", ""),
                "mosaic_data_type":      mosaic_dtype,
                "physical_data_type":    phys_dtype,
                "mosaic_description":    mosaic_col.get("mosaic_description", ""),
                "physical_description":  phys_col.get("phys_description", ""),
                "match_status":          status,
            })
    return dictionary


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict[str, Any]], path: str) -> None:
    if not rows:
        print("No rows to write.")
        return

    fieldnames = [
        "mosaic_table", "datasource_name", "namespace",
        "logical_table", "physical_table", "phys_ext_type",
        "column_name", "mosaic_column_id", "phys_column_id",
        "mosaic_data_type", "physical_data_type",
        "mosaic_description", "physical_description", "match_status",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nData dictionary written to: {path}")
    print(f"  Total rows: {len(rows)}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("MicroStrategy Data Dictionary Generator")
    print("=" * 60)
    print(f"  Base URL : {MSTR_BASE_URL}")
    print(f"  Username : {MSTR_USERNAME}")
    print(f"  Project  : {MSTR_PROJECT_NAME}")
    print(f"  Models    : {MOSAIC_MODELS or '(all)'}")
    print(f"  Output   : {OUTPUT_CSV}")
    print()

    conn = connect()

    # Map requested models to DS IDs
    ds_map = _get_model_to_ds_map(conn, MOSAIC_MODELS)

    if not ds_map and MOSAIC_MODELS:
        print(f"Error: None of the requested models {MOSAIC_MODELS} were found in the project.")
        conn.close()
        return

    mosaic_data = fetch_warehouse_tables(conn, ds_map)
    phys_data   = fetch_physical_schema(conn, mosaic_data, ds_map)
    log_data    = fetch_logical_tables(conn, MOSAIC_MODELS)

    print("\nBuilding data dictionary...")
    dictionary = build_dictionary(mosaic_data, phys_data, log_data)

    counts = {
        "MATCH": sum(1 for r in dictionary if r["match_status"] == "MATCH"),
        "TYPE_MISMATCH": sum(1 for r in dictionary if r["match_status"] == "TYPE_MISMATCH"),
        "IN_MOSAIC_ONLY": sum(1 for r in dictionary if r["match_status"] == "IN_MOSAIC_ONLY"),
        "IN_PHYSICAL_ONLY": sum(1 for r in dictionary if r["match_status"] == "IN_PHYSICAL_ONLY"),
    }

    print(f"\nSummary:")
    for k, v in counts.items():
        print(f"  {k:18s}: {v}")
    print(f"  {'Total columns':18s}: {len(dictionary)}")

    write_csv(dictionary, OUTPUT_CSV)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
