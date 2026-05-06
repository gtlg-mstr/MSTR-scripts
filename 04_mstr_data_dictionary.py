#!/usr/bin/env python3
"""
MicroStrategy Data Dictionary Generator
=======================================
Compares a Mosaic model against its actual data source tables,
outputting a side-by-side CSV with MATCH / TYPE_MISMATCH / IN_MOSAIC_ONLY / IN_PHYSICAL_ONLY.

Usage:
    # Compare a model (auto-discovers datasources from pipeline metadata)
    python 04_mstr_data_dictionary.py \\
        --model-id D9F9D6AF8512455F813A58F150FD56BB

    # Specify datasource name + table for physical queries (recommended)
    python 04_mstr_data_dictionary.py \\
        --model-id D9F9D6AF8512455F813A58F150FD56BB \\
        --datasource "glagrange - Postgresql" --table LRX_pg_orders --schema public \\
        --datasource "glagrange - MicroSoft SQL" --table LRX_mssql_rx --schema dbo

    # Custom output
    python 04_mstr_data_dictionary.py --model-id <ID> --output my_dict.csv
"""

import argparse
import csv
import json
import os
import re
from pathlib import Path
from typing import Any

from mstrio import connection
from mstrio.datasources import DatasourceInstance

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


def _normalize_type(dtype: str) -> str:
    """Normalize a data type string to a short canonical form for comparison."""
    dtype = dtype.upper().strip()
    m = re.search(r"'type'\s*:\s*'([^']*)'", dtype, re.IGNORECASE)
    if m:
        dtype = m.group(1).upper()
    else:
        dtype = dtype.upper()

    TYPE_ALIASES = {
        'FIXED_LENGTH_STRING': 'VARCHAR',
        'LONG_VARCHAR':        'VARCHAR',
        'TEXT':                'VARCHAR',
        'WIDE_CHAR':           'VARCHAR',
        'VARIABLE_LENGTH_STRING': 'VARCHAR',
        'UTF8_CHAR':           'VARCHAR',
        'CHAR':                'CHAR',
        'VARCHAR':             'VARCHAR',
        'STRING':              'VARCHAR',
        'NUMERIC':             'DECIMAL',
        'DECIMAL':             'DECIMAL',
        'FLOAT':               'DOUBLE',
        'DOUBLE':              'DOUBLE',
        'DOUBLE PRECISION':    'DOUBLE',
        'REAL':                'FLOAT',
        'INT':                 'BIGINT',
        'INTEGER':             'BIGINT',
        'INT64':               'BIGINT',
        'MEDIUMINT':           'BIGINT',
        'SMALLINT':            'SMALLINT',
        'TINYINT':             'TINYINT',
        'BIGINT':              'BIGINT',
        'BOOL':                'BOOLEAN',
        'BOOLEAN':             'BOOLEAN',
        'BIT':                 'BOOLEAN',
        'DATE':                'DATE',
        'DATETIME':            'TIMESTAMP',
        'TIMESTAMP':           'TIMESTAMP',
        'TIME':                'TIME',
        'TIME_STAMP':          'TIMESTAMP',
        'TIMESTAMP WITHOUT TIME ZONE': 'TIMESTAMP',
        re.compile(r'^ARRAY<.*>$'): 'ARRAY',
    }
    for key, val in TYPE_ALIASES.items():
        if isinstance(key, re.Pattern) and key.match(dtype):
            return val
    return TYPE_ALIASES.get(dtype, dtype)


def _resolve_datasource_id(conn: connection.Connection, name: str) -> str | None:
    """Resolve a datasource display name to its ID via server-wide /api/datasources."""
    r = conn.get(endpoint="/api/datasources")
    if not r.ok:
        return None
    ds_list = r.json().get("datasources", [])
    name_lower = name.lower()
    for ds in ds_list:
        if ds.get("name", "").lower() == name_lower:
            return ds["id"]
    for ds in ds_list:
        if name_lower in ds.get("name", "").lower():
            return ds["id"]
    return None


def _get_db_type(conn: connection.Connection, ds_id: str) -> str:
    """Detect whether datasource is postgres, mssql, bigquery, etc."""
    r = conn.get(endpoint="/api/datasources")
    if r.ok:
        for ds in r.json().get("datasources", []):
            if ds.get("id") == ds_id:
                return ds.get("database", {}).get("type", "").lower()
    return "unknown"


def _extract_pipeline_ds_info(physical_table: dict) -> dict:
    """Extract dataSourceId, namespace, tableName from the pipeline JSON."""
    pipeline_str = physical_table.get("pipeline", "{}")
    try:
        pipeline = json.loads(pipeline_str) if isinstance(pipeline_str, str) else pipeline_str
    except Exception:
        return {}
    for child in pipeline.get("rootTable", {}).get("children", []):
        src = child.get("importSource", {})
        if src:
            return {
                "datasource_id": src.get("dataSourceId", ""),
                "namespace": src.get("namespace", ""),
                "table_name": src.get("tableName", ""),
            }
    return {}


# ---------------------------------------------------------------------------
# FETCHERS
# ---------------------------------------------------------------------------

def fetch_model_columns(conn: connection.Connection, model_id: str) -> list[dict]:
    """Pull column definitions directly from the Mosaic model's physical tables."""
    print("\nFetching model column definitions...")
    r = conn.get(
        endpoint=f"/api/model/dataModels/{model_id}/tables",
        params={"fields": "information,physicalTable", "limit": 100},
    )
    if not r.ok:
        raise RuntimeError(f"Failed to fetch model tables: {r.text[:500]}")

    tables = r.json().get("tables", [])
    all_columns = []

    for tbl in tables:
        info = tbl.get("information", {})
        pt = tbl.get("physicalTable", {})
        tname = info.get("name", "")

        cols = pt.get("columns", [])
        for col in cols:
            cinfo = col.get("information", {})
            ctype = col.get("dataType", {})
            stype = col.get("sourceDataType", {})

            if isinstance(ctype, dict):
                data_type = ctype.get("type", "")
            elif isinstance(ctype, str):
                data_type = ctype
            else:
                data_type = ""

            all_columns.append({
                "model_table": tname,
                "column_name": cinfo.get("name", ""),
                "column_id":   cinfo.get("objectId", ""),
                "data_type":   data_type,
            })
        print(f"  {tname}: {len(cols)} columns")

    print(f"  Total: {len(all_columns)} model columns")
    return all_columns


def fetch_physical_columns(
    conn: connection.Connection,
    ds_name: str,
    table_name: str,
    schema_name: str = "public",
) -> list[dict]:
    """Query a physical datasource via INFORMATION_SCHEMA."""
    ds_id = _resolve_datasource_id(conn, ds_name)
    if not ds_id:
        print(f"  ERROR: Datasource \"{ds_name}\" not found")
        return []

    db_type = _get_db_type(conn, ds_id)
    fq_table = "INFORMATION_SCHEMA.COLUMNS"

    if "sql_server" in db_type or "mssql" in db_type:
        query = f"SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type FROM {fq_table} WHERE TABLE_NAME='{table_name}' ORDER BY ORDINAL_POSITION"
    else:
        query = f"SELECT column_name, data_type FROM {fq_table} WHERE table_name='{table_name}' ORDER BY ordinal_position"

    print(f"  Querying \"{ds_name}\" ({ds_id[:12]}...) for {schema_name}.{table_name}...")
    try:
        ds_obj = DatasourceInstance(connection=conn, id=ds_id)
        result = ds_obj.execute_query(project_id=conn.project_id, query=query)
    except Exception as e:
        print(f"  ERROR: query failed: {e}")
        return []

    data = result.get("results", {}).get("data", {})
    if not data:
        print(f"  Warning: no data returned")
        return []

    col_names = data.get("column_name", [])
    col_types = data.get("data_type", [])

    columns = []
    for i in range(len(col_names)):
        columns.append({
            "table_name": table_name,
            "datasource_name": ds_name,
            "column_name": col_names[i],
            "data_type": col_types[i] if i < len(col_types) else "",
        })
    print(f"  Found {len(columns)} physical columns")
    return columns


# ---------------------------------------------------------------------------
# BUILD DICTIONARY
# ---------------------------------------------------------------------------

def build_dictionary(
    model_columns: list[dict],
    phys_columns: list[dict],
) -> list[dict]:
    """Cross-reference model and physical columns, normalizing types."""
    # Index physical by (table_upper, column_upper)
    phys_index = {}
    for pc in phys_columns:
        key = (pc["table_name"].upper(), pc["column_name"].upper())
        phys_index[key] = pc

    model_tables = set(mc["model_table"].upper() for mc in model_columns)
    dictionary = []

    for mc in model_columns:
        key = (mc["model_table"].upper(), mc["column_name"].upper())
        pc = phys_index.get(key)

        model_dtype = _normalize_type(mc["data_type"])
        phys_dtype  = _normalize_type(pc["data_type"]) if pc else ""

        if pc is None:
            status = "IN_MOSAIC_ONLY"
        elif model_dtype != phys_dtype:
            status = "TYPE_MISMATCH"
        else:
            status = "MATCH"

        dictionary.append({
            "mosaic_table":      mc["model_table"],
            "datasource_name":   pc["datasource_name"] if pc else "",
            "column_name":       mc["column_name"],
            "mosaic_data_type":  model_dtype,
            "physical_data_type": phys_dtype,
            "match_status":      status,
        })

    # Catch physical-only columns (exist in DB but not in model)
    for key, pc in phys_index.items():
        if key[0] in model_tables:
            if not any(
                mc["model_table"].upper() == key[0] and mc["column_name"].upper() == key[1]
                for mc in model_columns
            ):
                dictionary.append({
                    "mosaic_table":      key[0],
                    "datasource_name":   pc["datasource_name"],
                    "column_name":       pc["column_name"],
                    "mosaic_data_type":  "",
                    "physical_data_type": _normalize_type(pc["data_type"]),
                    "match_status":      "IN_PHYSICAL_ONLY",
                })

    return dictionary


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], path: str) -> None:
    if not rows:
        print("No rows to write.")
        return
    fieldnames = ["mosaic_table", "datasource_name", "column_name",
                  "mosaic_data_type", "physical_data_type", "match_status"]
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
    parser = argparse.ArgumentParser(
        description="Compare a Mosaic model against its physical data sources"
    )
    parser.add_argument("--model-id", "-m", required=True,
                        help="MicroStrategy Mosaic model ID")
    parser.add_argument("--datasource", "-d", action="append", default=None,
                        help="Datasource display name (paired with --table)")
    parser.add_argument("--table", "-t", action="append", default=None,
                        help="Table name (paired with --datasource)")
    parser.add_argument("--schema", "-s", action="append", default=None,
                        help="Schema/namespace name (default: public)")
    parser.add_argument("--output", "-o", default=None,
                        help="Output CSV path (default: data_dictionary.csv)")

    args = parser.parse_args()

    MSTR_BASE_URL = _env("MSTR_BASE_URL")
    MSTR_USERNAME = _env("MSTR_USERNAME")
    MSTR_PASSWORD = _env("MSTR_PASSWORD")
    MSTR_PROJECT_NAME = _env("MSTR_PROJECT_NAME", "Shared Studio")
    MSTR_LOGIN_MODE = int(_env("MSTR_LOGIN_MODE", "1"))

    output_csv = args.output or "data_dictionary.csv"

    print("=" * 60)
    print("MicroStrategy Data Dictionary Generator")
    print("=" * 60)
    print(f"  Model ID  : {args.model_id}")
    print(f"  Output    : {output_csv}")
    print()

    conn = connection.Connection(
        base_url=MSTR_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_name=MSTR_PROJECT_NAME,
        login_mode=MSTR_LOGIN_MODE,
    )
    print(f"Connected. I-Server version: {conn.iserver_version}")

    # Fetch model columns
    model_columns = fetch_model_columns(conn, args.model_id)

    # Fetch physical columns
    phys_columns = []
    if args.datasource:
        ds_names = args.datasource
        table_list = args.table or []
        schema_list = args.schema or []
        while len(table_list) < len(ds_names):
            table_list.append("")
        while len(schema_list) < len(ds_names):
            schema_list.append("public")

        print(f"\nQuerying physical data sources ({len(ds_names)} target(s))...")
        for i in range(len(ds_names)):
            if table_list[i]:
                cols = fetch_physical_columns(conn, ds_names[i], table_list[i], schema_list[i])
                phys_columns.extend(cols)
    else:
        # Auto-discover from pipeline metadata
        print("\nAuto-discovering datasources from model pipeline metadata...")
        r = conn.get(
            endpoint=f"/api/model/dataModels/{args.model_id}/tables",
            params={"fields": "information,physicalTable", "limit": 100},
        )
        if r.ok:
            for tbl in r.json().get("tables", []):
                info = tbl.get("information", {})
                pt = tbl.get("physicalTable", {})
                ds_info = _extract_pipeline_ds_info(pt)
                if ds_info.get("datasource_id"):
                    # Resolve datasource name
                    ds_name = ds_info["datasource_id"]
                    dsr = conn.get(endpoint="/api/datasources")
                    if dsr.ok:
                        for d in dsr.json().get("datasources", []):
                            if d.get("id") == ds_info["datasource_id"]:
                                ds_name = d.get("name", ds_info["datasource_id"])
                                break
                    cols = fetch_physical_columns(
                        conn, ds_name,
                        ds_info.get("table_name", info.get("name", "")),
                        ds_info.get("namespace", "public"),
                    )
                    phys_columns.extend(cols)

    print("\nBuilding data dictionary...")
    dictionary = build_dictionary(model_columns, phys_columns)

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

    write_csv(dictionary, output_csv)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
