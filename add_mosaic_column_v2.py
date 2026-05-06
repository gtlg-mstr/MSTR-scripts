#!/usr/bin/env python3
"""
Add a PostgreSQL column to a MicroStrategy MOSAIC model.

This script bypasses the traditional Physical Table / Schema Lock workflow
and creates an Attribute or Fact directly in the Mosaic model context.

Usage:
    python add_mosaic_column.py           # Create as Attribute (default)
    python add_mosaic_column.py --attribute
    python add_mosaic_column.py --fact
"""
import os
import sys
from pathlib import Path

from mstrio import connection
from mstrio.modeling import Attribute, Fact, SchemaManagement, SchemaLockType
from mstrio.modeling import list_datasource_warehouse_tables

# ── Load credentials ──────────────────────────────────────────────────────
_ENV_PATH = Path("/home/support/dev-projects/Scripts/.env")

def _load_env(path: Path):
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_env(_ENV_PATH)

# ── Config ──────────────────────────────────────────────────────────────────
MSTR_BASE_URL = os.getenv("MSTR_BASE_URL")
MSTR_USER     = os.getenv("MSTR_USERNAME")
MSTR_PASS     = os.getenv("MSTR_PASSWORD")
MSTR_PROJECT  = os.getenv("MSTR_PROJECT_NAME", "Shared Studio")

# ── T A R G E T S ──────────────────────────────────────────────────────────
# The datasource must be connected to the mosaic model in the UI
DATASOURCE_NAME = "glagrange - Postgresql"
MOSAIC_MODEL    = "Hermy Test Dual 20260501"  # Object name of the mosaic model
TABLE_NAME      = "LRX_pg_orders"
COLUMN_NAME     = "subscription_flag"
OBJECT_TYPE     = "attribute"          # Set via CLI arg

# ===========================================================================
# HELPERS
# ===========================================================================

def connect():
    if not all([MSTR_BASE_URL, MSTR_USER, MSTR_PASS]):
        raise SystemExit("Missing MSTR credentials in .env")
    print(f"Connecting to {MSTR_BASE_URL} as {MSTR_USER}...")
    conn = connection.Connection(
        base_url=MSTR_BASE_URL,
        username=MSTR_USER,
        password=MSTR_PASS,
        project_name=MSTR_PROJECT,
        login_mode=int(os.getenv("MSTR_LOGIN_MODE", "1")),
    )
    print(f"Connected. Project: {conn.project_name}")
    return conn


def find_datasource(conn, name_hint: str):
    """Search ALL server-wide datasource instances."""
    from mstrio.datasources.datasource_instance import list_datasource_instances
    ds_list = list_datasource_instances(connection=conn)
    matches = [d for d in ds_list if name_hint.lower() == d.name.lower()]
    if not matches:
        # fallback: partial match
        matches = [d for d in ds_list if name_hint.lower() in d.name.lower()]
    if not matches:
        raise RuntimeError(f"No datasource matching '{name_hint}'")
    ds = matches[0]
    print(f"  Datasource: {ds.name} (ID={ds.id})")
    return ds.id


def find_warehouse_table(conn, ds_id: str, table_name: str):
    """Return the WarehouseTable object + column info."""
    from mstrio.modeling.namespace import list_namespaces

    ns_list = list_namespaces(conn, id=ds_id)
    print(f"  Namespaces: {[n['name'] for n in ns_list]}")

    for ns in ns_list:
        try:
            tables = list_datasource_warehouse_tables(
                conn, datasource_id=ds_id, namespace_id=ns["id"]
            )
        except Exception as e:
            print(f"    Skip {ns['name']}: {e}")
            continue

        for tbl in tables:
            if tbl.name.upper() == table_name.upper():
                print(f"  Found table: {tbl.name} (namespace={ns['name']})")
                return tbl, ns

    raise RuntimeError(f"Warehouse table '{table_name}' not found")


def resolve_pg_type(conn, ds_id: str, table: str, column: str) -> str:
    """Query the PG datasource for the actual column type."""
    from mstrio.datasources import DatasourceInstance

    print(f"\nResolving PG column type...")
    ds = DatasourceInstance(connection=conn, id=ds_id)
    result = ds.execute_query(
        project_id=conn.project_id,
        query=f"""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
              AND column_name = '{column}'
        """,
    )
    data = result.get("results", {}).get("data", {})
    types = data.get("data_type", [])
    if types:
        print(f"  PG type: {types[0]}")
        return types[0]
    print(f"  Fallback type: VARCHAR")
    return "VARCHAR"


# ===========================================================================
# MOSAIC-STYLE OBJECT CREATION
# ===========================================================================

def create_attribute_mosaic(conn, table_obj, column_name: str, pg_type: str):
    """
    Mosaic: Attribute can reference a warehouse table column via its column ID.
    If the warehouse column has a stable ID, we reference it directly.
    Otherwise we create the Attribute with a generic warehouse expression.
    """
    print(f"\nCreating Attribute '{column_name}'...")

    # Try to get the exact column from the WarehouseTable object
    columns = table_obj.list_columns()
    target_col = None
    for c in columns:
        if c.name.upper() == column_name.upper():
            target_col = c
            break

    if not target_col:
        raise RuntimeError(
            f"Column '{column_name}' not found in warehouse table '{table_obj.name}'"
        )

    print(f"  Column: {target_col.name} (type={target_col.data_type}, id={target_col.id})")

    # Map PG types to MSTR data types
    type_map = {
        "integer": "INTEGER", "bigint": "BIGINT", "smallint": "INTEGER",
        "numeric": "NUMERIC", "decimal": "NUMERIC",
        "real": "FLOAT", "double precision": "DOUBLE",
        "character varying": "VARCHAR", "varchar": "VARCHAR",
        "text": "VARCHAR", "char": "CHAR",
        "timestamp": "TIMESTAMP", "date": "DATE", "time": "TIME",
        "boolean": "BOOLEAN", "bool": "BOOLEAN",
    }
    mstr_type = type_map.get(pg_type.lower(), "VARCHAR")
    print(f"  MSTR type: {mstr_type}")

    # For Mosaic, create an Attribute with an expression that maps
    # directly to the warehouse column.  The easiest way is to build
    # the form expression using the internal warehouse reference.
    #
    # NOTE: In traditional MSTR, forms map to LogicalTable columns.
    # In Mosaic, you can use the WarehouseTable schema object reference.
    #
    # Attempt 1: Use the WarehouseTable object as the lookup table
    # and reference the column by ID in the expression.

    # Build a schema object reference to the warehouse table
    from mstrio.modeling.schema import SchemaObjectReference

    tbl_ref = SchemaObjectReference(
        id=table_obj.id,
        name=table_obj.name,
        type=ObjectSubTypes.DSSTABLENAME,   # warehouse table type
    )

    # Create the AttributeForm referencing the column
    from mstrio.modeling import AttributeForm

    form = AttributeForm(
        name="ID",
        category="ID",
        data_type=mstr_type,
        expressions=[
            {
                "expression": f"{table_obj.name}.{target_col.name}",
                "tables": [tbl_ref.to_dict()],       # reference the warehouse table
            }
        ],
    )

    # In Mosaic mode / newer MSTR, you may also need a lookup table
    # (the warehouse table acts as both source and lookup)
    attr = Attribute.create(
        connection=conn,
        name=column_name,
        sub_type=ObjectSubTypes.ATTRIBUTE,      # standard attribute
        destination_folder="",                  # default folder
        forms=[form],
        key_form=form,
        displays=... ,                          # browse / report displays
        description=f"Attribute auto-created from PG column {TABLE_NAME}.{column_name}",
    )
    print(f"  Created Attribute: {attr.id} | {attr.name}")
    return attr


def create_fact_mosaic(conn, table_obj, column_name: str, pg_type: str):
    """Create a Fact mapped directly to a warehouse column."""
    from mstrio.modeling.schema import SchemaObjectReference
    from mstrio.modeling import FactExpression

    print(f"\nCreating Fact '{column_name}'...")

    type_map = {
        "integer": "INTEGER", "bigint": "BIGINT",
        "numeric": "NUMERIC", "decimal": "NUMERIC",
        "real": "FLOAT", "double precision": "DOUBLE",
    }
    mstr_type = type_map.get(pg_type.lower(), "NUMERIC")

    tbl_ref = SchemaObjectReference(
        id=table_obj.id,
        name=table_obj.name,
        type=ObjectSubTypes.DSSTABLENAME,
    )

    fact = Fact.create(
        connection=conn,
        name=column_name,
        expressions=[
            {
                "expression": f"{table_obj.name}.{column_name}",
                "tables": [tbl_ref.to_dict()],
            }
        ],
        data_type=mstr_type,
        description=f"Fact auto-created from PG column {TABLE_NAME}.{column_name}",
    )
    print(f"  Created Fact: {fact.id} | {fact.name}")
    return fact


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    if "--fact" in sys.argv:
        object_type = "fact"
    else:
        object_type = "attribute"

    print("=" * 60)
    print("Mosaic Column Importer (v2 — No Schema Lock)")
    print("=" * 60)
    print(f"  Datasource : {DATASOURCE_NAME}")
    print(f"  Table      : {TABLE_NAME}")
    print(f"  Column     : {COLUMN_NAME}")
    print(f"  Type       : {object_type.upper()}")
    print()

    conn = connect()

    # 1. Resolve datasource
    ds_id = find_datasource(conn, DATASOURCE_NAME)

    # 2. Resolve warehouse table (Mosaic-native)
    wh_tbl, ns = find_warehouse_table(conn, ds_id, TABLE_NAME)

    # 3. Get PG type
    pg_type = resolve_pg_type(conn, ds_id, TABLE_NAME, COLUMN_NAME)

    # 4. Create object in Mosaic context (NO schema lock)
    if object_type == "attribute":
        create_attribute_mosaic(conn, wh_tbl, COLUMN_NAME, pg_type)
    else:
        create_fact_mosaic(conn, wh_tbl, COLUMN_NAME, pg_type)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
