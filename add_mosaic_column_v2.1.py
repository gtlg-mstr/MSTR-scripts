#!/usr/bin/env python3
"""
Add a new warehouse column as an Attribute in a MicroStrategy Mosaic Model/Changeset (Strategy One Cloud).
Derived from reverse-engineered HAR of Studio Web UI.

Uses mstrio-py for connection and authentication.

Usage:
    python add_mosaic_column_v2.2.py --model-id 970B1CAAB8D2473F845064DF60E647FF \\
        --table-name LRX_pg_orders \\
        --column-name subscription_flag \\
        --attr-name "Subscription Flag" \\
        --description "Optional description"

Note: This is an unofficial workaround because mstrio-py SDK v11.6 does not expose
Mosaic model editing APIs.
"""

import sys, json, uuid, argparse, logging, time
from pathlib import Path

from mstrio.connection import Connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger("add_mosaic_column_v2.2")


def load_env(dotenv: Path = Path("/home/support/dev-projects/Scripts/.env")) -> dict:
    env = {}
    if not dotenv.exists():
        raise FileNotFoundError(f"{dotenv} not found")
    with open(dotenv, "rb") as f:
        for line in f:
            line = line.decode().strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k] = v.strip('"').strip("'")
    return env


def _h(conn: Connection, changeset: str = None) -> dict:
    h = {
        "X-MSTR-ProjectID": conn.project_id,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if changeset:
        h["x-mstr-ms-changeset"] = changeset
    return h


def create_changeset(conn: Connection, model_id: str) -> str:
    r = conn.post(
        endpoint="/api/model/changesets",
        headers=_h(conn),
        params={"enableOperationHistory": "true"},
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Changeset creation failed: {r.text[:500]}")
    cs = r.json()["id"]
    LOG.info("Created changeset %s", cs)
    rebase = conn.post(
        endpoint=f"/api/model/changesets/{cs}/operations",
        headers=_h(conn),
        params={"operationType": "rebase", "dataModelId": model_id},
    )
    if not rebase.ok:
        LOG.warning("Rebase warning: %s %s", rebase.status_code, rebase.text[:200])
    return cs


def commit_changeset(conn: Connection, cs: str) -> bool:
    r = conn.post(
        endpoint=f"/api/model/changesets/{cs}/commit",
        headers=_h(conn),
    )
    if not r.ok:
        LOG.error("Commit failed: %s %s", r.status_code, r.text[:500])
        return False
    LOG.info("Changeset committed successfully")
    return True


def _batch(conn: Connection, requests_list: list, changeset: str = None, allow_partial: bool = True) -> list:
    params = {"allowPartialSuccess": "true" if allow_partial else "false", "showChanges": "true"}
    r = conn.post(
        endpoint="/api/model/batch",
        headers=_h(conn, changeset),
        params=params,
        json={"requests": requests_list},
    )
    if not r.ok:
        LOG.error("Batch request failed: %s %s", r.status_code, r.text[:500])
        raise RuntimeError("Batch request failed")
    return r.json().get("responses", [])


def get_tables(conn: Connection, model_id: str) -> list:
    fields = "information,attributes,factMetrics,physicalTable,logicalSize,isLogicalSizeLocked,isTrueKey,isPartOfPartition,unmappedColumns,refreshPolicy"
    r = conn.get(
        endpoint=f"/api/model/dataModels/{model_id}/tables",
        headers=_h(conn),
        params={"fields": fields, "limit": 100, "offset": 0},
    )
    r.raise_for_status()
    return r.json().get("tables", [])


def get_table(conn: Connection, model_id: str, table_id: str) -> dict:
    fields = "information,attributes,factMetrics,physicalTable,logicalSize,isLogicalSizeLocked,isTrueKey,isPartOfPartition,unmappedColumns,refreshPolicy"
    r = conn.get(
        endpoint=f"/api/model/dataModels/{model_id}/tables/{table_id}",
        headers=_h(conn),
        params={"fields": fields},
    )
    r.raise_for_status()
    return r.json()


def infer_column_type(conn: Connection, table_name: str, column_name: str) -> dict:
    payload = {
        "payload": {
            "type": "infer",
            "enableGeoRoleInference": True,
            "async": True,
            "tableName": table_name,
            "columns": [{"columnName": column_name, "dataType": None}],
        }
    }
    r = conn.post(
        endpoint="/api/aiservice/model/v2/columns/infer",
        headers=_h(conn),
        json=payload,
    )
    r.raise_for_status()
    task = r.json()
    task_id = task.get("taskId")
    total_batches = task.get("totalBatches", 1)
    for b in range(total_batches):
        for _ in range(30):
            s = conn.get(
                endpoint=f"/api/aiservice/model/v2/columns/infer/status/{task_id}/{b}",
                headers=_h(conn),
            )
            s.raise_for_status()
            st = s.json()
            if st.get("status") == "completed":
                break
            elif st.get("status") == "failed":
                raise RuntimeError(f"Inference failed for batch {b}")
            time.sleep(2)
    last_status = conn.get(
        endpoint=f"/api/aiservice/model/v2/columns/infer/status/{task_id}/{total_batches - 1}",
        headers=_h(conn),
    )
    last_status.raise_for_status()
    result = last_status.json().get("result", [])
    if isinstance(result, list) and len(result) >= total_batches:
        all_cols = []
        for b in range(total_batches):
            st2 = conn.get(
                endpoint=f"/api/aiservice/model/v2/columns/infer/status/{task_id}/{b}",
                headers=_h(conn),
            )
            all_cols.extend(st2.json().get("result", []))
        for c in all_cols:
            if c.get("columnName") == column_name:
                return c
    return result[0] if isinstance(result, list) and result else {}


def build_column_entry(col_name: str, mstr_type: str, precision: int = 32000) -> dict:
    type_map = {
        "string": "utf8_char",
        "integer": "int64",
        "number": "double",
        "float": "double",
        "boolean": "bool",
        "date": "date",
        "datetime": "timestamp",
        "time": "time",
        "uuid": "guid",
    }
    mapped = type_map.get(mstr_type.lower(), mstr_type)
    return {
        "id": uuid.uuid4().hex.upper(),
        "name": col_name,
        "dataType": {"type": mapped, "precision": precision, "scale": 0},
        "sourceDataType": {"type": mapped, "precision": precision, "scale": 0},
    }


def build_attribute_payload(attr_name: str, description: str, col_name: str, table_id: str, col_id: str) -> dict:
    return {
        "information": {"name": attr_name, "description": description},
        "forms": [
            {
                "name": attr_name,
                "description": description,
                "displayFormat": "text",
                "expressions": [
                    {
                        "expression": {
                            "tree": {
                                "objectId": col_id,
                                "type": "column_reference",
                            }
                        },
                        "tables": [
                            {"objectId": table_id, "subType": "logical_table"}
                        ],
                    }
                ],
                "semanticRole": "none",
            }
        ],
        "keyForm": {"name": attr_name},
        "displays": {
            "reportDisplays": [{"name": attr_name}],
            "browseDisplays": [{"name": attr_name}],
        },
        "autoDetectLookupTable": True,
        "attributeLookupTable": {"objectId": table_id, "subType": "logical_table"},
    }


def run(model_id, table_name, column_name, attr_name, description):
    env = load_env()
    base_url = env.get("MSTR_BASE_URL", "https://studio.strategy.com/MicroStrategyLibrary")

    conn = Connection(
        base_url=base_url,
        username=env["MSTR_USERNAME"],
        password=env["MSTR_PASSWORD"],
        project_name=env.get("MSTR_PROJECT_NAME", "Shared Studio"),
        login_mode=int(env.get("MSTR_LOGIN_MODE", "1")),
    )
    LOG.info("Connected to project %s", conn.project_name)

    try:
        tables = get_tables(conn, model_id)
        table = next((t for t in tables if t.get("information", {}).get("name") == table_name), None)
        if not table:
            raise RuntimeError(f"Table '{table_name}' not found in model {model_id}")
        table_id = table["information"]["objectId"]
        LOG.info("Target table %s (%s)", table_name, table_id)

        LOG.info("Inferring column type for %s via AI service ...", column_name)
        inferred = infer_column_type(conn, table_name, column_name)
        mstr_type = "utf8_char"
        precision = 32000
        dt = inferred.get("dataType", {}) if isinstance(inferred, dict) else {}
        if isinstance(dt, dict):
            mstr_type = dt.get("type", "utf8_char")
            precision = dt.get("precision", 32000)
        elif isinstance(dt, str):
            mstr_type = dt
        if not description:
            description = inferred.get("description", f"Inferred attribute for {column_name}") if isinstance(inferred, dict) else f"Inferred attribute for {column_name}"
        LOG.info("Inferred type %s precision %d", mstr_type, precision)

        cs = create_changeset(conn, model_id)

        table_def = get_table(conn, model_id, table_id)

        col_entry = build_column_entry(column_name, mstr_type, precision)
        simple_col_entry = {
            "id": col_entry["id"],
            "name": col_entry["name"],
            "dataType": col_entry["dataType"],
            "sourceDataType": col_entry["sourceDataType"],
        }
        pt = table_def.get("physicalTable", {})
        pipeline_str = pt.get("pipeline", "{}")
        pipeline = json.loads(pipeline_str) if isinstance(pipeline_str, str) else (pipeline_str if isinstance(pipeline_str, dict) else {})
        for child in pipeline.get("rootTable", {}).get("children", []):
            if "columns" in child:
                if not any(c.get("name") == simple_col_entry["name"] for c in child["columns"]):
                    child["columns"].append(simple_col_entry)
        updated_pipeline = json.dumps(pipeline)
        LOG.info("Generated column ID %s and injected into definition", col_entry["id"])

        patch_body = {
            "information": {},
            "physicalTable": {
                "type": pt.get("type", "pipeline"),
                "pipeline": updated_pipeline,
            },
        }
        requests_list = [
            {
                "method": "PATCH",
                "path": f"/model/dataModels/{model_id}/tables/{table_id}",
                "body": patch_body,
            },
        ]

        LOG.info("Sending batch request (PATCH table only)")
        patch_responses = _batch(conn, [requests_list[0]], changeset=cs)
        for i, resp in enumerate(patch_responses):
            status = resp.get("status", 0)
            LOG.info("Patch response %d: HTTP %d", i, status)
            if status >= 400:
                LOG.error("Body: %s", json.dumps(resp.get("body", {}), indent=2)[:500])
                raise RuntimeError(f"Patch operation failed with HTTP {status}")

        patch_body_resp = patch_responses[0].get("body", {})
        real_col_id = None
        for col in patch_body_resp.get("physicalTable", {}).get("columns", []):
            if col.get("information", {}).get("name") == column_name:
                real_col_id = col["information"]["objectId"]
                break
        if not real_col_id:
            LOG.error("Could not find server-assigned column ID for %s", column_name)
            raise RuntimeError("Server did not return column ID")
        LOG.info("Server assigned column ID: %s", real_col_id)

        attr_payload = build_attribute_payload(attr_name, description, column_name, table_id, real_col_id)

        LOG.info("Sending batch request (POST attribute)")
        attr_responses = _batch(
            conn,
            [{
                "method": "POST",
                "path": f"/model/dataModels/{model_id}/attributes?showExpressionAs=tree&showExpressionAs=tokens&allowLink=true",
                "body": attr_payload,
            }],
            changeset=cs,
        )
        for i, resp in enumerate(attr_responses):
            status = resp.get("status", 0)
            LOG.info("Attribute response %d: HTTP %d", i, status)
            if status >= 400:
                LOG.error("Body: %s", json.dumps(resp.get("body", {}), indent=2)[:500])
                raise RuntimeError(f"Attribute POST failed with HTTP {status}")

        if not commit_changeset(conn, cs):
            sys.exit(1)

        table_ids = [table_id] + [t["information"]["objectId"] for t in tables if t["information"]["objectId"] != table_id]
        publish = conn.post(
            endpoint=f"/api/dataModels/{model_id}/publish",
            headers=_h(conn),
            json={"tables": [{"id": tid, "refreshPolicy": "replace"} for tid in table_ids]},
        )
        if publish.ok:
            LOG.info("Publish queued (status=%s)", publish.status_code)
        else:
            LOG.warning("Publish returned %s – new attribute exists but may need manual publish", publish.status_code)

        LOG.info("✅ Attribute '%s' added to model %s", attr_name, model_id)

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add a warehouse column as a Mosaic Model attribute")
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--column-name", required=True)
    parser.add_argument("--attr-name", required=True)
    parser.add_argument("--description", default="")
    args = parser.parse_args()
    run(args.model_id, args.table_name, args.column_name, args.attr_name, args.description)
