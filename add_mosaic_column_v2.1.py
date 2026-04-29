#!/usr/bin/env python3
# Tested on Strategy One April 2026 release.

"""
Add a warehouse column as an Attribute or Fact to an existing MicroStrategy Mosaic Model.

Uses mstrio-py for connection / session management (like create_mosaic_model_v1.py)
and raw REST calls for model editing (mstrio does not expose Mosaic mutations).

Usage:
    python add_mosaic_column_v2.1.py \
        --model-id AC49E200F8E041C1AF38D87B99EF19DC \
        --table-name GRASP_flight_trips \
        --column-name GRASP_ex \
        --attr-name "GRASP Ex" \
        --object-type attribute

    python add_mosaic_column_v2.1.py \
        --model-id AC49E200F8E041C1AF38D87B99EF19DC \
        --table-name GRASP_flight_trips \
        --column-name total_cost \
        --attr-name "Total Cost" \
        --object-type fact
"""

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path

import requests
from mstrio.connection import Connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger("add_mosaic_column_v2.1")

# ═══════════════════════════════════════════════════════════════════════════════
# 1. ENV LOADER  (same parser as create_mosaic_model_v1.py)
# ═══════════════════════════════════════════════════════════════════════════════

def _load_env(dotenv: Path = Path("/home/support/dev-projects/Scripts/.env")) -> dict:
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


# ═══════════════════════════════════════════════════════════════════════════════
# 2. MSTRSession  (mirrors create_mosaic_model_v1.py)
# ═══════════════════════════════════════════════════════════════════════════════

class MSTRSession:
    """Thin wrapper around mstrio.connection.Connection for the model workflow."""

    def __init__(self, base_url: str, username: str, password: str,
                 project_name: str = "Shared Studio", login_mode: int = 1):
        self._base_url = base_url
        self._username = username
        self._password = password
        self._conn = Connection(
            base_url=base_url, username=username, password=password,
            project_name=project_name, login_mode=login_mode, ssl_verify=True,
        )
        self._changeset = None
        self._raw_session = None
        self._auth_token = None
        LOG.info("Connected; project = %s", self._conn.project_name)

    # ------------------------------------------------------------------
    # mstrio-backed helpers
    # ------------------------------------------------------------------
    def _resp(self, r, exp=(200, 201, 202, 204, 207)):
        if r.status_code not in (exp if isinstance(exp, (list, tuple)) else (exp,)):
            msg = f"HTTP {r.status_code}: {r.text[:500]}"
            raise RuntimeError(msg)
        return r

    def _req(self, method: str, path: str, **kw):
        headers = kw.pop("headers", {})
        if path.startswith("/api/model/") or path.startswith("/api/data"):
            headers.setdefault("X-MSTR-ProjectID", self._conn.project_id)
        if self._changeset and path.startswith("/api/model/"):
            headers.setdefault("X-MSTR-MS-Changeset", self._changeset)
        if headers:
            kw["headers"] = headers
        m = getattr(self._conn, method.lower())
        return m(endpoint=path, **kw)

    def _get(self, path, **kw):    return self._resp(self._req("GET", path, **kw))
    def _post(self, path, **kw):   return self._resp(self._req("POST", path, **kw))
    def _put(self, path, **kw):   return self._resp(self._req("PUT", path, **kw))
    def _patch(self, path, **kw):  return self._resp(self._req("PATCH", path, **kw))
    def _delete(self, path, **kw): return self._resp(self._req("DELETE", path, **kw),
                                                     exp=(200, 201, 202, 204))

    # ------------------------------------------------------------------
    # raw requests helpers  (for batch, AI service, etc.)
    # ------------------------------------------------------------------
    def _ensure_raw_session(self) -> requests.Session:
        if self._raw_session is None:
            s = requests.Session()
            r = s.post(
                f"{self._base_url}/api/auth/login",
                json={"username": self._username, "password": self._password, "loginMode": 1},
                timeout=30,
            )
            r.raise_for_status()
            self._auth_token = r.headers.get("X-MSTR-AuthToken") or r.headers.get("X-Mstr-Authtoken")
            self._raw_session = s
        return self._raw_session

    def _raw_headers(self, changeset: str = None) -> dict:
        h = {
            "X-Mstr-AuthToken": self._auth_token,
            "X-MSTR-ProjectID": self._conn.project_id,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if changeset:
            h["x-mstr-ms-changeset"] = changeset
        return h

    @property
    def project_id(self):  return self._conn.project_id
    @property
    def base_url(self):    return self._base_url

    # ------------------------------------------------------------------
    # changeset workflow
    # ------------------------------------------------------------------
    def create_changeset(self, model_id: str) -> str:
        r = self._post("/api/model/changesets", json={"dataModelId": model_id})
        cs_id = r.json()["id"]
        self._changeset = cs_id
        LOG.info("Created changeset %s", cs_id)
        # rebase
        try:
            self._post(f"/api/model/changesets/{cs_id}/operations",
                       params={"operationType": "rebase", "dataModelId": model_id})
        except Exception as e:
            LOG.warning("Rebase warning: %s", e)
        return cs_id

    def commit_changeset(self) -> bool:
        if not self._changeset:
            LOG.warning("No active changeset to commit")
            return False
        self._post(f"/api/model/changesets/{self._changeset}/commit")
        LOG.info("Changeset %s committed", self._changeset)
        self._changeset = None
        return True

    def abort_changeset(self):
        if not self._changeset:
            return
        s = self._ensure_raw_session()
        s.post(
            f"{self._base_url}/api/model/changesets/{self._changeset}/abort",
            headers=self._raw_headers(), timeout=120,
        )
        LOG.info("Aborted changeset %s", self._changeset)
        self._changeset = None

    # ------------------------------------------------------------------
    # raw batch helper
    # ------------------------------------------------------------------
    def raw_batch(self, requests_list: list, changeset: str = None,
                  allow_partial: bool = True) -> list:
        s = self._ensure_raw_session()
        params = {
            "allowPartialSuccess": "true" if allow_partial else "false",
            "showChanges": "true",
        }
        r = s.post(
            f"{self._base_url}/api/model/batch",
            headers=self._raw_headers(changeset=changeset or self._changeset),
            params=params,
            json={"requests": requests_list},
            timeout=120,
        )
        if not r.ok:
            LOG.error("Batch failed: %s %s", r.status_code, r.text[:500])
            raise RuntimeError("Batch request failed")
        return r.json().get("responses", [])


# ═══════════════════════════════════════════════════════════════════════════════
# 3. TABLE & COLUMN DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════════

def get_tables(session: MSTRSession, model_id: str) -> list:
    fields = (
        "information,attributes,factMetrics,physicalTable,logicalSize,"
        "isLogicalSizeLocked,isTrueKey,isPartOfPartition,unmappedColumns,refreshPolicy"
    )
    r = session._get(f"/api/model/dataModels/{model_id}/tables",
                     params={"fields": fields, "limit": 100, "offset": 0})
    return r.json().get("tables", [])


def get_table(session: MSTRSession, model_id: str, table_id: str) -> dict:
    fields = (
        "information,attributes,factMetrics,physicalTable,logicalSize,"
        "isLogicalSizeLocked,isTrueKey,isPartOfPartition,unmappedColumns,refreshPolicy"
    )
    r = session._get(f"/api/model/dataModels/{model_id}/tables/{table_id}",
                     params={"fields": fields})
    return r.json()


# ═══════════════════════════════════════════════════════════════════════════════
# 4. AI COLUMN TYPE INFERENCE
# ═══════════════════════════════════════════════════════════════════════════════

def infer_column_type(session: MSTRSession, table_name: str, column_name: str) -> dict:
    payload = {
        "payload": {
            "type": "infer",
            "enableGeoRoleInference": True,
            "async": True,
            "tableName": table_name,
            "columns": [{"columnName": column_name, "dataType": None}],
        }
    }
    s = session._ensure_raw_session()
    r = s.post(
        f"{session.base_url}/api/aiservice/model/v2/columns/infer",
        headers=session._raw_headers(), json=payload, timeout=60,
    )
    r.raise_for_status()
    task = r.json()
    task_id = task.get("taskId")
    total_batches = task.get("totalBatches", 1)

    for b in range(total_batches):
        for _attempt in range(30):
            status = s.get(
                f"{session.base_url}/api/aiservice/model/v2/columns/infer/status/{task_id}/{b}",
                headers=session._raw_headers(), timeout=30,
            )
            status.raise_for_status()
            st = status.json()
            if st.get("status") == "completed":
                break
            elif st.get("status") == "failed":
                raise RuntimeError(f"Inference failed for batch {b}")
            import time; time.sleep(2)

    last = s.get(
        f"{session.base_url}/api/aiservice/model/v2/columns/infer/status/{task_id}/{total_batches-1}",
        headers=session._raw_headers(), timeout=30,
    )
    last.raise_for_status()
    result = last.json().get("result", [])
    if isinstance(result, list) and len(result) >= total_batches:
        all_cols = []
        for b in range(total_batches):
            st2 = s.get(
                f"{session.base_url}/api/aiservice/model/v2/columns/infer/status/{task_id}/{b}",
                headers=session._raw_headers(), timeout=30,
            )
            all_cols.extend(st2.json().get("result", []))
        for c in all_cols:
            if c.get("columnName") == column_name:
                return c
    return result[0] if isinstance(result, list) and result else {}


# ═══════════════════════════════════════════════════════════════════════════════
# 5. PAYLOAD BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

def build_column_entry(col_name: str, mstr_type: str, precision: int = 32000) -> dict:
    type_map = {
        "string": "utf8_char", "integer": "int64", "number": "double",
        "float": "double", "boolean": "bool", "date": "date",
        "datetime": "timestamp", "time": "time", "uuid": "guid",
    }
    mapped = type_map.get(mstr_type.lower(), mstr_type)
    return {
        "id": uuid.uuid4().hex.upper(),
        "name": col_name,
        "dataType": {"type": mapped, "precision": precision, "scale": 0},
        "sourceDataType": {"type": mapped, "precision": precision, "scale": 0},
    }


def build_attribute_payload(attr_name: str, description: str, col_name: str,
                            table_id: str, col_id: str) -> dict:
    return {
        "information": {"name": attr_name, "description": description},
        "forms": [{
            "name": attr_name,
            "description": description,
            "displayFormat": "text",
            "expressions": [{
                "expression": {
                    "tree": {"objectId": col_id, "type": "column_reference"}
                },
                "tables": [{"objectId": table_id, "subType": "logical_table"}],
            }],
            "semanticRole": "none",
        }],
        "keyForm": {"name": attr_name},
        "displays": {
            "reportDisplays": [{"name": attr_name}],
            "browseDisplays": [{"name": attr_name}],
        },
        "autoDetectLookupTable": True,
        "attributeLookupTable": {"objectId": table_id, "subType": "logical_table"},
    }


def build_fact_payload(fact_name: str, description: str, col_name: str,
                       table_id: str, col_id: str, data_type: str = "double") -> dict:
    return {
        "information": {"name": fact_name, "description": description},
        "fact": {
            "expressions": [{
                "expression": {
                    "tree": {
                        "type": "column_reference",
                        "columnName": col_name,
                        "objectId": col_id,
                    }
                },
                "tables": [{"objectId": table_id, "subType": "logical_table"}],
            }]
        },
        "dataType": data_type,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 6. MAIN WORKFLOW
# ═══════════════════════════════════════════════════════════════════════════════

def run(args: argparse.Namespace):
    env = _load_env()
    base_url = env.get("MSTR_BASE_URL", "https://studio.strategy.com/MicroStrategyLibrary")

    session = MSTRSession(
        base_url=base_url,
        username=env["MSTR_USERNAME"],
        password=env["MSTR_PASSWORD"],
        project_name=env.get("MSTR_PROJECT_NAME", "Shared Studio"),
        login_mode=int(env.get("MSTR_LOGIN_MODE", "1")),
    )

    try:
        # 1. Resolve table
        LOG.info("Resolving table '%s' in model %s ...", args.table_name, args.model_id)
        tables = get_tables(session, args.model_id)
        table = next(
            (t for t in tables if t.get("information", {}).get("name") == args.table_name),
            None,
        )
        if not table:
            raise RuntimeError(f"Table '{args.table_name}' not found in model {args.model_id}")
        table_id = table["information"]["objectId"]
        LOG.info("Target table %s (%s)", args.table_name, table_id)

        # 2. Infer column type (optional)
        mstr_type = "utf8_char"
        precision = 32000
        description = args.description or ""
        if args.infer_type:
            LOG.info("Inferring column type for '%s' via AI service ...", args.column_name)
            inferred = infer_column_type(session, args.table_name, args.column_name)
            dt = inferred.get("dataType", {}) if isinstance(inferred, dict) else {}
            if isinstance(dt, dict):
                mstr_type = dt.get("type", "utf8_char")
                precision = dt.get("precision", 32000)
            elif isinstance(dt, str):
                mstr_type = dt
            if not description:
                description = inferred.get("description", f"Inferred {args.object_type} for {args.column_name}")
            LOG.info("Inferred type %s precision %d", mstr_type, precision)

        # 3. Create changeset
        cs = session.create_changeset(args.model_id)

        # 4. Get full table definition & inject column into pipeline
        table_def = get_table(session, args.model_id, table_id)
        col_entry = build_column_entry(args.column_name, mstr_type, precision)
        simple_col = {
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
                if not any(c.get("name") == simple_col["name"] for c in child["columns"]):
                    child["columns"].append(simple_col)
        updated_pipeline = json.dumps(pipeline)
        temp_col_id = col_entry["id"]
        LOG.info("Generated column ID %s and injected into pipeline", temp_col_id)

        # 5. PATCH table (only pipeline update)
        patch_body = {
            "information": {},
            "physicalTable": {
                "type": pt.get("type", "pipeline"),
                "pipeline": updated_pipeline,
            },
        }
        patch_responses = session.raw_batch(
            [{"method": "PATCH", "path": f"/model/dataModels/{args.model_id}/tables/{table_id}", "body": patch_body}],
            changeset=cs,
        )
        for resp in patch_responses:
            status = resp.get("status", 0)
            LOG.info("Patch response: HTTP %d", status)
            if status >= 400:
                LOG.error("Body: %s", json.dumps(resp.get("body", {}), indent=2)[:500])
                raise RuntimeError(f"Table PATCH failed with HTTP {status}")

        # 6. Extract real server-assigned column ID
        real_col_id = None
        patch_body_resp = patch_responses[0].get("body", {})
        for col in patch_body_resp.get("physicalTable", {}).get("columns", []):
            if col.get("information", {}).get("name") == args.column_name:
                real_col_id = col["information"]["objectId"]
                break
        if not real_col_id:
            LOG.error("Server did not return a column ID for '%s'", args.column_name)
            raise RuntimeError("Server did not return column ID")
        LOG.info("Server-assigned column ID: %s", real_col_id)

        # 7. Create attribute or fact
        attr_name = args.attr_name or args.column_name.replace("_", " ").title()
        if args.object_type == "attribute":
            payload = build_attribute_payload(
                attr_name, description, args.column_name, table_id, real_col_id
            )
            path = f"/model/dataModels/{args.model_id}/attributes?showExpressionAs=tree&showExpressionAs=tokens&allowLink=true"
        else:
            fact_type = "double" if mstr_type in ("double", "float", "real", "numeric", "decimal", "number") else mstr_type
            payload = build_fact_payload(
                attr_name, description, args.column_name, table_id, real_col_id, fact_type
            )
            path = f"/model/dataModels/{args.model_id}/factMetrics?showExpressionAs=tree&showExpressionAs=tokens"

        create_responses = session.raw_batch(
            [{"method": "POST", "path": path, "body": payload}],
            changeset=cs,
        )
        for resp in create_responses:
            status = resp.get("status", 0)
            LOG.info("Create response: HTTP %d", status)
            if status >= 400:
                LOG.error("Body: %s", json.dumps(resp.get("body", {}), indent=2)[:500])
                raise RuntimeError(f"{args.object_type.upper()} POST failed with HTTP {status}")

        # 8. Commit
        if not session.commit_changeset():
            raise RuntimeError("Changeset commit failed")

        # 9. Publish
        s = session._ensure_raw_session()
        publish = s.post(
            f"{session.base_url}/api/dataModels/{args.model_id}/publish",
            headers=session._raw_headers(),
            json={"tables": [{"id": table_id, "refreshPolicy": "replace"}]},
            timeout=120,
        )
        if publish.ok:
            LOG.info("Publish queued (status=%s)", publish.status_code)
        else:
            LOG.warning("Publish returned %s — object exists but may need manual publish", publish.status_code)

        LOG.info("✅ %s '%s' added to model %s", args.object_type.upper(), attr_name, args.model_id)

    except Exception as e:
        LOG.error("Operation failed: %s", e)
        session.abort_changeset()
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. CLI
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add a warehouse column as an Attribute or Fact to a Mosaic Model"
    )
    parser.add_argument("--model-id", required=True, help="Mosaic model object ID")
    parser.add_argument("--table-name", required=True, help="Logical table name in the model")
    parser.add_argument("--column-name", required=True, help="Warehouse column name to add")
    parser.add_argument("--attr-name", default="", help="Display name for the new object (default: title-cased column name)")
    parser.add_argument("--object-type", choices=["attribute", "fact"], default="attribute",
                        help="Type of object to create (default: attribute)")
    parser.add_argument("--description", default="", help="Object description")
    parser.add_argument("--infer-type", action="store_true", default=True,
                        help="Use AI service to infer data type (default: True)")
    parser.add_argument("--no-infer-type", action="store_false", dest="infer_type",
                        help="Skip AI type inference")
    parser.add_argument("--project", default="Shared Studio", help="MSTR project name")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
