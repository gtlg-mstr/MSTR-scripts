#!/usr/bin/env python3
# Tested on Strategy One April 2026 release.

"""
Create a Mosaic data model from multiple database tables across different datasources.

Usage (single source - backward compatible):
    python create_mosaic_model_v2.py \
        --datasource "glagrange - Postgresql" \
        --table GRASP_expense_analysis \
        --model-name "Expense Analysis Model" \
        --project "Shared Studio"

Usage (multiple sources via JSON):
    python create_mosaic_model_v2.py \
        --source-config sources.json \
        --model-name "Multi-Source Model" \
        --project "Shared Studio"

sources.json format:
    [
      {"datasource": "glagrange_Postgresql", "schema": "public", "table": "jnj_clinical_data", "date_columns": "col1,col2"},
      {"datasource": "mssql_prod",          "schema": "dbo",    "table": "sales"},
      {"datasource": "mssql_prod",          "schema": "dbo",    "table": "customers"}
    ]

Optional:
    --schema public              # default: public (used with single-source mode)
    --date-columns col1,col2   # comma-separated (used with single-source mode)
    --description "..."        # model description
    --folder-id ID             # destination folder for the model
    --keep-workspace           # do not delete workspace after commit
"""

import argparse
import json
import sys
import time
import base64
import re
from dataclasses import dataclass
from pathlib import Path

import requests
from mstrio.connection import Connection


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


class MSTRSession:
    """Thin wrapper around mstrio.connection.Connection for the model workflow."""

    def __init__(self, base_url: str, username: str, password: str,
                 project_name: str = "Shared Studio",
                 login_mode: int = 1):
        self._base_url = base_url
        self._username = username
        self._password = password
        self._conn = Connection(
            base_url=base_url,
            username=username,
            password=password,
            project_name=project_name,
            login_mode=login_mode,
            ssl_verify=True,
        )
        self._changeset = None
        self._raw_session = None
        self._auth_token = None
        print(f"[+] Connected; project = {self._conn.project_name}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _resp(self, r, exp=(200, 201, 202, 204, 207)):
        if r.status_code not in (exp if isinstance(exp, (list, tuple)) else (exp,)):
            msg = f"HTTP {r.status_code}: {r.text[:500]}"
            raise RuntimeError(msg)
        return r
    def _req(self, method: str, path: str, **kw):
        """Dispatch to mstrio Connection method using endpoint (base_url + path)."""
        headers = kw.pop("headers", {})
        if path.startswith("/api/model/") or path.startswith("/api/data"):
            headers.setdefault("X-MSTR-ProjectID", self._conn.project_id)
        if self._changeset and path.startswith("/api/model/"):
            headers.setdefault("X-MSTR-MS-Changeset", self._changeset)
        if headers:
            kw["headers"] = headers
        m = getattr(self._conn, method.lower())
        return m(endpoint=path, **kw)

    def _get(self, path, **kw):   return self._resp(self._req("GET", path, **kw))
    def _post(self, path, **kw):  return self._resp(self._req("POST", path, **kw))
    def _put(self, path, **kw):   return self._resp(self._req("PUT", path, **kw))
    def _patch(self, path, **kw): return self._resp(self._req("PATCH", path, **kw))
    def _delete(self, path, **kw): return self._resp(self._req("DELETE", path, **kw),
                                                      exp=(200, 201, 202, 204))

    # ------------------------------------------------------------------
    # AIService (raw requests — mstrio doesn't cover /api/aiservice/*)
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

    def _aiservice_headers(self) -> dict:
        return {
            "X-MSTR-AuthToken": self._auth_token,
            "X-MSTR-ProjectID": self._conn.project_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def aiservice_recommend_metrics(self, model_id: str,
                                     attributes: list[dict] | None = None,
                                     metrics: list[dict] | None = None) -> list[dict]:
        """Fetch AI-suggested metrics."""
        s = self._ensure_raw_session()
        context = {"attributes": attributes or [], "metrics": metrics or []}
        body = {
            "payload": {
                "userMessage": "suggest new metric",
                "context": context
            }
        }
        r = s.post(
            f"{self._base_url}/api/aiservice/model/objects/metrics/recommendations",
            headers=self._aiservice_headers(),
            json=body,
            timeout=120,
        )
        if r.status_code != 200:
            print(f"[!] AIService recommendations failed ({r.status_code}): {r.text[:300]}")
            return []
        data = r.json()
        categories = data.get("answer", {}).get("result", {}).get("categories", [])
        suggestions = []
        for cat in categories:
            for m in cat.get("metrics", []):
                suggestions.append({
                    "category": cat.get("category", ""),
                    "name": m.get("name", ""),
                    "query": m.get("query", ""),
                })
        return suggestions

    def aiservice_create_metric(self, model_id: str, metric_query: str) -> dict | None:
        """Ask the AI to generate a metric definition from a query string."""
        s = self._ensure_raw_session()
        body = {
            "payload": {
                "userMessage": metric_query,
                "context": {"attributes": [], "metrics": []}
            }
        }
        r = s.post(
            f"{self._base_url}/api/aiservice/model/objects/metrics",
            headers=self._aiservice_headers(),
            json=body,
            timeout=120,
        )
        if r.status_code != 200:
            print(f"[!] AIService metric creation failed ({r.status_code}): {r.text[:300]}")
            return None
        data = r.json()
        data_info = data.get("answer", {}).get("data", {})
        if not data_info.get("formula"):
            return None
        return {
            "name": data_info.get("name"),
            "description": data_info.get("description"),
            "formula": data_info.get("formula"),
            "type": data_info.get("type"),
        }

    # ------------------------------------------------------------------
    # convenience API wrappers
    # ------------------------------------------------------------------

    @property
    def _project_id(self):
        return self._conn.project_id

    def get_datasources(self, name_filter: str | None = None):
        r = self._get("/api/datasources")
        dss = r.json().get("datasources", [])
        if name_filter:
            dss = [d for d in dss if name_filter.lower() in d.get("name", "").lower()]
        return dss

    def find_datasource(self, name: str):
         dss = self.get_datasources(name)
         if not dss:
             raise RuntimeError(f"Datasource '{name}' not found")
         return dss[0]

    def create_changeset(self, model_id: str | None = None):
        body = {}
        if model_id:
            body["dataModelId"] = model_id
        r = self._post("/api/model/changesets", json=body)
        cs_id = r.json()["id"]
        self._changeset = cs_id
        print(f"[+] Changeset {cs_id}")
        return cs_id

    def create_model(self, name: str, folder_id: str | None = None):
        body = {
            "dataServeMode": "in_memory",
            "information": {"name": name}
        }
        if folder_id:
            body["information"]["destinationFolderId"] = folder_id
        r = self._post("/api/model/dataModels", json=body)
        j = r.json()
        model_id = j["information"]["objectId"]
        folder_id = j.get("information", {}).get("destinationFolderId")
        print(f"[+] Model created: {model_id} (folder={folder_id})")
        return model_id, folder_id

    def create_workspace(self):
        r = self._post("/api/dataServer/workspaces", json={})
        ws = r.json()["id"]
        print(f"[+] Workspace {ws}")
        return ws

    def delete_workspace(self, ws: str):
        self._delete(f"/api/dataServer/workspaces/{ws}")
        print(f"[+] Workspace {ws} deleted")

    def poll_changeset(self, cs: str, timeout=120):
        t0 = time.time()
        while time.time() - t0 < timeout:
            r = self._req("GET", f"/api/model/changesets/{cs}")
            if r.status_code == 200:
                print(f"[+] Changeset {cs} committed")
                return
            time.sleep(1)
        raise RuntimeError(f"Changeset {cs} commit timed out")

    def poll_publish_status(self, model_id: str, timeout=600):
        print(f"[ ] Polling publish status (timeout: {timeout}s)...")
        t0 = time.time()
        while time.time() - t0 < timeout:
            r = self._req("GET", f"/api/dataModels/{model_id}/publishStatus")
            if r.status_code == 200:
                j = r.json()
                status = j.get("status", -1)
                if status == 2:
                    print("[+] Publish SUCCESS")
                    return True
                elif status == 6:
                    raise RuntimeError(f"Publish FAILED: {j}")
            time.sleep(3)
        raise RuntimeError("Publish timed out")


# ------------------------------------------------------------------
# tableId encoder
# ------------------------------------------------------------------

def encode_table_id(datasource_id: str, schema: str, table_name: str) -> str:
    raw = f"{datasource_id}\x00{schema}\x00{table_name}\x00"
    b = raw.encode("utf-16le")
    return base64.b64encode(b).decode("ascii")


# ------------------------------------------------------------------
# Pipeline helpers
# ------------------------------------------------------------------

def create_pipeline_phase1(session: MSTRSession, workspace_id: str,
                           datasource_id: str, datasource_name: str,
                           schema: str, table_name: str) -> dict:
    dbms_info = session.find_datasource(datasource_name)
    db_name = dbms_info.get("database", {}).get("type", "postgre_sql")
    db_type_map = {
        "postgre_sql": {"type": "single_table", "dbType": 1800, "dbVersion": 149},
        "sql_server":  {"type": "single_table", "dbType": 26,   "dbVersion": 5},
        "mysql":       {"type": "single_table", "dbType": 15,   "dbVersion": 10},
    }
    db_cfg = db_type_map.get(db_name, db_type_map["postgre_sql"])

    table_id_b64 = encode_table_id(datasource_id, schema, table_name)

    # Step 1: create empty pipeline
    r1 = session._post(f"/api/dataServer/workspaces/{workspace_id}/pipelines", json={})
    pipeline_id = r1.json()["id"]
    print(f"[+] Empty pipeline created: {pipeline_id}")

    # Step 2: POST import source to /tables endpoint
    body = {
        "name": table_name,
        "type": "wrangle",
        "children": [{
            "name": table_name,
            "type": "source",
            "importSource": {
                "tableId": table_id_b64,
                **db_cfg,
                "dataSourceId": datasource_id,
                "dataSourceName": datasource_name,
                "tableName": table_name,
                "namespace": schema,
            },
            "filter": None
        }],
        "operations": []
    }
    r2 = session._post(
        f"/api/dataServer/workspaces/{workspace_id}/pipelines/{pipeline_id}/tables",
        json=body
    )
    print(f"[+] Import triggered (status {r2.status_code})")
    try:
        j2 = r2.json()
    except Exception as e:
        print(f"[!] /tables response parse error: {e}")
        print(f"[!] Raw: {r2.text[:500]}")
        j2 = {}

    if r2.status_code == 201 and j2.get("type") == "wrangle" and j2.get("columns"):
        print(f"[+] Pipeline ready synchronously: {j2['id']}")
        return {"ready": j2}
    elif "id" in j2:
        print(f"[+] Pipeline ID for polling: {j2['id']}")
        return {"poll_id": j2["id"]}
    else:
        return {"poll_id": pipeline_id}


def poll_pipeline(session: MSTRSession, workspace_id: str,
                  pipeline_id: str, timeout=120):
    url = f"/api/dataServer/workspaces/{workspace_id}/pipelines/{pipeline_id}"
    print(f"[ ] Polling: {url}")
    t0 = time.time()
    dots = 0
    while time.time() - t0 < timeout:
        r = session._req("GET", url)
        if r.status_code == 200:
            j = r.json()
            print(f"\n[+] Pipeline ready: {pipeline_id} (type={j.get('type')})")
            return j
        dots += 1
        if dots % 5 == 0:
            try:
                _j = r.json()
                _err = json.dumps(_j, indent=2)[:400]
                print(f"\n[ ] Poll {r.status_code}: {_err}")
            except:
                print(f"\n[ ] Poll {r.status_code}: {r.text[:200]}")
        else:
            print(".", end="", flush=True)
        time.sleep(2)
    raise RuntimeError(f"Pipeline {pipeline_id} poll timed out")


def create_pipeline_phase2(session: MSTRSession, workspace_id: str,
                           source_pipeline: dict) -> dict:
    src = source_pipeline

    # Phase-1 sync response: columns at root, children[] has source
    columns = []
    for col in src.get("columns", []):
        new_col = {
            "id": col.get("id", ""),
            "name": col["name"],
            "dataType": col.get("dataType", {}),
            "sourceDataType": col.get("sourceDataType", {}),
        }
        columns.append(new_col)
    children = src.get("children", [])
    if not children:
        raise RuntimeError("Phase-1 pipeline has no source children")
    
    source_child = children[0]
    table_name = src.get("name", "table")

    wrangle_child = {
        "id": "",
        "name": table_name,
        "type": "wrangle",
        "columns": columns,
        "operations": [],
        "children": [source_child]
    }

    body = {
        "id": "",
        "rootTable": {
            "id": "",
            "type": "root",
            "children": [wrangle_child]
        }
    }

    r = session._post(
        f"/api/dataServer/workspaces/{workspace_id}/pipelines",
        json=body
    )
    j = r.json()
    pipeline_id = j["id"]
    print(f"[+] Phase-2 pipeline {pipeline_id}")
    return j


def apply_date_conversions(session: MSTRSession, workspace_id: str,
                           pipeline_id: str, column_names: list[str]):
    for col in column_names:
        session._post(
            f"/api/dataServer/workspaces/{workspace_id}/pipelines/{pipeline_id}",
            json={"columns": [{"name": col, "targetDataType": "date",
                               "targetFormat": "yyyy-MM-dd"}]}
        )
        print(f"[+] Date conversion: {col}")


def add_table_to_model(session: MSTRSession, model_id: str,
                       pipeline_json: dict) -> str:
    body = {
        "physicalTable": {
            "type": "pipeline",
            "pipeline": json.dumps(pipeline_json, separators=(",", ":"))
        }
    }
    r = session._post(
        f"/api/model/dataModels/{model_id}/tables?fields=information,attributes,factMetrics,physicalTable,logicalSize,isLogicalSizeLocked,isTrueKey,isPartOfPartition,unMappedColumns,refreshPolicy",
        json=body
    )
    j = r.json()
    table_id = j["information"]["objectId"]
    col_map = {}
    for col in j.get("physicalTable", {}).get("columns", []):
        info = col["information"]
        col_map[info["name"]] = {
            "id": info["objectId"],
            "type": col["dataType"]["type"],
        }
    print(f"[+] Table attached: {table_id} ({len(col_map)} columns)")
    return table_id, col_map


def _build_attr_body(col_name: str, col_id: str, table_id: str, model_id: str,
                     data_type: str, display_format: str | None = None) -> dict:
    """Build a single attribute-creation request object for a batch."""
    if display_format is None:
        display_format = "text" if data_type in ("utf8_char", "string") else "number"
    if display_format == "text":
        semantic = "none"
        geo = "none"
    elif data_type in ("double", "float", "real"):
        semantic = "currency"
        geo = "none"
    elif data_type == "date":
        semantic = "none"
        geo = "none"
    else:
        semantic = "none"
        geo = "none"

    path = f"/model/dataModels/{model_id}/attributes?showExpressionAs=tree&showExpressionAs=tokens&allowLink=true"
    return {
        "method": "POST",
        "path": path,
        "body": {
            "information": {
                "name": col_name.replace("_", " ").title(),
                "description": f"Attribute for {col_name}"
            },
            "forms": [{
                "name": col_name.replace("_", " ").title(),
                "displayFormat": display_format,
                "expressions": [{
                    "expression": {
                        "tree": {"objectId": col_id, "type": "column_reference"}
                    },
                    "tables": [{
                        "objectId": table_id,
                        "subType": "logical_table"
                    }]
                }],
                "geographicalRole": geo,
                "semanticRole": semantic
            }],
            "keyForm": {"name": col_name.replace("_", " ").title()},
            "displays": {
                "reportDisplays": [{"name": col_name.replace("_", " ").title()}],
                "browseDisplays": [{"name": col_name.replace("_", " ").title()}]
            },
            "autoDetectLookupTable": True,
            "attributeLookupTable": {"objectId": table_id, "subType": "logical_table"},
            "alias": col_name,
            "lookupTable": {"objectId": table_id, "subType": "logical_table"}
        }
    }




def create_attributes_batch(session: MSTRSession, model_id: str,
                            col_map: dict, table_id: str) -> list[str]:
    """Batch-create one attribute per column so the model has dimension elements."""
    requests = []
    for col_name, meta in col_map.items():
        col_id = meta["id"]
        data_type = meta["type"]
        attr = _build_attr_body(col_name, col_id, table_id, model_id, data_type)
        requests.append(attr)

    r = session._post(
        f"/api/model/batch?allowPartialSuccess=true&showChanges=true",
        json={"requests": requests}
    )
    j = r.json()
    attr_ids = []
    for resp_item in j.get("responses", []):
        if resp_item.get("status") == 201:
            body = resp_item.get("body", {})
            attr_id = body.get("information", {}).get("objectId")
            if attr_id:
                attr_ids.append(attr_id)
        elif resp_item.get("status") not in (200, 201, 204):
            print(f"[!] Attribute creation failed: {resp_item}")

    print(f"[+] Created {len(attr_ids)} attributes")
    return attr_ids


def create_fact_metrics_batch(session: MSTRSession, model_id: str,
                              col_map: dict, table_id: str) -> dict[str, str]:
    """Create Sum fact-metrics for numeric/decimal/money columns (skip int/bit).
    Returns {metric_name: metric_id}."""
    metric_types = {
        "int64", "integer", "short", "long",
        "double", "float", "real",
        "numeric", "decimal", "money",
    }

    # 1. Create factMetrics
    create_requests = []
    name_to_col = {}
    for col_name, meta in col_map.items():
        col_lower = col_name.lower()
        dt = meta["type"].lower()
        # Skip boolean-like flag columns — these stay as attributes only
        if "flag" in col_lower:
            continue
        # Skip ID-like columns — these stay as attributes only
        if " id" in col_lower or " id " in col_lower:
            continue
        if dt not in metric_types:
            continue
        safe_name = col_name.replace("_", " ").title() + " Sum"
        name_to_col[safe_name] = col_name
        create_requests.append({
            "method": "POST",
            "path": f"/model/dataModels/{model_id}/factMetrics?showExpressionAs=tree&showExpressionAs=tokens",
            "body": {
                "information": {"name": safe_name},
                "fact": {
                    "expressions": [{
                        "expression": {
                            "tree": {
                                "type": "column_reference",
                                "columnName": col_name,
                                "objectId": meta["id"]
                            }
                        },
                        "tables": [{
                            "objectId": table_id,
                            "subType": "logical_table"
                        }]
                    }]
                }
            }
        })

    if not create_requests:
        print("[+] No numeric/decimal columns — no metrics created")
        return {}

    r = session._post(
        "/api/model/batch?allowPartialSuccess=true&showChanges=true",
        json={"requests": create_requests}
    )
    j = r.json()

    metric_map = {}
    patch_requests = []
    for i, resp_item in enumerate(j.get("responses", [])):
        if resp_item.get("status") == 201:
            body = resp_item.get("body", {})
            metric_id = body.get("information", {}).get("objectId")
            metric_name = body.get("information", {}).get("name", "")
            if metric_id:
                metric_map[metric_name] = metric_id
                patch_requests.append({
                    "method": "PATCH",
                    "path": f"/model/dataModels/{model_id}/factMetrics/{metric_id}?showExpressionAs=tree&showExpressionAs=tokens",
                    "body": {"function": "sum"}
                })
        elif resp_item.get("status") not in (200, 201, 204):
            print(f"[!] FactMetric creation failed: {resp_item}")

    if not metric_map:
        print("[+] No metrics were created")
        return {}

    print(f"[+] Created {len(metric_map)} factMetrics, setting Sum function...")

    # 2. Patch each to set function: "sum"
    r2 = session._post(
        "/api/model/batch?allowPartialSuccess=true&showChanges=true",
        json={"requests": patch_requests}
    )
    j2 = r2.json()
    success = 0
    for resp_item in j2.get("responses", []):
        if resp_item.get("status") in (200, 204):
            success += 1
        else:
            print(f"[!] Sum patch failed: {resp_item}")

    print(f"[+] Set Sum function on {success}/{len(metric_map)} metrics")
    return metric_map


def create_calculated_metrics(session: MSTRSession, model_id: str,
                               existing_metrics: dict[str, str], changeset_id: str) -> list[str]:
    """Use AIService to recommend & create calculated metrics.
    Accept every suggestion the AI returns a formula for.
    Try to build it; if any step fails, delete the shell."""

    # 1. Fetch real model context for better AI suggestions
    print("[ ] Fetching model context...")
    try:
        attr_ctx = _fetch_model_attributes(session, model_id)
        metric_ctx = _fetch_model_metrics(session, model_id)
    except Exception as e:
        print(f"[!] Failed to fetch model context: {e}")
        attr_ctx, metric_ctx = [], []

    # 2. Fetch AI suggestions with context
    print("[ ] Asking AIService for metric suggestions...")
    suggestions = session.aiservice_recommend_metrics(model_id, attr_ctx, metric_ctx)
    if not suggestions:
        print("[+] No AI suggestions returned")
        return []

    created_ids = []
    skipped = []
    for sug in suggestions:
        name = sug.get("name", "")
        query = sug.get("query", "")
        if not query:
            skipped.append((name, "no query"))
            continue

        # Ask AIService for the formula
        print(f"[ ] AI: '{name}' → {query[:60]}...")
        ai_metric = session.aiservice_create_metric(model_id, query)
        if not ai_metric:
            skipped.append((name, "no formula from AI"))
            continue

        formula = ai_metric.get("formula", "")
        print(f"    formula: {formula[:100]}")

        # Reject non-metric responses
        if not formula or ai_metric.get("type") == "message":
            skipped.append((name, "unbuildable by AI"))
            continue

        # Extract leaf names from formula
        leafs = re.findall(r"\[([^\]]+)\]", formula)
        safe_leafs = [l for l in leafs if l.strip()]

        # Resolve IDs via a closure that looks at metrics first, then attributes
        def _resolve(name: str) -> str | None:
            mid = _resolve_metric_id(name, existing_metrics)
            if mid:
                return mid
            for key, aid in existing_metrics.items():
                if key == name + " Sum":
                    return aid
            return None

        resolved = {}
        for l in safe_leafs:
            rid = _resolve(l)
            if rid:
                resolved[l] = rid

        if safe_leafs and len(resolved) != len(safe_leafs):
            missing = [l for l in safe_leafs if l not in resolved]
            skipped.append((name, f"missing refs: {missing}"))
            continue

        # Build token tree
        tokens = _formula_to_tokens(formula, _resolve)
        if not tokens:
            skipped.append((name, "tokenization failed"))
            continue

        # Create metric
        shell_id = None
        try:
            shell_id = _create_metric_shell_raw(session, model_id, changeset_id)
            valid_expr = _validate_expression_raw(session, model_id, shell_id, tokens, changeset_id)
            _update_metric_raw(session, model_id, shell_id, name, valid_expr, changeset_id)
            created_ids.append(shell_id)
            print(f"[+] Created calculated metric: {name} ({shell_id})")
        except Exception as e:
            if shell_id:
                try:
                    session._delete(f"/api/model/dataModels/{model_id}/metrics/{shell_id}")
                    print(f"[!] Deleted failed metric shell: {shell_id}")
                except Exception as del_err:
                    print(f"[!] Could not delete failed shell {shell_id}: {del_err}")
            skipped.append((name, str(e)))
            print(f"[!] Failed to create '{name}': {e}")

    if skipped:
        print(f"\n[+] Skipped {len(skipped)} suggestions:")
        for name, reason in skipped:
            print(f"   - {name}: {reason}")

    return created_ids


def _resolve_metric_id(name: str, metric_map: dict[str, str]) -> str | None:
    """Resolve a metric name to its ID, handling 'Sum' suffix mismatches."""
    if name in metric_map:
        return metric_map[name]
    # Try base name vs "Name Sum"
    for key in metric_map:
        if key == name + " Sum":
            return metric_map[key]
        if key.replace(" Sum", "") == name:
            return metric_map[key]
    return None


# ── Function DID map ─────────────────────────────────────────────
_FUNCTION_DIDS = {
    "Sum":    "8107C31BDD9911D3B98100C04F2233EA",
    "Count":  "8107C31CDD9911D3B98100C04F2233EA",
    "Avg":    "8107C31DDD9911D3B98100C04F2233EA",
    "Average":"375104850C3E45EA889D9F5FEB37D317",
    "Median": "8107C32DDD9911D3B98100C04F2233EA",
    "Min":    "8107C31EDD9911D3B98100C04F2233EA",
    "Max":    "8107C31FDD9911D3B98100C04F2233EA",
    "RunningSum":  "8107C328DD9911D3B98100C04F2233EA",
    "RunningAvg":  "8107C329DD9911D3B98100C04F2233EA",
    "MovingAvg":   "8107C32ADD9911D3B98100C04F2233EA",
}

_OPERATOR_DIDS = {
    "-": "8107C311DD9911D3B98100C04F2233EA",  # Minus
    "+": "0DFE6FB4356F4FA2A0C1C10D0B415355",  # Add
    "*": "9AD889E56E4D4E56A127207B4AF77166",  # Multiply
    "/": "F262C825DDA611D3B98100C04F2233EA",  # Quotient
}


def _formula_to_tokens(formula: str, resolve_fn) -> list[dict]:
    """Convert an AIService formula string into MSTR token list.

    Resolve helper:  resolve_fn(metric_name: str) -> object_id  | None
    """
    tokens: list[dict] = []

    # ── 1. Aggregation over a metric:  Func([Name]){~+}  or  Func([Name]){~}  ──
    aggr_re = re.compile(
        r"^(Sum|Count|Avg|Average|Median|Min|Max|RunningSum|RunningAvg|MovingAvg)"
        r"\s*\(\s*\[([^\]]+)\]\s*\)(?:\{~\+?\})?\s*$"
    )
    m = aggr_re.match(formula)
    if m:
        func_name = m.group(1)
        name = m.group(2)
        mid = resolve_fn(name)
        if not mid:
            return []
        did = _FUNCTION_DIDS[func_name]
        tokens.append({"value": "&", "type": "character", "level": "resolved", "state": "initial"})
        tokens.append({
            "value": func_name,
            "type": "function",
            "level": "client",
            "state": "initial",
            "target": {"objectId": did, "subType": "function", "name": func_name},
        })
        tokens.append({"value": "(", "type": "character", "level": "resolved", "state": "initial"})
        tokens.append({
            "value": f"[{name}]",
            "type": "object_reference",
            "level": "client",
            "state": "initial",
            "target": {"objectId": mid, "subType": "fact_metric", "name": name},
        })
        tokens.append({"value": ")", "type": "character", "level": "resolved", "state": "initial"})
        return tokens

    # ── 2. Percentile / PercentRank  Percentile([Name], 0.9){~+}    ──
    pct_re = re.compile(
        r"^(Percentile|PercentRank)\s*\(\s*\[([^\]]+)\]\s*,\s*([0-9.]+)\s*\)(?:\{~\+?\})?\s*$"
    )
    m = pct_re.match(formula)
    if m:
        func_name = m.group(1)
        name = m.group(2)
        val = m.group(3)
        mid = resolve_fn(name)
        if not mid:
            return []
        # Percentile DID from catalog
        did = "8107C335DD9911D3B98100C04F2233EA" if func_name == "Percentile" else "8107C336DD9911D3B98100C04F2233EA"
        tokens.append({"value": "&", "type": "character", "level": "resolved", "state": "initial"})
        tokens.append({
            "value": func_name,
            "type": "function",
            "level": "client",
            "state": "initial",
            "target": {"objectId": did, "subType": "function", "name": func_name},
        })
        tokens.append({"value": "(", "type": "character", "level": "resolved", "state": "initial"})
        tokens.append({
            "value": f"[{name}]",
            "type": "object_reference",
            "level": "client",
            "state": "initial",
            "target": {"objectId": mid, "subType": "fact_metric", "name": name},
        })
        tokens.append({"value": ",", "type": "character", "level": "resolved", "state": "initial"})
        tokens.append({"value": val, "type": "constant", "level": "resolved", "state": "initial"})
        tokens.append({"value": ")", "type": "character", "level": "resolved", "state": "initial"})
        return tokens

    # ── 3. Two-source calc: [Left] op [Right]  ──
    binary_re = re.compile(
        r"^\s*\[([^\]]+)\]\s*([\-+*/])\s*\[([^\]]+)\]\s*$"
    )
    m = binary_re.match(formula)
    if m:
        left = m.group(1).strip()
        op = m.group(2)
        right = m.group(3).strip()
        lid = resolve_fn(left)
        rid = resolve_fn(right)
        if not lid or not rid:
            return []
        op_did = _OPERATOR_DIDS.get(op)
        op_token = {
            "value": op,
            "type": "character",
            "level": "resolved",
            "state": "initial",
        }
        if op_did:
            op_token["target"] = {"objectId": op_did, "subType": "function", "name": op}
        tokens = [
            {"value": "&", "type": "character", "level": "resolved", "state": "initial"},
            {
                "value": f"[{left}]",
                "type": "object_reference",
                "level": "client",
                "state": "initial",
                "target": {"objectId": lid, "subType": "fact_metric", "name": left},
            },
            op_token,
            {
                "value": f"[{right}]",
                "type": "object_reference",
                "level": "client",
                "state": "initial",
                "target": {"objectId": rid, "subType": "fact_metric", "name": right},
            },
        ]
        return tokens

    # ── 4. Ratio / nested:  Sum([A]) / Sum([B])   (two aggs + operator) ──
    ratio_re = re.compile(
        r"^\s*(Sum|Count|Avg|Average|Median|Min|Max)\s*\(\s*\[([^\]]+)\]\s*\)\s*([\-+*/])\s*(Sum|Count|Avg|Average|Median|Min|Max)\s*\(\s*\[([^\]]+)\]\s*\)\s*$"
    )
    m = ratio_re.match(formula)
    if m:
        fn1, name1, op, fn2, name2 = m.groups()
        mid1 = resolve_fn(name1)
        mid2 = resolve_fn(name2)
        if not mid1 or not mid2:
            return []
        did1 = _FUNCTION_DIDS.get(fn1)
        did2 = _FUNCTION_DIDS.get(fn2)
        op_did = _OPERATOR_DIDS.get(op)
        if not did1 or not did2:
            return []

        def _agg_token(fn, did, name, mid):
            return [
                {"value": fn, "type": "function", "level": "client", "state": "initial",
                 "target": {"objectId": did, "subType": "function", "name": fn}},
                {"value": "(", "type": "character", "level": "resolved", "state": "initial"},
                {"value": f"[{name}]", "type": "object_reference", "level": "client", "state": "initial",
                 "target": {"objectId": mid, "subType": "fact_metric", "name": name}},
                {"value": ")", "type": "character", "level": "resolved", "state": "initial"},
            ]

        op_token = {"value": op, "type": "character", "level": "resolved", "state": "initial"}
        if op_did:
            op_token["target"] = {"objectId": op_did, "subType": "function", "name": op}

        tokens = [{"value": "&", "type": "character", "level": "resolved", "state": "initial"}]
        tokens.extend(_agg_token(fn1, did1, name1, mid1))
        tokens.append(op_token)
        tokens.extend(_agg_token(fn2, did2, name2, mid2))
        return tokens

    return []


def _create_metric_shell_raw(session: MSTRSession, model_id: str, changeset_id: str | None) -> str:
    """Create a metric shell via raw requests (mstrio doesn't handle /metrics)."""
    s = session._ensure_raw_session()
    headers = session._aiservice_headers()
    if changeset_id:
        headers["x-mstr-ms-changeset"] = changeset_id
    r = s.post(
        f"{session._base_url}/api/model/dataModels/{model_id}/metrics?showAdvancedProperties=true",
        headers=headers,
        json={"information": {"subType": "metric"}},
        timeout=30,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Metric shell creation failed: {r.text[:300]}")
    return r.json()["information"]["objectId"]


def _validate_expression_raw(session: MSTRSession, model_id: str, metric_id: str,
                             tokens: list, changeset_id: str | None) -> dict:
    s = session._ensure_raw_session()
    headers = session._aiservice_headers()
    if changeset_id:
        headers["x-mstr-ms-changeset"] = changeset_id
    r = s.post(
        f"{session._base_url}/api/model/dataModels/{model_id}/metrics/{metric_id}/expression/validate",
        headers=headers,
        json={"expression": {"tokens": tokens}},
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Expression validation failed: {r.text[:300]}")
    return r.json()["expression"]


def _update_metric_raw(session: MSTRSession, model_id: str, metric_id: str,
                       name: str, expression: dict, changeset_id: str | None):
    s = session._ensure_raw_session()
    headers = session._aiservice_headers()
    if changeset_id:
        headers["x-mstr-ms-changeset"] = changeset_id
    r = s.put(
        f"{session._base_url}/api/model/dataModels/{model_id}/metrics/{metric_id}?showExpressionAs=tree&showExpressionAs=tokens&showAdvancedProperties=true&clearUnusedEmbeddedObjects=true",
        headers=headers,
        json={"information": {"name": name}, "expression": expression},
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Metric update failed: {r.text[:300]}")


def _fetch_model_attributes(session: MSTRSession, model_id: str) -> list[dict]:
    """Fetch real attribute context for AIService (full objects)."""
    r = session._get(
        f"/api/model/dataModels/{model_id}/attributes?showExpressionAs=tree&showExpressionAs=tokens"
    )
    return r.json().get("attributes", [])


def _fetch_model_metrics(session: MSTRSession, model_id: str) -> list[dict]:
    """Fetch real metric context for AIService (full objects)."""
    r = session._get(
        f"/api/model/dataModels/{model_id}/factMetrics?showExpressionAs=tree&showExpressionAs=tokens"
    )
    return r.json().get("factMetrics", [])


def apply_column_mapping(session: MSTRSession, workspace_id: str,
                         pipeline_id: str, column_pairs: list[list[int]]):
    session._post(
        f"/api/dataServer/workspaces/{workspace_id}/pipelines/{pipeline_id}/relationships",
        json={"columnPairs": column_pairs}
    )
    print(f"[+] Column mapping applied ({len(column_pairs)} pairs)")


def update_model_info(session: MSTRSession, model_id: str,
                      name: str | None = None,
                      description: str | None = None,
                      folder_id: str | None = None):
    patch = {"information": {}}
    if name:
        patch["information"]["name"] = name
    if description:
        patch["information"]["description"] = description
    if folder_id:
        patch["information"]["destinationFolderId"] = folder_id
    if not patch["information"]:
        return
    session._patch(
        f"/api/model/dataModels/{model_id}?showExecutiveSummary=true",
        json=patch
    )
    print(f"[+] Model updated")


def commit_model(session: MSTRSession, changeset_id: str, model_id: str,
                 folder_id: str | None = None):
    if folder_id:
        session._patch(f"/api/model/dataModels/{model_id}?showExecutiveSummary=true",
                       json={"information": {"destinationFolderId": folder_id}})
        print("[+] Model folder set")

    session._post(f"/api/model/changesets/{changeset_id}/commit", json={})
    print("[+] Commit in progress...")
    session.poll_changeset(changeset_id)
    print("[+] Model saved")


# ------------------------------------------------------------------
# Multi-source pipeline helpers
# ------------------------------------------------------------------

@dataclass
class TableSource:
    datasource_name: str
    schema: str
    table: str
    date_columns: list[str]


def _load_sources(args) -> list[TableSource]:
    """Resolve table sources from CLI args or JSON config."""
    if args.source_config:
        with open(args.source_config, "r") as f:
            cfg = json.load(f)
        sources = []
        for item in cfg:
            sources.append(TableSource(
                datasource_name=item["datasource"],
                schema=item.get("schema", "public"),
                table=item["table"],
                date_columns=[c.strip() for c in item.get("date_columns", "").split(",") if c.strip()],
            ))
        return sources
    # Single-source backward-compatible mode
    return [TableSource(
        datasource_name=args.datasource,
        schema=args.schema,
        table=args.table,
        date_columns=[c.strip() for c in (args.date_columns or "").split(",") if c.strip()],
    )]


def multi_pipeline_phase1(session: MSTRSession, workspace_id: str,
                          sources: list[TableSource]) -> list[dict]:
    """Phase 1: Create an empty pipeline for each source table."""
    ready_sources = []
    for src in sources:
        ds = session.find_datasource(src.datasource_name)
        datasource_id = ds["id"]
        datasource_name = ds["name"]
        print(f"\n[+] Source: {src.schema}.{src.table} @ {datasource_name}")
        p1_result = create_pipeline_phase1(
            session, workspace_id, datasource_id, datasource_name,
            src.schema, src.table
        )
        if "ready" in p1_result:
            p1_ready = p1_result["ready"]
        else:
            p1_ready = poll_pipeline(session, workspace_id, p1_result["poll_id"])
        ready_sources.append({
            "source": src,
            "pipeline": p1_ready,
            "datasource_id": datasource_id,
            "datasource_name": datasource_name,
        })
    return ready_sources


def multi_pipeline_phase2(session: MSTRSession, workspace_id: str,
                          ready_sources: list[dict]) -> list[dict]:
    """Phase 2: Rebuild each source into a clean pipeline."""
    p2_results = []
    for entry in ready_sources:
        p2 = create_pipeline_phase2(session, workspace_id, entry["pipeline"])
        print(f"[+] Phase-2 pipeline {p2['id']} for {entry['source'].table}")
        # Apply date conversions per source
        if entry["source"].date_columns:
            apply_date_conversions(session, workspace_id, p2["id"], entry["source"].date_columns)
        p2_results.append({
            **entry,
            "phase2": p2,
            "phase2_id": p2["id"],
        })
    return p2_results


def add_all_tables_to_model(session: MSTRSession, model_id: str,
                            p2_results: list[dict]) -> list[tuple[str, str, dict]]:
    """Attach each Phase-2 pipeline as a logical table."""
    tables = []
    for entry in p2_results:
        table_id, col_map = add_table_to_model(session, model_id, entry["phase2"])
        tables.append((entry["source"].table, table_id, col_map))
        entry["table_id"] = table_id
        entry["col_map"] = col_map
    return tables


def create_all_attributes(session: MSTRSession, model_id: str,
                          tables: list[tuple[str, str, dict]]) -> list[str]:
    """Batch-create attributes for every column across all tables."""
    all_attr_ids = []
    for table_name, table_id, col_map in tables:
        attr_ids = create_attributes_batch(session, model_id, col_map, table_id)
        all_attr_ids.extend(attr_ids)
    return all_attr_ids


def create_all_fact_metrics(session: MSTRSession, model_id: str,
                            tables: list[tuple[str, str, dict]]) -> dict[str, str]:
    """Create Sum fact-metrics for every numeric column across all tables.
    Returns a merged {metric_name: metric_id} map."""
    merged = {}
    for table_name, table_id, col_map in tables:
        metric_map = create_fact_metrics_batch(session, model_id, col_map, table_id)
        merged.update(metric_map)
    return merged


def apply_all_column_mappings(session: MSTRSession, workspace_id: str,
                              p2_results: list[dict]):
    """Auto-map columns for each Phase-2 pipeline."""
    for entry in p2_results:
        cols = entry["phase2"].get("rootTable", {}).get("children", [{}])[0].get("columns", [])
        pairs = [[i, 0] for i in range(1, len(cols))]
        if pairs:
            apply_column_mapping(session, workspace_id, entry["phase2_id"], pairs)



def _assign_folder_post_commit(session: MSTRSession, model_id: str, folder_id: str):
    """MSTR silently drops destinationFolderId during full commit;
    create a new changeset, patch it back, and commit again."""
    old_cs = session._changeset
    session._changeset = None  # clear committed changeset so create_changeset works
    cs2 = None
    try:
        cs2 = session.create_changeset()
        session._patch(
            f"/api/model/dataModels/{model_id}?showExecutiveSummary=true",
            json={"information": {"destinationFolderId": folder_id}}
        )
        print(f"[+] Folder patch applied")
        session._post(f"/api/model/changesets/{cs2}/commit", json={})
        print("[+] Folder commit in progress...")
        session.poll_changeset(cs2)
        print("[+] Folder assigned successfully")
    finally:
        if cs2:
            session._delete(f"/api/model/changesets/{cs2}")
            session._changeset = old_cs
            print(f"[+] Cleanup changeset {cs2}")


# ------------------------------------------------------------------
# main orchestrator
# ------------------------------------------------------------------

def run(args):
    _env = _load_env()
    base_url = _env.get("MSTR_BASE_URL", "https://studio.strategy.com/MicroStrategyLibrary")
    username = _env["MSTR_USERNAME"]
    password = _env["MSTR_PASSWORD"]
    return _run(args, base_url, username, password)


def _run(args, base_url, username, password):
    session = MSTRSession(
        base_url=base_url,
        username=username,
        password=password,
        project_name=args.project,
    )

    # 1. Resolve table sources
    sources = _load_sources(args)
    print(f"[+] Building model from {len(sources)} source(s)")
    for s in sources:
        print(f"   - {s.schema}.{s.table} @ {s.datasource_name}")

    # 2. create changeset + model (set folder at creation so it's visible)
    folder_id = args.folder_id or "0268E42CB84F8CCCE28909A111004E8F"
    cs = session.create_changeset()
    model_id, _ = session.create_model(args.model_name, folder_id=folder_id)

    # 3. create workspace
    ws = session.create_workspace()

    try:
        # 4-6. Build pipelines for every source
        p1_ready = multi_pipeline_phase1(session, ws, sources)
        p2_results = multi_pipeline_phase2(session, ws, p1_ready)

        # 7. Attach all tables
        tables = add_all_tables_to_model(session, model_id, p2_results)

        # 8. Create attributes for all columns across all tables
        attr_ids = create_all_attributes(session, model_id, tables)

        # 9. Create fact metrics for all numeric columns across all tables
        metric_map = create_all_fact_metrics(session, model_id, tables)

        # 10. AI calculated metrics
        calc_ids = create_calculated_metrics(session, model_id, metric_map, cs)
        metric_ids = list(metric_map.values()) + calc_ids

        # 11. Auto column mappings per pipeline
        apply_all_column_mappings(session, ws, p2_results)

        # 12. Model metadata
        update_model_info(session, model_id,
                          name=args.model_name,
                          description=args.description,
                          folder_id=args.folder_id)

        # 13. Commit
        print("[+] Committing...")
        session._post(f"/api/model/changesets/{cs}/commit", json={})
        print("[+] Commit in progress...")
        session.poll_changeset(cs)
        print("[+] Model saved")

        # MSTR silently drops destinationFolderId during full commit;
        # reattach it via a second changeset+commit.
        folder_id = args.folder_id or "0268E42CB84F8CCCE28909A111004E8F"
        _assign_folder_post_commit(session, model_id, folder_id)

        print(f"\n✅ Model '{args.model_name}' created and saved")
        print(f"   Model ID:     {model_id}")
        for tname, tid, _ in tables:
            print(f"   Table:        {tname} ({tid})")
        print(f"   Attributes:   {len(attr_ids)}")
        print(f"   Metrics:      {len(metric_ids)}")
        for entry in p2_results:
            print(f"   Pipeline ID:  {entry['phase2_id']} ({entry['source'].table})")
        print(f"   Workspace ID: {ws}")

    finally:
        if not args.keep_workspace:
            session.delete_workspace(ws)
        session._delete(f"/api/model/changesets/{cs}")
        print(f"[+] Changeset {cs} deleted")

    return model_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a Mosaic data model from one or more DB tables")
    parser.add_argument("--datasource", help="Datasource name (single-source mode)")
    parser.add_argument("--table", help="Table name (single-source mode)")
    parser.add_argument("--schema", default="public", help="Schema name (single-source, default: public)")
    parser.add_argument("--source-config", help="JSON file with list of source table definitions (multi-source mode)")
    parser.add_argument("--model-name", required=True, help="Name for the new Mosaic model")
    parser.add_argument("--project", default="Shared Studio", help="MSTR project name")
    parser.add_argument("--description", default="", help="Model description")
    parser.add_argument("--folder-id", default=None, help="Destination folder ID for the model")
    parser.add_argument("--date-columns", default="", help="Comma-separated column names to convert to date (single-source mode)")
    parser.add_argument("--keep-workspace", action="store_true",
                        help="Do not delete the workspace after committing")

    _args = parser.parse_args()
    # Validate mode: either --source-config or both --datasource + --table
    if _args.source_config:
        if _args.datasource or _args.table:
            print("[!] Warning: --source-config provided; ignoring --datasource / --table")
    else:
        if not _args.datasource or not _args.table:
            parser.error("Either --source-config or both --datasource and --table are required")
    run(_args)
