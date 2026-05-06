"""
Commands for interacting with MicroStrategy Mosaic Models.
This file contains the logic ported from the original script `01_create_mosaic_model.py`.
"""

import json
import time
import base64
import uuid
import re
from dataclasses import dataclass

from mstr_cli.core.auth import MstrClient
from mstr_cli.core.utils import (Changeset, find_metric_by_name, find_attribute_by_name, get_model_tables, get_model_table, poll_changeset_commit)


# ------------------------------------------------------------------
# tableId encoder
# ------------------------------------------------------------------

def encode_table_id(datasource_id: str, schema: str, table_name: str) -> str:
    """Encodes datasource, schema, and table name into a Base64 ID for the API."""
    raw = f"{datasource_id}\x00{schema}\x00{table_name}\x00"
    b = raw.encode("utf-16le")
    return base64.b64encode(b).decode("ascii")


# ------------------------------------------------------------------
# Pipeline helpers (Ported from 01_create_mosaic_model.py)
# ------------------------------------------------------------------

def _find_datasource(client: MstrClient, name: str):
    """Finds a datasource by name."""
    r = client.get("/api/datasources")
    dss = r.json().get("datasources", [])
    if name:
        filtered_dss = [d for d in dss if name.lower() in d.get("name", "").lower()]
        if not filtered_dss:
            raise RuntimeError(f"Datasource '{name}' not found")
        return filtered_dss[0]
    raise RuntimeError("Datasource name not provided")


def create_pipeline_phase1(client: MstrClient, workspace_id: str,
                           datasource_id: str, datasource_name: str,
                           schema: str, table_name: str) -> dict:
    """Creates the first phase of the data import pipeline."""
    dbms_info = _find_datasource(client, datasource_name)
    db_name = dbms_info.get("database", {}).get("type", "postgre_sql")
    db_type_map = {
        "postgre_sql": {"type": "single_table", "dbType": 1800, "dbVersion": 149},
        "sql_server":  {"type": "single_table", "dbType": 26,   "dbVersion": 5},
        "mysql":       {"type": "single_table", "dbType": 15,   "dbVersion": 10},
    }
    db_cfg = db_type_map.get(db_name, db_type_map["postgre_sql"])

    table_id_b64 = encode_table_id(datasource_id, schema, table_name)

    # Step 1: create empty pipeline
    r1 = client.post(f"/api/dataServer/workspaces/{workspace_id}/pipelines", json={})
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
    r2 = client.post(
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


def poll_pipeline(client: MstrClient, workspace_id: str,
                  pipeline_id: str, timeout=120):
    """Polls the pipeline until it is ready."""
    url = f"/api/dataServer/workspaces/{workspace_id}/pipelines/{pipeline_id}"
    print(f"[ ] Polling: {url}")
    t0 = time.time()
    dots = 0
    while time.time() - t0 < timeout:
        r = client.get(url)
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


def create_pipeline_phase2(client: MstrClient, workspace_id: str,
                           source_pipeline: dict) -> dict:
    """Creates the second phase of the data import pipeline."""
    src = source_pipeline
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

    r = client.post(
        f"/api/dataServer/workspaces/{workspace_id}/pipelines",
        json=body
    )
    j = r.json()
    pipeline_id = j["id"]
    print(f"[+] Phase-2 pipeline {pipeline_id}")
    return j


def apply_date_conversions(client: MstrClient, workspace_id: str,
                           pipeline_id: str, column_names: list[str]):
    """Applies date conversion to specified columns."""
    for col in column_names:
        client.post(
            f"/api/dataServer/workspaces/{workspace_id}/pipelines/{pipeline_id}",
            json={"columns": [{"name": col, "targetDataType": "date",
                               "targetFormat": "yyyy-MM-dd"}]}
        )
        print(f"[+] Date conversion: {col}")


def add_table_to_model(client: MstrClient, model_id: str,
                       pipeline_json: dict) -> tuple[str, dict]:
    """Adds a physical table from a pipeline to the model."""
    body = {
        "physicalTable": {
            "type": "pipeline",
            "pipeline": json.dumps(pipeline_json, separators=(",", ":"))
        }
    }
    r = client.post(
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


def create_attributes_batch(client: MstrClient, model_id: str,
                            col_map: dict, table_id: str) -> list[str]:
    """Batch-create one attribute per column."""
    requests = []
    for col_name, meta in col_map.items():
        col_id = meta["id"]
        data_type = meta["type"]
        attr = _build_attr_body(col_name, col_id, table_id, model_id, data_type)
        requests.append(attr)

    r = client.post(
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


def create_fact_metrics_batch(client: MstrClient, model_id: str,
                              col_map: dict, table_id: str) -> dict[str, str]:
    """Create Sum fact-metrics for numeric/decimal/money columns."""
    metric_types = {
        "int64", "integer", "short", "long",
        "double", "float", "real",
        "numeric", "decimal", "money",
    }

    create_requests = []
    name_to_col = {}
    for col_name, meta in col_map.items():
        col_lower = col_name.lower()
        dt = meta["type"].lower()
        if "flag" in col_lower or " id" in col_lower or " id " in col_lower:
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
                        "tables": [{"objectId": table_id, "subType": "logical_table"}]
                    }]
                }
            }
        })

    if not create_requests:
        print("[+] No numeric/decimal columns — no metrics created")
        return {}

    r = client.post(
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

    r2 = client.post(
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


def _aiservice_recommend_metrics(client: MstrClient, model_id: str,
                                 attributes: list[dict] | None = None,
                                 metrics: list[dict] | None = None) -> list[dict]:
    """Fetch AI-suggested metrics."""
    s = client._get_raw_session()
    context = {"attributes": attributes or [], "metrics": metrics or []}
    body = {"payload": {"userMessage": "suggest new metric", "context": context}}
    r = s.post(
        f"{client._base_url}/api/aiservice/model/objects/metrics/recommendations",
        headers=client._get_headers(), json=body, timeout=120,
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


def _aiservice_create_metric(client: MstrClient, model_id: str, metric_query: str) -> dict | None:
    """Ask the AI to generate a metric definition from a query string."""
    s = client._get_raw_session()
    body = {"payload": {"userMessage": metric_query, "context": {"attributes": [], "metrics": []}}}
    r = s.post(
        f"{client._base_url}/api/aiservice/model/objects/metrics",
        headers=client._get_headers(), json=body, timeout=120,
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


def create_calculated_metrics(client: MstrClient, model_id: str,
                              existing_metrics: dict[str, str], changeset_id: str) -> list[str]:
    """Use AIService to recommend & create calculated metrics."""
    print("[ ] Fetching model context...")
    try:
        attr_ctx = _fetch_model_attributes(client, model_id)
        metric_ctx = _fetch_model_metrics(client, model_id)
    except Exception as e:
        print(f"[!] Failed to fetch model context: {e}")
        attr_ctx, metric_ctx = [], []

    print("[ ] Asking AIService for metric suggestions...")
    suggestions = _aiservice_recommend_metrics(client, model_id, attr_ctx, metric_ctx)
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

        print(f"[ ] AI: '{name}' → {query[:60]}...")
        ai_metric = _aiservice_create_metric(client, model_id, query)
        if not ai_metric:
            skipped.append((name, "no formula from AI"))
            continue

        formula = ai_metric.get("formula", "")
        print(f"    formula: {formula[:100]}")

        if not formula or ai_metric.get("type") == "message":
            skipped.append((name, "unbuildable by AI"))
            continue

        leafs = re.findall(r"\[([^\]]+)\]", formula)
        safe_leafs = [l for l in leafs if l.strip()]

        def _resolve(name: str) -> str | None:
            mid = _resolve_metric_id(name, existing_metrics)
            if mid: return mid
            for key, aid in existing_metrics.items():
                if key == name + " Sum": return aid
            return None

        resolved = {l: _resolve(l) for l in safe_leafs if _resolve(l)}

        if safe_leafs and len(resolved) != len(safe_leafs):
            missing = [l for l in safe_leafs if l not in resolved]
            skipped.append((name, f"missing refs: {missing}"))
            continue

        tokens = _formula_to_tokens(formula, _resolve)
        if not tokens:
            skipped.append((name, "tokenization failed"))
            continue

        shell_id = None
        try:
            shell_id = _create_metric_shell_raw(client, model_id, changeset_id)
            valid_expr = _validate_expression_raw(client, model_id, shell_id, tokens, changeset_id)
            _update_metric_raw(client, model_id, shell_id, name, valid_expr, changeset_id)
            created_ids.append(shell_id)
            print(f"[+] Created calculated metric: {name} ({shell_id})")
        except Exception as e:
            if shell_id:
                try:
                    client.delete(f"/api/model/dataModels/{model_id}/metrics/{shell_id}")
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
    if name in metric_map: return metric_map[name]
    for key in metric_map:
        if key == name + " Sum": return metric_map[key]
        if key.replace(" Sum", "") == name: return metric_map[key]
    return None


_FUNCTION_DIDS = {
    "Sum": "8107C31BDD9911D3B98100C04F2233EA", "Count": "8107C31CDD9911D3B98100C04F2233EA",
    "Avg": "8107C31DDD9911D3B98100C04F2233EA", "Average": "375104850C3E45EA889D9F5FEB37D317",
    "Median": "8107C32DDD9911D3B98100C04F2233EA", "Min": "8107C31EDD9911D3B98100C04F2233EA",
    "Max": "8107C31FDD9911D3B98100C04F2233EA", "RunningSum": "8107C328DD9911D3B98100C04F2233EA",
    "RunningAvg": "8107C329DD9911D3B98100C04F2233EA", "MovingAvg": "8107C32ADD9911D3B98100C04F2233EA",
}

_OPERATOR_DIDS = {
    "-": "8107C311DD9911D3B98100C04F2233EA", "+": "0DFE6FB4356F4FA2A0C1C10D0B415355",
    "*": "9AD889E56E4D4E56A127207B4AF77166", "/": "F262C825DDA611D3B98100C04F2233EA",
}


def _formula_to_tokens(formula: str, resolve_fn) -> list[dict]:
    """Convert an AIService formula string into MSTR token list."""
    tokens: list[dict] = []
    aggr_re = re.compile(r"^(Sum|Count|Avg|Average|Median|Min|Max|RunningSum|RunningAvg|MovingAvg)\s*\(\s*\[([^\]]+)\]\s*\)(?:\{~\+?\})?\s*$")
    m = aggr_re.match(formula)
    if m:
        func_name, name = m.groups()
        mid = resolve_fn(name)
        if not mid: return []
        did = _FUNCTION_DIDS[func_name]
        return [
            {"value": "&", "type": "character", "level": "resolved", "state": "initial"},
            {"value": func_name, "type": "function", "level": "client", "state": "initial", "target": {"objectId": did, "subType": "function", "name": func_name}},
            {"value": "(", "type": "character", "level": "resolved", "state": "initial"},
            {"value": f"[{name}]", "type": "object_reference", "level": "client", "state": "initial", "target": {"objectId": mid, "subType": "fact_metric", "name": name}},
            {"value": ")", "type": "character", "level": "resolved", "state": "initial"},
        ]

    pct_re = re.compile(r"^(Percentile|PercentRank)\s*\(\s*\[([^\]]+)\]\s*,\s*([0-9.]+)\s*\)(?:\{~\+?\})?\s*$")
    m = pct_re.match(formula)
    if m:
        func_name, name, val = m.groups()
        mid = resolve_fn(name)
        if not mid: return []
        did = "8107C335DD9911D3B98100C04F2233EA" if func_name == "Percentile" else "8107C336DD9911D3B98100C04F2233EA"
        return [
            {"value": "&", "type": "character", "level": "resolved", "state": "initial"},
            {"value": func_name, "type": "function", "level": "client", "state": "initial", "target": {"objectId": did, "subType": "function", "name": func_name}},
            {"value": "(", "type": "character", "level": "resolved", "state": "initial"},
            {"value": f"[{name}]", "type": "object_reference", "level": "client", "state": "initial", "target": {"objectId": mid, "subType": "fact_metric", "name": name}},
            {"value": ",", "type": "character", "level": "resolved", "state": "initial"},
            {"value": val, "type": "constant", "level": "resolved", "state": "initial"},
            {"value": ")", "type": "character", "level": "resolved", "state": "initial"},
        ]

    binary_re = re.compile(r"^\s*\[([^\]]+)\]\s*([\-+*/])\s*\[([^\]]+)\]\s*$")
    m = binary_re.match(formula)
    if m:
        left, op, right = m.groups()
        left, right = left.strip(), right.strip()
        lid, rid = resolve_fn(left), resolve_fn(right)
        if not lid or not rid: return []
        op_did = _OPERATOR_DIDS.get(op)
        op_token = {"value": op, "type": "character", "level": "resolved", "state": "initial"}
        if op_did: op_token["target"] = {"objectId": op_did, "subType": "function", "name": op}
        return [
            {"value": "&", "type": "character", "level": "resolved", "state": "initial"},
            {"value": f"[{left}]", "type": "object_reference", "level": "client", "state": "initial", "target": {"objectId": lid, "subType": "fact_metric", "name": left}},
            op_token,
            {"value": f"[{right}]", "type": "object_reference", "level": "client", "state": "initial", "target": {"objectId": rid, "subType": "fact_metric", "name": right}},
        ]

    ratio_re = re.compile(r"^\s*(Sum|Count|Avg|Average|Median|Min|Max)\s*\(\s*\[([^\]]+)\]\s*\)\s*([\-+*/])\s*(Sum|Count|Avg|Average|Median|Min|Max)\s*\(\s*\[([^\]]+)\]\s*\)\s*$")
    m = ratio_re.match(formula)
    if m:
        fn1, name1, op, fn2, name2 = m.groups()
        mid1, mid2 = resolve_fn(name1), resolve_fn(name2)
        if not mid1 or not mid2: return []
        did1, did2 = _FUNCTION_DIDS.get(fn1), _FUNCTION_DIDS.get(fn2)
        op_did = _OPERATOR_DIDS.get(op)
        if not did1 or not did2: return []

        def _agg_token(fn, did, name, mid):
            return [
                {"value": fn, "type": "function", "level": "client", "state": "initial", "target": {"objectId": did, "subType": "function", "name": fn}},
                {"value": "(", "type": "character", "level": "resolved", "state": "initial"},
                {"value": f"[{name}]", "type": "object_reference", "level": "client", "state": "initial", "target": {"objectId": mid, "subType": "fact_metric", "name": name}},
                {"value": ")", "type": "character", "level": "resolved", "state": "initial"},
            ]

        op_token = {"value": op, "type": "character", "level": "resolved", "state": "initial"}
        if op_did: op_token["target"] = {"objectId": op_did, "subType": "function", "name": op}

        tokens = [{"value": "&", "type": "character", "level": "resolved", "state": "initial"}]
        tokens.extend(_agg_token(fn1, did1, name1, mid1))
        tokens.append(op_token)
        tokens.extend(_agg_token(fn2, did2, name2, mid2))
        return tokens

    return []


def _create_metric_shell_raw(client: MstrClient, model_id: str, changeset_id: str | None) -> str:
    """Create a metric shell via raw requests."""
    s = client._get_raw_session()
    headers = client._get_headers()
    if changeset_id: headers["x-mstr-ms-changeset"] = changeset_id
    r = s.post(
        f"{client._base_url}/api/model/dataModels/{model_id}/metrics?showAdvancedProperties=true",
        headers=headers, json={"information": {"subType": "metric"}}, timeout=30,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Metric shell creation failed: {r.text[:300]}")
    return r.json()["information"]["objectId"]


def _validate_expression_raw(client: MstrClient, model_id: str, metric_id: str,
                             tokens: list, changeset_id: str | None) -> dict:
    """Validate a metric expression via raw requests."""
    s = client._get_raw_session()
    headers = client._get_headers()
    if changeset_id: headers["x-mstr-ms-changeset"] = changeset_id
    r = s.post(
        f"{client._base_url}/api/model/dataModels/{model_id}/metrics/{metric_id}/expression/validate",
        headers=headers, json={"expression": {"tokens": tokens}}, timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Expression validation failed: {r.text[:300]}")
    return r.json()["expression"]


def _update_metric_raw(client: MstrClient, model_id: str, metric_id: str,
                       name: str, expression: dict, changeset_id: str | None):
    """Update a metric via raw requests."""
    s = client._get_raw_session()
    headers = client._get_headers()
    if changeset_id: headers["x-mstr-ms-changeset"] = changeset_id
    r = s.put(
        f"{client._base_url}/api/model/dataModels/{model_id}/metrics/{metric_id}?showExpressionAs=tree&showExpressionAs=tokens&showAdvancedProperties=true&clearUnusedEmbeddedObjects=true",
        headers=headers, json={"information": {"name": name}, "expression": expression}, timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Metric update failed: {r.text[:300]}")


def _fetch_model_attributes(client: MstrClient, model_id: str) -> list[dict]:
    """Fetch real attribute context for AIService."""
    r = client.get(f"/api/model/dataModels/{model_id}/attributes?showExpressionAs=tree&showExpressionAs=tokens")
    return r.json().get("attributes", [])


def _fetch_model_metrics(client: MstrClient, model_id: str) -> list[dict]:
    """Fetch real metric context for AIService."""
    r = client.get(f"/api/model/dataModels/{model_id}/factMetrics?showExpressionAs=tree&showExpressionAs=tokens")
    return r.json().get("factMetrics", [])


def apply_column_mapping(client: MstrClient, workspace_id: str,
                         pipeline_id: str, column_pairs: list[list[int]]):
    """Apply column mappings to a pipeline."""
    client.post(
        f"/api/dataServer/workspaces/{workspace_id}/pipelines/{pipeline_id}/relationships",
        json={"columnPairs": column_pairs}
    )
    print(f"[+] Column mapping applied ({len(column_pairs)} pairs)")


def update_model_info(client: MstrClient, model_id: str,
                      name: str | None = None,
                      description: str | None = None,
                      folder_id: str | None = None):
    """Update model metadata."""
    patch = {"information": {}}
    if name: patch["information"]["name"] = name
    if description: patch["information"]["description"] = description
    if folder_id: patch["information"]["destinationFolderId"] = folder_id
    if not patch["information"]: return
    client.patch(f"/api/model/dataModels/{model_id}?showExecutiveSummary=true", json=patch)
    print(f"[+] Model updated")


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
        return [
            TableSource(
                datasource_name=item["datasource"],
                schema=item.get("schema", "public"),
                table=item["table"],
                date_columns=[c.strip() for c in item.get("date_columns", "").split(",") if c.strip()],
            ) for item in cfg
        ]
    # Single-source backward-compatible mode
    return [TableSource(
        datasource_name=args.datasource,
        schema=args.schema,
        table=args.table,
        date_columns=[c.strip() for c in (args.date_columns or "").split(",") if c.strip()],
    )]


def multi_pipeline_phase1(client: MstrClient, workspace_id: str,
                          sources: list[TableSource]) -> list[dict]:
    """Phase 1: Create an empty pipeline for each source table."""
    ready_sources = []
    for src in sources:
        ds = _find_datasource(client, src.datasource_name)
        datasource_id = ds["id"]
        datasource_name = ds["name"]
        print(f"\n[+] Source: {src.schema}.{src.table} @ {datasource_name}")
        p1_result = create_pipeline_phase1(
            client, workspace_id, datasource_id, datasource_name,
            src.schema, src.table
        )
        p1_ready = p1_result.get("ready") or poll_pipeline(client, workspace_id, p1_result["poll_id"])
        ready_sources.append({
            "source": src, "pipeline": p1_ready,
            "datasource_id": datasource_id, "datasource_name": datasource_name,
        })
    return ready_sources


def multi_pipeline_phase2(client: MstrClient, workspace_id: str,
                          ready_sources: list[dict]) -> list[dict]:
    """Phase 2: Rebuild each source into a clean pipeline."""
    p2_results = []
    for entry in ready_sources:
        p2 = create_pipeline_phase2(client, workspace_id, entry["pipeline"])
        print(f"[+] Phase-2 pipeline {p2['id']} for {entry['source'].table}")
        if entry["source"].date_columns:
            apply_date_conversions(client, workspace_id, p2["id"], entry["source"].date_columns)
        p2_results.append({**entry, "phase2": p2, "phase2_id": p2["id"]})
    return p2_results


def add_all_tables_to_model(client: MstrClient, model_id: str,
                            p2_results: list[dict]) -> list[tuple[str, str, dict]]:
    """Attach each Phase-2 pipeline as a logical table."""
    tables = []
    for entry in p2_results:
        table_id, col_map = add_table_to_model(client, model_id, entry["phase2"])
        tables.append((entry["source"].table, table_id, col_map))
        entry["table_id"] = table_id
        entry["col_map"] = col_map
    return tables


def create_all_attributes(client: MstrClient, model_id: str,
                          tables: list[tuple[str, str, dict]]) -> list[str]:
    """Batch-create attributes for every column across all tables."""
    all_attr_ids = []
    for _, table_id, col_map in tables:
        attr_ids = create_attributes_batch(client, model_id, col_map, table_id)
        all_attr_ids.extend(attr_ids)
    return all_attr_ids


def create_all_fact_metrics(client: MstrClient, model_id: str,
                            tables: list[tuple[str, str, dict]]) -> dict[str, str]:
    """Create Sum fact-metrics for every numeric column across all tables."""
    merged = {}
    for _, table_id, col_map in tables:
        metric_map = create_fact_metrics_batch(client, model_id, col_map, table_id)
        merged.update(metric_map)
    return merged


def apply_all_column_mappings(client: MstrClient, workspace_id: str,
                              p2_results: list[dict]):
    """Auto-map columns for each Phase-2 pipeline."""
    for entry in p2_results:
        cols = entry["phase2"].get("rootTable", {}).get("children", [{}])[0].get("columns", [])
        pairs = [[i, 0] for i in range(1, len(cols))]
        if pairs:
            apply_column_mapping(client, workspace_id, entry["phase2_id"], pairs)


def _assign_folder_post_commit(client: MstrClient, model_id: str, folder_id: str):
    """Re-apply folder ID after main commit, as it can be dropped."""
    client.changeset_id = None
    cs2 = None
    try:
        r = client.post("/api/model/changesets", json={})
        cs2 = r.json()["id"]
        client.changeset_id = cs2
        client.patch(
            f"/api/model/dataModels/{model_id}?showExecutiveSummary=true",
            json={"information": {"destinationFolderId": folder_id}}
        )
        print(f"[+] Folder patch applied in new changeset {cs2}")
        client.post(f"/api/model/changesets/{cs2}/commit", json={})
        print("[+] Folder commit in progress...")
        poll_changeset_commit(client, cs2)
        print("[+] Folder assigned successfully")
    finally:
        if cs2:
            client.delete(f"/api/model/changesets/{cs2}")
            client.changeset_id = None
            print(f"[+] Cleanup changeset {cs2}")


def create_model(client: MstrClient, args):
    """Orchestrates the creation of a new Mosaic model."""
    sources = _load_sources(args)
    print(f"[+] Building model from {len(sources)} source(s)")
    for s in sources:
        print(f"   - {s.schema}.{s.table} @ {s.datasource_name}")

    # Create changeset and model
    folder_id = args.folder_id or "0268E42CB84F8CCCE28909A111004E8F"
    r_cs = client.post("/api/model/changesets", json={})
    cs = r_cs.json()["id"]
    client.changeset_id = cs
    print(f"[+] Changeset {cs}")

    model_body = {"dataServeMode": "in_memory", "information": {"name": args.model_name}}
    if folder_id:
        model_body["information"]["destinationFolderId"] = folder_id
    r_model = client.post("/api/model/dataModels", json=model_body)
    model_id = r_model.json()["information"]["objectId"]
    print(f"[+] Model created: {model_id}")

    # Create workspace
    r_ws = client.post("/api/dataServer/workspaces", json={})
    ws = r_ws.json()["id"]
    print(f"[+] Workspace {ws}")

    try:
        p1_ready = multi_pipeline_phase1(client, ws, sources)
        p2_results = multi_pipeline_phase2(client, ws, p1_ready)
        tables = add_all_tables_to_model(client, model_id, p2_results)
        attr_ids = create_all_attributes(client, model_id, tables)
        metric_map = create_all_fact_metrics(client, model_id, tables)
        calc_ids = create_calculated_metrics(client, model_id, metric_map, cs)
        metric_ids = list(metric_map.values()) + calc_ids
        apply_all_column_mappings(client, ws, p2_results)
        update_model_info(client, model_id, name=args.model_name, description=args.description, folder_id=args.folder_id)

        print("[+] Committing...")
        client.post(f"/api/model/changesets/{cs}/commit", json={})
        poll_changeset_commit(client, cs)
        print("[+] Model saved")

        if args.folder_id:
            _assign_folder_post_commit(client, model_id, args.folder_id)

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
            client.delete(f"/api/dataServer/workspaces/{ws}")
            print(f"[+] Workspace {ws} deleted")
        client.delete(f"/api/model/changesets/{cs}")
        print(f"[+] Changeset {cs} deleted")


# ------------------------------------------------------------------
# ADD COLUMN (Ported from add_mosaic_column_v2.1.py)
# ------------------------------------------------------------------

def _infer_column_type(client: MstrClient, table_name: str, column_name: str) -> dict:
    """Use AI service to infer data type for a new column."""
    s = client._get_raw_session()
    payload = {"payload": {"type": "infer", "enableGeoRoleInference": True, "async": True, "tableName": table_name, "columns": [{"columnName": column_name, "dataType": None}]}}
    r = s.post(f"{client._base_url}/api/aiservice/model/v2/columns/infer", headers=client._get_headers(), json=payload, timeout=60)
    r.raise_for_status()
    task_id = r.json().get("taskId")
    for _ in range(30): # Poll for completion
        status_r = s.get(f"{client._base_url}/api/aiservice/model/v2/columns/infer/status/{task_id}/0", headers=client._get_headers(), timeout=30)
        status_r.raise_for_status()
        status_json = status_r.json()
        if status_json.get("status") == "completed":
            return status_json.get("result", [{}])[0]
        elif status_json.get("status") == "failed":
            raise RuntimeError(f"AI Inference failed: {status_json}")
        time.sleep(2)
    raise RuntimeError("AI Inference timed out.")

def _build_column_entry(col_name: str, mstr_type: str, precision: int = 32000) -> dict:
    """Builds the JSON structure for a new column."""
    type_map = {"string": "utf8_char", "integer": "int64", "number": "double", "float": "double", "boolean": "bool", "date": "date", "datetime": "timestamp", "time": "time"}
    mapped_type = type_map.get(mstr_type.lower(), mstr_type)
    return {"id": uuid.uuid4().hex.upper(), "name": col_name, "dataType": {"type": mapped_type, "precision": precision, "scale": 0}, "sourceDataType": {"type": mapped_type, "precision": precision, "scale": 0}}

def _build_attribute_payload(attr_name: str, desc: str, table_id: str, col_id: str) -> dict:
    """Builds the payload to create a new attribute from a column."""
    return {"information": {"name": attr_name, "description": desc}, "forms": [{"name": attr_name, "displayFormat": "text", "expressions": [{"expression": {"tree": {"objectId": col_id, "type": "column_reference"}}, "tables": [{"objectId": table_id, "subType": "logical_table"}]}], "semanticRole": "none"}], "keyForm": {"name": attr_name}, "displays": {"reportDisplays": [{"name": attr_name}], "browseDisplays": [{"name": attr_name}]}, "autoDetectLookupTable": True, "attributeLookupTable": {"objectId": table_id, "subType": "logical_table"}}

def add_column(client: MstrClient, args):
    """Orchestrates adding a new column to a model table and creating an attribute for it."""
    print(f"[ ] Adding column '{args.column_name}' to table '{args.table_name}' in model '{args.model_id}'")
    with Changeset(client, args.model_id) as cs:
        tables = get_model_tables(client, args.model_id)
        table = next((t for t in tables if t.get("information", {}).get("name") == args.table_name), None)
        if not table:
            raise RuntimeError(f"Table '{args.table_name}' not found in model {args.model_id}")
        table_id = table["information"]["objectId"]
        print(f"[+] Target table found: {table_name} ({table_id})")

        mstr_type, precision, description = "utf8_char", 32000, args.description
        if args.infer_type:
            print(f"[ ] Inferring type for '{args.column_name}' via AI service...")
            inferred = _infer_column_type(client, args.table_name, args.column_name)
            dt = inferred.get("dataType", {})
            mstr_type = dt.get("type", "utf8_char")
            precision = dt.get("precision", 32000)
            if not description:
                description = inferred.get("description", f"Attribute for {args.column_name}")
            print(f"[+] Inferred type: {mstr_type}, precision: {precision}")

        table_def = get_model_table(client, args.model_id, table_id)
        col_entry = _build_column_entry(args.column_name, mstr_type, precision)
        
        pipeline_str = table_def.get("physicalTable", {}).get("pipeline", "{}")
        pipeline = json.loads(pipeline_str)
        for child in pipeline.get("rootTable", {}).get("children", []):
            if "columns" in child and not any(c.get("name") == col_entry["name"] for c in child["columns"]):
                child["columns"].append({k: v for k, v in col_entry.items() if k != 'id'})
        
        patch_body = {"physicalTable": {"type": "pipeline", "pipeline": json.dumps(pipeline)}}
        patch_req = {"method": "PATCH", "path": f"/model/dataModels/{args.model_id}/tables/{table_id}", "body": patch_body}
        
        print("[ ] Patching table definition with new column...")
        patch_responses = client.post("/api/model/batch", json={"requests": [patch_req]}).json()["responses"]
        
        if patch_responses[0].get("status", 500) >= 400:
            raise RuntimeError(f"Table PATCH failed: {patch_responses[0].get('body')}")

        real_col_id = None
        for col in patch_responses[0].get("body", {}).get("physicalTable", {}).get("columns", []):
            if col.get("information", {}).get("name") == args.column_name:
                real_col_id = col["information"]["objectId"]
                break
        if not real_col_id:
            raise RuntimeError("Could not find server-assigned ID for the new column.")
        print(f"[+] Server assigned column ID: {real_col_id}")

        attr_name = args.attr_name or args.column_name.replace("_", " ").title()
        attr_payload = _build_attribute_payload(attr_name, description, table_id, real_col_id)
        attr_req = {"method": "POST", "path": f"/model/dataModels/{args.model_id}/attributes?showExpressionAs=tree&showExpressionAs=tokens&allowLink=true", "body": attr_payload}
        
        print(f"[ ] Creating attribute '{attr_name}'...")
        attr_responses = client.post("/api/model/batch", json={"requests": [attr_req]}).json()["responses"]

        if attr_responses[0].get("status", 500) >= 400:
            raise RuntimeError(f"Attribute creation failed: {attr_responses[0].get('body')}")

        print(f"✅ Attribute '{attr_name}' added to model. Committing changeset...")

    # Publish after commit
    print("[ ] Triggering model publish...")
    client.post(f"/api/dataModels/{args.model_id}/publish", json={"tables": [{"id": table_id, "refreshPolicy": "replace"}]})
    print("✅ Done.")


# ------------------------------------------------------------------
# CREATE METRIC (Ported from 03_create_mosaic_metric.py)
# ------------------------------------------------------------------

def _build_tokens_for_sum(metric_map: dict, metric_name: str) -> list:
    mid = metric_map[metric_name]
    SUM_FUNCTION_ID = "8107C31BDD9911D3B98100C04F2233EA"
    return [{"value": "&", "type": "character"}, {"value": "Sum", "type": "function", "target": {"objectId": SUM_FUNCTION_ID, "subType": "function", "name": "Sum"}}, {"value": "(", "type": "character"}, {"value": f"[{metric_name}]", "type": "object_reference", "target": {"objectId": mid, "subType": "fact_metric", "name": metric_name}}, {"value": ")", "type": "character"}]

def _build_tokens_for_calc(metric_map: dict, left: str, right: str, op_char: str) -> list:
    lid = metric_map[left]
    rid = metric_map[right]
    op_map = {"-": "8107C311DD9911D3B98100C04F2233EA", "+": "0DFE6FB4356F4FA2A0C1C10D0B415355"}
    op_token = {"value": op_char, "type": "character"}
    if op_id := op_map.get(op_char):
        op_token["target"] = {"objectId": op_id, "subType": "function", "name": op_char}
    return [{"value": "&", "type": "character"}, {"value": f"[{left}]", "type": "object_reference", "target": {"objectId": lid, "subType": "fact_metric", "name": left}}, op_token, {"value": f"[{right}]", "type": "object_reference", "target": {"objectId": rid, "subType": "fact_metric", "name": right}}]

def create_metric(client: MstrClient, args):
    """Orchestrates creating a new calculated metric in a model."""
    print(f"[ ] Creating metric '{args.metric_name}' in model '{args.model_id}'")
    with Changeset(client, args.model_id) as cs:
        all_metrics = find_metric_by_name(client, None, return_all=True)
        metric_map = {m['name']: m['objectId'] for m in all_metrics}
        print(f"[+] Found {len(metric_map)} existing metrics.")

        if args.skip_if_exists and args.metric_name in metric_map:
            print(f"[+] Metric '{args.metric_name}' already exists. Skipping.")
            return

        shell_r = client.post(f"/api/model/dataModels/{args.model_id}/metrics", json={"information": {"subType": "metric"}})
        new_metric_id = shell_r.json()["information"]["objectId"]
        print(f"[+] Created metric shell: {new_metric_id}")

        tokens = []
        if args.mode == "sum":
            if args.source not in metric_map:
                raise RuntimeError(f"Source metric '{args.source}' not found.")
            tokens = _build_tokens_for_sum(metric_map, args.source)
        elif args.mode == "calc":
            op_char = "-" if "-" in args.source else "+"
            left, right = [s.strip() for s in args.source.split(op_char, 1)]
            if left not in metric_map or right not in metric_map:
                raise RuntimeError(f"One or both source metrics ('{left}', '{right}') not found.")
            tokens = _build_tokens_for_calc(metric_map, left, right, op_char)

        validate_r = client.post(f"/api/model/dataModels/{args.model_id}/metrics/{new_metric_id}/expression/validate", json={"expression": {"tokens": tokens}})
        validated_expr = validate_r.json()["expression"]
        print("[+] Expression validated successfully.")

        update_payload = {"information": {"name": args.metric_name}, "expression": validated_expr}
        client.put(f"/api/model/dataModels/{args.model_id}/metrics/{new_metric_id}?showExpressionAs=tree&showExpressionAs=tokens", json=update_payload)
        print(f"[+] Updated metric shell with name and expression. Committing...")

    print("[ ] Triggering model publish...")
    client.post(f"/api/dataModels/{args.model_id}/publish")
    print("✅ Done.")


# ------------------------------------------------------------------
# DELETE ATTRIBUTE (Ported from 08_delete_mosaic_attribute.py)
# ------------------------------------------------------------------

def delete_attribute(client: MstrClient, args):
    """Orchestrates deleting an attribute from a model."""
    print(f"[ ] Deleting attribute '{args.attr_name}' from model '{args.model_id}'")
    with Changeset(client, args.model_id) as cs:
        attr = find_attribute_by_name(client, args.attr_name)
        if not attr:
            raise RuntimeError(f"Attribute '{args.attr_name}' not found in model.")
        
        attr_id = attr['objectId']
        print(f"[+] Found attribute '{args.attr_name}' with ID: {attr_id}")

        delete_r = client.delete(f"/api/model/dataModels/{args.model_id}/attributes/{attr_id}")
        print(f"[+] DELETE request sent (Status: {delete_r.status_code}). Committing...")

    # Verify deletion
    if not find_attribute_by_name(client, args.attr_name):
        print(f"✅ Attribute '{args.attr_name}' removed successfully.")
    else:
        print(f"[!] Warning: Attribute '{args.attr_name}' still present after commit. A manual publish may be required.")


# ------------------------------------------------------------------
# DELETE METRIC (Ported from 09_delete_mosaic_metric.py)
# ------------------------------------------------------------------

def delete_metric(client: MstrClient, args):
    """Orchestrates deleting a metric from a model."""
    print(f"[ ] Deleting metric '{args.metric_name}' from model '{args.model_id}'")
    with Changeset(client, args.model_id) as cs:
        metric = find_metric_by_name(client, args.metric_name)
        if not metric:
            raise RuntimeError(f"Metric '{args.metric_name}' not found in model.")
        
        metric_id = metric["objectId"]
        endpoint_suffix = metric["endpoint"]
        print(f"[+] Found metric '{args.metric_name}' with ID: {metric_id} (type: {endpoint_suffix})")

        delete_r = client.delete(f"/api/model/dataModels/{args.model_id}/{endpoint_suffix}/{metric_id}")
        print(f"[+] DELETE request sent (Status: {delete_r.status_code}). Committing...")

    # Verify deletion
    if not find_metric_by_name(client, args.metric_name):
        print(f"✅ Metric '{args.metric_name}' removed successfully.")
    else:
        print(f"[!] Warning: Metric '{args.metric_name}' still present after commit. A manual publish may be required.")