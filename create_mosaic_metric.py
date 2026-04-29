
# Tested on Strategy One April 2026 release.

#!/usr/bin/env python3
import os, sys, json, argparse, logging, requests, time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger("create_mosaic_metric")

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

def login(env: dict, base: str) -> tuple:
    r = requests.post(
        f"{base}/api/auth/login",
        json={"username": env["MSTR_USERNAME"], "password": env["MSTR_PASSWORD"], "loginMode": 1},
        timeout=30,
    )
    r.raise_for_status()
    token = r.headers["X-Mstr-Authtoken"]
    cookies = r.cookies
    project_name = env.get("MSTR_PROJECT_NAME", "Shared Studio")
    r2 = requests.get(f"{base}/api/projects", headers={"X-Mstr-AuthToken": token}, cookies=cookies, timeout=30)
    r2.raise_for_status()
    rj = r2.json()
    pid = None
    if isinstance(rj, list):
        pid = next((p["id"] for p in rj if p.get("name") == project_name), None)
    elif isinstance(rj, dict):
        pid = next((p["id"] for p in rj.get("projects", []) if p.get("name") == project_name), None)
    if not pid:
        raise RuntimeError(f"Project '{project_name}' not found")
    LOG.info("Authenticated to project %s (%s)", project_name, pid)
    return token, cookies, pid

def _model_headers(token: str, pid: str, changeset: str = None, instance_id: str = None) -> dict:
    h = {
        "X-Mstr-AuthToken": token,
        "X-MSTR-ProjectID": pid,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if changeset:
        h["x-mstr-ms-changeset"] = changeset
    if instance_id:
        h["X-MSTR-DataModelInstanceId"] = instance_id
    return h

def create_changeset(base: str, token: str, cookies, pid: str, model_id: str) -> str:
    r = requests.post(
        f"{base}/api/model/changesets?enableOperationHistory=true",
        headers=_model_headers(token, pid),
        cookies=cookies,
        timeout=30,
    )
    r.raise_for_status()
    cs = r.json()["id"]
    LOG.info("Created changeset %s", cs)
    requests.post(
        f"{base}/api/model/changesets/{cs}/operations",
        headers=_model_headers(token, pid),
        cookies=cookies,
        params={"operationType": "rebase", "dataModelId": model_id},
        timeout=30,
    )
    return cs

def commit_changeset(base: str, token: str, cookies, pid: str, cs: str) -> bool:
    r = requests.post(
        f"{base}/api/model/changesets/{cs}/commit",
        headers=_model_headers(token, pid),
        cookies=cookies,
        timeout=120,
    )
    if not r.ok:
        LOG.error("Commit failed: %s", r.text[:500])
        return False
    LOG.info("Changeset committed successfully")
    return True

def publish_model(base: str, token: str, cookies, pid: str, model_id: str):
    LOG.info("Publishing model...")
    
    # 1. Get data model instance ID from header of POST /instances
    LOG.info("Getting data model instance...")
    r = requests.post(
        f"{base}/api/dataModels/{model_id}/instances",
        headers=_model_headers(token, pid),
        cookies=cookies,
        timeout=30,
    )
    instance_id = None
    if r.ok:
        instance_id = r.headers.get("X-Mstr-Datamodelinstanceid") or r.headers.get("X-MSTR-DataModelInstanceId")
        if not instance_id and r.content:
            try:
                rj = r.json()
                instance_id = rj.get("id")
            except:
                pass
    
    if not instance_id:
        LOG.warning("Could not get instance ID; publish may fail")
        instance_id = model_id
    
    LOG.info("Using instance ID: %s", instance_id)
    
    # 2. Get tables
    r = requests.get(
        f"{base}/api/model/dataModels/{model_id}/tables",
        headers=_model_headers(token, pid),
        cookies=cookies,
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Failed to get tables: {r.text}")
    data = r.json()
    tables = data.get("tables", data) if isinstance(data, dict) else data
    table_ids = []
    for t in (tables or []):
        if isinstance(t, dict):
            tid = t.get("information", {}).get("objectId")
            if tid:
                table_ids.append(tid)
    
    if not table_ids:
        LOG.warning("No tables found; using hardcoded IDs")
        table_ids = [
            "D9613AFFF5314A0DB2F1D9988A681F0B",
            "F9B8F49AD2D04FECAF9331395ED887B7",
            "680B2296FEA9471CBE9820E68762478F",
        ]
    
    # 3. Publish
    publish_payload = {"tables": [{"id": tid, "refreshPolicy": "replace"} for tid in table_ids]}
    r = requests.post(
        f"{base}/api/dataModels/{model_id}/publish",
        headers=_model_headers(token, pid, instance_id=instance_id),
        cookies=cookies,
        json=publish_payload,
        timeout=120,
    )
    # Publish returns 204 on success
    if not r.ok:
        raise RuntimeError(f"Publish failed: {r.text}")
    LOG.info("Publish accepted (status %d)", r.status_code)
    
    # 4. Poll status
    for _ in range(10):
        time.sleep(3)
        r = requests.get(
            f"{base}/api/dataModels/{model_id}/publishStatus",
            headers=_model_headers(token, pid, instance_id=instance_id),
            cookies=cookies,
            timeout=30,
        )
        if r.ok:
            status = r.json()
            if status.get("status") in ["Succeeded", "Ready", "Completed"]:
                LOG.info("Publish completed successfully")
                return
            elif status.get("status") in ["Failed", "Error"]:
                raise RuntimeError(f"Publish failed with status: {status}")
            else:
                LOG.info("Publish status: %s", status.get("status", "unknown"))
    LOG.warning("Publish status polling timed out; check Studio Web for final status")

def find_metrics(base: str, token: str, cookies, pid: str, model_id: str, cs: str = None) -> dict:
    name_to_id = {}
    for endpoint_suffix in ["factMetrics", "metrics"]:
        url = f"{base}/api/model/dataModels/{model_id}/{endpoint_suffix}"
        r = requests.get(
            url,
            headers=_model_headers(token, pid, cs),
            cookies=cookies,
            params={"limit": 500, "fields": "information"},
        )
        if not r.ok:
            continue
        data = r.json()
        items = data.get(endpoint_suffix, data) if isinstance(data, dict) else data
        for item in (items or []):
            if not isinstance(item, dict):
                continue
            info = item.get("information", {})
            name = info.get("name")
            oid = info.get("objectId")
            if name and oid:
                name_to_id[name] = oid
    return name_to_id

def create_metric_shell(base: str, token: str, cookies, pid: str, model_id: str, cs: str = None) -> str:
    payload = {"information": {"subType": "metric"}}
    r = requests.post(
        f"{base}/api/model/dataModels/{model_id}/metrics?showAdvancedProperties=true",
        headers=_model_headers(token, pid, cs),
        cookies=cookies,
        json=payload,
    )
    if not r.ok:
        raise RuntimeError(f"Metric shell creation failed: {r.text}")
    return r.json()["information"]["objectId"]

def validate_expression(base: str, token: str, cookies, pid: str, model_id: str, metric_id: str, tokens: list, cs: str = None) -> dict:
    payload = {"expression": {"tokens": tokens}}
    r = requests.post(
        f"{base}/api/model/dataModels/{model_id}/metrics/{metric_id}/expression/validate",
        headers=_model_headers(token, pid, cs),
        cookies=cookies,
        json=payload,
    )
    if not r.ok:
        raise RuntimeError(f"Expression validation failed: {r.text}")
    return r.json()["expression"]

def update_metric(base: str, token: str, cookies, pid: str, model_id: str, metric_id: str, name: str, expression: dict, cs: str = None):
    payload = {
        "information": {"name": name},
        "expression": expression,
    }
    r = requests.put(
        f"{base}/api/model/dataModels/{model_id}/metrics/{metric_id}?showExpressionAs=tree&showExpressionAs=tokens&showAdvancedProperties=true&clearUnusedEmbeddedObjects=true",
        headers=_model_headers(token, pid, cs),
        cookies=cookies,
        json=payload,
    )
    if not r.ok:
        raise RuntimeError(f"Metric update failed: {r.text}")
    LOG.info("Updated metric '%s' with expression", name)

def build_tokens_for_sum(metric_map: dict, metric_name: str, sum_function_id: str) -> list:
    mid = metric_map[metric_name]
    return [
        {"value": "&", "type": "character", "level": "resolved", "state": "initial"},
        {
            "value": "Sum",
            "type": "function",
            "level": "client",
            "state": "initial",
            "target": {"objectId": sum_function_id, "subType": "function", "name": "Sum"},
        },
        {"value": "(", "type": "character", "level": "resolved", "state": "initial"},
        {
            "value": f"[{metric_name}]",
            "type": "object_reference",
            "level": "client",
            "state": "initial",
            "target": {"objectId": mid, "subType": "fact_metric", "name": metric_name},
        },
        {"value": ")", "type": "character", "level": "resolved", "state": "initial"},
    ]

def build_tokens_for_calc(metric_map: dict, left: str, right: str, op_char: str, op_function_id: str = None) -> list:
    lid = metric_map[left]
    rid = metric_map[right]

    op_token = {
        "value": op_char,
        "type": "character",
        "level": "resolved",
        "state": "initial",
    }
    if op_function_id:
        op_token["target"] = {"objectId": op_function_id, "subType": "function", "name": op_char}

    return [
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

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--metric-name", required=True)
    parser.add_argument("--source", required=True, help="Sum mode: 'MetricName'. Calc mode: 'Metric1-Metric2' or 'Metric1+Metric2'.")
    parser.add_argument("--mode", choices=["sum", "calc"], default="calc")
    parser.add_argument("--skip-if-exists", action="store_true", default=True, help="Skip creation if metric already exists")
    return parser.parse_args()

def run():
    args = parse_args()
    env = load_env()
    base = env.get("MSTR_BASE_URL", "https://studio.strategy.com/MicroStrategyLibrary")
    token, cookies, pid = login(env, base)

    SUM_FUNCTION_ID = "8107C31BDD9911D3B98100C04F2233EA"
    MINUS_FUNCTION_ID = "8107C311DD9911D3B98100C04F2233EA"

    try:
        cs = create_changeset(base, token, cookies, pid, args.model_id)
        LOG.info("Using changeset %s", cs)

        metric_map = find_metrics(base, token, cookies, pid, args.model_id, cs)
        LOG.info("Found %d metrics in model", len(metric_map))

        if args.skip_if_exists and args.metric_name in metric_map:
            LOG.info("Metric '%s' already exists (ID: %s). Skipping creation.", args.metric_name, metric_map[args.metric_name])
        else:
            new_metric_id = create_metric_shell(base, token, cookies, pid, args.model_id, cs)
            LOG.info("Created metric shell %s", new_metric_id)

            if args.mode == "sum":
                if args.source not in metric_map:
                    raise RuntimeError(f"Metric '{args.source}' not found. Available: {list(metric_map.keys())}")
                tokens = build_tokens_for_sum(metric_map, args.source, SUM_FUNCTION_ID)
            elif args.mode == "calc":
                if "-" in args.source:
                    left, right = [s.strip() for s in args.source.split("-", 1)]
                    op_char = "-"
                    op_id = MINUS_FUNCTION_ID
                elif "+" in args.source:
                    left, right = [s.strip() for s in args.source.split("+", 1)]
                    op_char = "+"
                    op_id = None
                else:
                    raise RuntimeError("Calc mode requires '-' or '+' in source string")

                if left not in metric_map:
                    raise RuntimeError(f"Metric '{left}' not found. Available: {list(metric_map.keys())}")
                if right not in metric_map:
                    raise RuntimeError(f"Metric '{right}' not found. Available: {list(metric_map.keys())}")

                tokens = build_tokens_for_calc(metric_map, left, right, op_char, op_id)

            validated_expr = validate_expression(base, token, cookies, pid, args.model_id, new_metric_id, tokens, cs)
            LOG.info("Expression validated successfully")
            update_metric(base, token, cookies, pid, args.model_id, new_metric_id, args.metric_name, validated_expr, cs)

        commit_changeset(base, token, cookies, pid, cs)
        publish_model(base, token, cookies, pid, args.model_id)
        LOG.info("Done. Metric '%s' is created and published.", args.metric_name)
    except Exception as e:
        LOG.error("Operation failed: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    run()
