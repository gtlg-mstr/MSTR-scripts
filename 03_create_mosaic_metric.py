#!/usr/bin/env python3
# Tested on Strategy One April 2026 release.

"""
Create a calculated metric in a MicroStrategy Mosaic Model.

Uses mstrio-py for connection and authentication.
Supports Sum (wrap a fact metric in Sum()) and Calc (binary operations + -).

Usage (Sum mode):
    python 03_create_mosaic_metric.py \\
        --model-id AC49E200F8E041C1AF38D87B99EF19DC \\
        --metric-name "Total Cost Sum" \\
        --source "Total Cost" \\
        --mode sum

Usage (Calc mode):
    python 03_create_mosaic_metric.py \\
        --model-id AC49E200F8E041C1AF38D87B99EF19DC \\
        --metric-name "Profit" \\
        --source "Revenue-Cost" \\
        --mode calc
"""

import os, sys, json, argparse, logging, time
from pathlib import Path

from mstrio.connection import Connection

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


def _h(conn: Connection, changeset: str = None, instance_id: str = None) -> dict:
    """Build request headers with ProjectID and optional changeset / instance."""
    h = {"X-MSTR-ProjectID": conn.project_id}
    if changeset:
        h["x-mstr-ms-changeset"] = changeset
    if instance_id:
        h["X-MSTR-DataModelInstanceId"] = instance_id
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
    conn.post(
        endpoint=f"/api/model/changesets/{cs}/operations",
        headers=_h(conn),
        params={"operationType": "rebase", "dataModelId": model_id},
    )
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


def publish_model(conn: Connection, model_id: str):
    LOG.info("Publishing model...")

    # 1. Get data model instance ID
    LOG.info("Getting data model instance...")
    r = conn.post(
        endpoint=f"/api/dataModels/{model_id}/instances",
        headers=_h(conn),
    )
    instance_id = None
    if r.ok:
        instance_id = r.headers.get("X-Mstr-Datamodelinstanceid") or r.headers.get("X-MSTR-DataModelInstanceId")
        if not instance_id and r.content:
            try:
                rj = r.json()
                instance_id = rj.get("id")
            except Exception:
                pass

    if not instance_id:
        LOG.warning("Could not get instance ID; publish may fail")
        instance_id = model_id

    LOG.info("Using instance ID: %s", instance_id)

    # 2. Get tables
    r = conn.get(
        endpoint=f"/api/model/dataModels/{model_id}/tables",
        headers=_h(conn),
    )
    if not r.ok:
        raise RuntimeError(f"Failed to get tables: {r.text}")
    data = r.json()
    tables = data.get("tables", data) if isinstance(data, dict) else data
    table_ids = []
    for t in tables or []:
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
    r = conn.post(
        endpoint=f"/api/dataModels/{model_id}/publish",
        headers=_h(conn, instance_id=instance_id),
        json=publish_payload,
    )
    if not r.ok:
        raise RuntimeError(f"Publish failed: {r.text}")
    LOG.info("Publish accepted (status %d)", r.status_code)

    # 4. Poll status
    for _ in range(10):
        time.sleep(3)
        r = conn.get(
            endpoint=f"/api/dataModels/{model_id}/publishStatus",
            headers=_h(conn, instance_id=instance_id),
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


def find_metrics(conn: Connection, model_id: str, cs: str = None) -> dict:
    """Return {metric_name: objectId} from both /factMetrics and /metrics endpoints."""
    name_to_id = {}
    for endpoint_suffix in ["factMetrics", "metrics"]:
        r = conn.get(
            endpoint=f"/api/model/dataModels/{model_id}/{endpoint_suffix}",
            headers=_h(conn, cs),
            params={"limit": 500, "fields": "information"},
        )
        if not r.ok:
            continue
        data = r.json()
        items = data.get(endpoint_suffix, data) if isinstance(data, dict) else data
        for item in items or []:
            if not isinstance(item, dict):
                continue
            info = item.get("information", {})
            name = info.get("name")
            oid = info.get("objectId")
            if name and oid:
                name_to_id[name] = oid
    return name_to_id


def create_metric_shell(conn: Connection, model_id: str, cs: str = None) -> str:
    payload = {"information": {"subType": "metric"}}
    r = conn.post(
        endpoint=f"/api/model/dataModels/{model_id}/metrics",
        headers=_h(conn, cs),
        params={"showAdvancedProperties": "true"},
        json=payload,
    )
    if not r.ok:
        raise RuntimeError(f"Metric shell creation failed: {r.text}")
    return r.json()["information"]["objectId"]


def validate_expression(conn: Connection, model_id: str, metric_id: str, tokens: list, cs: str = None) -> dict:
    payload = {"expression": {"tokens": tokens}}
    r = conn.post(
        endpoint=f"/api/model/dataModels/{model_id}/metrics/{metric_id}/expression/validate",
        headers=_h(conn, cs),
        json=payload,
    )
    if not r.ok:
        raise RuntimeError(f"Expression validation failed: {r.text}")
    return r.json()["expression"]


def update_metric(conn: Connection, model_id: str, metric_id: str, name: str, expression: dict, cs: str = None):
    payload = {
        "information": {"name": name},
        "expression": expression,
    }
    r = conn.put(
        endpoint=f"/api/model/dataModels/{model_id}/metrics/{metric_id}"
        "?showExpressionAs=tree&showExpressionAs=tokens"
        "&showAdvancedProperties=true&clearUnusedEmbeddedObjects=true",
        headers=_h(conn, cs),
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
    parser.add_argument(
        "--source", required=True,
        help="Sum mode: 'MetricName'. Calc mode: 'Metric1-Metric2' or 'Metric1+Metric2'.",
    )
    parser.add_argument("--mode", choices=["sum", "calc"], default="calc")
    parser.add_argument("--skip-if-exists", action="store_true", default=True,
                        help="Skip creation if metric already exists")
    return parser.parse_args()


def run():
    args = parse_args()
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

    SUM_FUNCTION_ID = "8107C31BDD9911D3B98100C04F2233EA"
    MINUS_FUNCTION_ID = "8107C311DD9911D3B98100C04F2233EA"

    try:
        cs = create_changeset(conn, args.model_id)
        LOG.info("Using changeset %s", cs)

        metric_map = find_metrics(conn, args.model_id, cs)
        LOG.info("Found %d metrics in model", len(metric_map))

        if args.skip_if_exists and args.metric_name in metric_map:
            LOG.info("Metric '%s' already exists (ID: %s). Skipping creation.",
                     args.metric_name, metric_map[args.metric_name])
        else:
            new_metric_id = create_metric_shell(conn, args.model_id, cs)
            LOG.info("Created metric shell %s", new_metric_id)

            if args.mode == "sum":
                if args.source not in metric_map:
                    raise RuntimeError(
                        f"Metric '{args.source}' not found. Available: {list(metric_map.keys())}"
                    )
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
                    raise RuntimeError(
                        f"Metric '{left}' not found. Available: {list(metric_map.keys())}"
                    )
                if right not in metric_map:
                    raise RuntimeError(
                        f"Metric '{right}' not found. Available: {list(metric_map.keys())}"
                    )

                tokens = build_tokens_for_calc(metric_map, left, right, op_char, op_id)

            validated_expr = validate_expression(conn, args.model_id, new_metric_id, tokens, cs)
            LOG.info("Expression validated successfully")
            update_metric(conn, args.model_id, new_metric_id, args.metric_name, validated_expr, cs)

        commit_changeset(conn, cs)
        publish_model(conn, args.model_id)
        LOG.info("Done. Metric '%s' is created and published.", args.metric_name)
    except Exception as e:
        LOG.error("Operation failed: %s", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
