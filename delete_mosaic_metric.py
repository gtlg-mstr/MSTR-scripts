#!/usr/bin/env python3
# Tested on Strategy One April 2026 release.

"""
Remove a Metric or FactMetric from a MicroStrategy Mosaic Model.

Uses mstrio-py for connection and authentication.
Resolves by name against both /factMetrics and /metrics endpoints,
DELETEs via changeset, commits, and verifies removal.

Usage:
    python delete_mosaic_metric.py --model-id AC49E200F8E041C1AF38D87B99EF19DC \
        --metric-name "Total Cost Sum"
"""

import os, sys, json, argparse, logging
from pathlib import Path

from mstrio.connection import Connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger("delete_mosaic_metric")


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


def _h(conn: Connection, changeset: str = None) -> dict:
    """Build request headers with ProjectID and optional changeset."""
    h = {"X-MSTR-ProjectID": conn.project_id}
    if changeset:
        h["x-mstr-ms-changeset"] = changeset
    return h


def create_changeset(conn: Connection, model_id: str) -> str:
    r = conn.post(
        endpoint="/api/model/changesets",
        headers=_h(conn),
        json={"dataModelId": model_id},
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
        json={},
    )
    if not r.ok:
        LOG.error("Commit failed: %s %s", r.status_code, r.text[:500])
        return False
    LOG.info("Changeset committed successfully")
    return True


def abort_changeset(conn: Connection, cs: str):
    r = conn.post(
        endpoint=f"/api/model/changesets/{cs}/abort",
        headers=_h(conn),
    )
    LOG.info("Aborted changeset %s (status=%s)", cs, r.status_code)


def get_metrics(conn: Connection, model_id: str, limit: int = 500) -> list:
    all_metrics = []
    for suffix in ["factMetrics", "metrics"]:
        r = conn.get(
            endpoint=f"/api/model/dataModels/{model_id}/{suffix}",
            headers=_h(conn),
            params={"fields": "information", "limit": limit, "offset": 0},
        )
        if not r.ok:
            LOG.warning("Could not list %s: %s", suffix, r.text[:200])
            continue
        data = r.json()
        items = data.get(suffix, data) if isinstance(data, dict) else data
        for item in (items or []):
            if not isinstance(item, dict):
                continue
            info = item.get("information", {})
            name = info.get("name")
            oid = info.get("objectId")
            if name and oid:
                all_metrics.append({"name": name, "objectId": oid, "endpoint": suffix})
    return all_metrics


def find_metric(metrics: list, metric_name: str) -> dict | None:
    for m in metrics:
        if m.get("name") == metric_name:
            return m
    return None


DEFAULT_MODEL_ID = "AC49E200F8E041C1AF38D87B99EF19DC"


def run(model_id: str = DEFAULT_MODEL_ID, metric_name: str = ""):
    if not metric_name:
        raise ValueError("--metric-name is required")

    env = _load_env()
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
        LOG.info("Resolving metric '%s' in model %s ...", metric_name, model_id)
        metrics = get_metrics(conn, model_id)
        metric = find_metric(metrics, metric_name)
        if not metric:
            LOG.error("Metric '%s' not found in model %s", metric_name, model_id)
            return sys.exit(1)
        metric_id = metric["objectId"]
        endpoint = metric["endpoint"]
        LOG.info("Found metric '%s' → %s (endpoint: %s)", metric_name, metric_id, endpoint)

        cs = create_changeset(conn, model_id)

        r = conn.delete(
            endpoint=f"/api/model/dataModels/{model_id}/{endpoint}/{metric_id}",
            headers=_h(conn, cs),
        )
        LOG.info("DELETE status: %s", r.status_code)
        if r.status_code >= 400:
            body = r.text[:500] if r.text else "(empty)"
            LOG.error("DELETE failed: %s", body)
            abort_changeset(conn, cs)
            return sys.exit(1)

        if not commit_changeset(conn, cs):
            abort_changeset(conn, cs)
            return sys.exit(1)

        after = get_metrics(conn, model_id)
        names = [m.get("name") for m in after]
        if metric_name not in names:
            LOG.info("Metric '%s' removed successfully.", metric_name)
        else:
            LOG.warning("Metric '%s' still present after commit — model may need publish.", metric_name)

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove a metric from a Mosaic Model")
    parser.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    parser.add_argument("--metric-name", required=True, help="Exact name of the metric to delete")
    args = parser.parse_args()
    run(args.model_id, args.metric_name)
