#!/usr/bin/env python3
# Tested on Strategy One April 2026 release.

"""
Remove an Attribute from a MicroStrategy Mosaic Model/Changeset (Strategy One Cloud).

Uses mstrio-py for connection and authentication.
Resolves by name, DELETEs via changeset, commits, and verifies removal.

Usage:
    python 08_delete_mosaic_attribute.py --model-id AC49E200F8E041C1AF38D87B99EF19DC \\
        --attr-name "GRASP Ex"

Note: Underlying warehouse columns in physicalTable.pipeline are *not* removed
automatically; Studio Web UI typically cleans those on next table refresh/publish.
"""

import sys, json, argparse, logging
from pathlib import Path

from mstrio.connection import Connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger("delete_mosaic_attribute")


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
    """Build request headers with ProjectID and optional changeset."""
    h = {"X-MSTR-ProjectID": conn.project_id}
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


def abort_changeset(conn: Connection, cs: str):
    r = conn.post(
        endpoint=f"/api/model/changesets/{cs}/abort",
        headers=_h(conn),
    )
    LOG.info("Aborted changeset %s (status=%s)", cs, r.status_code)


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


def get_attributes(conn: Connection, model_id: str, limit: int = 100) -> list:
    r = conn.get(
        endpoint=f"/api/model/dataModels/{model_id}/attributes",
        headers=_h(conn),
        params={"fields": "information", "limit": limit, "offset": 0},
    )
    r.raise_for_status()
    return r.json().get("attributes", [])


def find_attribute_id(attributes: list, attr_name: str) -> str:
    for a in attributes:
        info = a.get("information", {})
        if info.get("name") == attr_name:
            return info.get("objectId")
    return None


DEFAULT_MODEL_ID = "AC49E200F8E041C1AF38D87B99EF19DC"


def run(model_id: str = DEFAULT_MODEL_ID, attr_name: str = ""):
    if not attr_name:
        raise ValueError("--attr-name is required")

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
        LOG.info("Resolving attribute '%s' in model %s ...", attr_name, model_id)
        attributes = get_attributes(conn, model_id)
        attr_id = find_attribute_id(attributes, attr_name)
        if not attr_id:
            LOG.error("Attribute '%s' not found in model %s", attr_name, model_id)
            return sys.exit(1)
        LOG.info("Found attribute '%s' → %s", attr_name, attr_id)

        cs = create_changeset(conn, model_id)

        r = conn.delete(
            endpoint=f"/api/model/dataModels/{model_id}/attributes/{attr_id}",
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

        after = get_attributes(conn, model_id)
        names = [a.get("information", {}).get("name") for a in after]
        if attr_name not in names:
            LOG.info("✅ Attribute '%s' removed successfully.", attr_name)
        else:
            LOG.warning("Attribute '%s' still present after commit — model may need publish.", attr_name)

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove an attribute from a Mosaic Model")
    parser.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    parser.add_argument("--attr-name", required=True, help="Exact name of the attribute to delete")
    args = parser.parse_args()
    run(args.model_id, args.attr_name)
