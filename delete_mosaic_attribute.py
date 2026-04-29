#!/usr/bin/env python3
# Tested on Strategy One April 2026 release.

"""
Remove an Attribute from a MicroStrategy Mosaic Model/Changeset (Strategy One Cloud).
Requires an existing attribute name; resolves its objectId, DELETEs via changeset, commits.

Usage:
    python delete_mosaic_attribute.py --model-id AC49E200F8E041C1AF38D87B99EF19DC \
        --attr-name "GRASP Ex"

Note: Underlying warehouse columns in physicalTable.pipeline are *not* removed
automatically; Studio Web UI typically cleans those on next table refresh/publish.
"""

import os, sys, json, argparse, logging, requests
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger("delete_mosaic_attribute")

# ── credentials ─────────────────────────────────────────────────────────
def load_env(dotenv: Path = Path(".env")) -> dict:
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


# ── authentication ──────────────────────────────────────────────────────
def login(env: dict, base: str, username: str, password: str) -> tuple:
    r = requests.post(
        f"{base}/api/auth/login",
        json={"username": username, "password": password, "loginMode": 1},
        timeout=30,
    )
    r.raise_for_status()
    token = r.headers["X-Mstr-Authtoken"]
    cookies = r.cookies
    project_name = env.get("MSTR_PROJECT_NAME", "Shared Studio")
    r2 = requests.get(
        f"{base}/api/projects",
        headers={"X-Mstr-AuthToken": token},
        cookies=cookies,
        timeout=30,
    )
    r2.raise_for_status()
    rj = r2.json()
    if isinstance(rj, list):
        pid = next(
            (p["id"] for p in rj if p.get("name") == project_name),
            None,
        )
    elif isinstance(rj, dict):
        pid = next(
            (p["id"] for p in rj.get("projects", []) if p.get("name") == project_name),
            None,
        )
    else:
        pid = None
    if not pid:
        raise RuntimeError(f"Project '{project_name}' not found")
    LOG.info("Authenticated to project %s (%s)", project_name, pid)
    return token, cookies, pid


def logout(base: str, token: str, cookies):
    requests.post(
        f"{base}/api/auth/logout",
        headers={"X-Mstr-AuthToken": token},
        cookies=cookies,
        timeout=10,
    )


# ── changeset helpers ──────────────────────────────────────────────────
def model_headers(token: str, pid: str, changeset: str = None) -> dict:
    h = {
        "X-Mstr-AuthToken": token,
        "X-MSTR-ProjectID": pid,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if changeset:
        h["x-mstr-ms-changeset"] = changeset
    return h


def create_changeset(base: str, token: str, cookies, pid: str, model_id: str) -> str:
    r = requests.post(
        f"{base}/api/model/changesets?enableOperationHistory=true",
        headers=model_headers(token, pid),
        cookies=cookies,
        timeout=30,
    )
    r.raise_for_status()
    cs = r.json()["id"]
    LOG.info("Created changeset %s", cs)
    rebase = requests.post(
        f"{base}/api/model/changesets/{cs}/operations",
        headers=model_headers(token, pid),
        cookies=cookies,
        params={"operationType": "rebase", "dataModelId": model_id},
        timeout=30,
    )
    if not rebase.ok:
        LOG.warning("Rebase warning: %s %s", rebase.status_code, rebase.text[:200])
    return cs


def abort_changeset(base: str, token: str, cookies, pid: str, cs: str):
    r = requests.post(
        f"{base}/api/model/changesets/{cs}/abort",
        headers=model_headers(token, pid),
        cookies=cookies,
        timeout=120,
    )
    LOG.info("Aborted changeset %s (status=%s)", cs, r.status_code)


def commit_changeset(base: str, token: str, cookies, pid: str, cs: str) -> bool:
    r = requests.post(
        f"{base}/api/model/changesets/{cs}/commit",
        headers=model_headers(token, pid),
        cookies=cookies,
        timeout=120,
    )
    if not r.ok:
        LOG.error("Commit failed: %s %s", r.status_code, r.text[:500])
        return False
    LOG.info("Changeset committed successfully")
    return True


# ── attribute discovery ───────────────────────────────────────────────
def get_attributes(base: str, token: str, cookies, pid: str, model_id: str, limit: int = 100) -> list:
    r = requests.get(
        f"{base}/api/model/dataModels/{model_id}/attributes",
        headers=model_headers(token, pid),
        cookies=cookies,
        params={"fields": "information", "limit": limit, "offset": 0},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("attributes", [])


def find_attribute_id(attributes: list, attr_name: str) -> str:
    for a in attributes:
        info = a.get("information", {})
        if info.get("name") == attr_name:
            return info.get("objectId")
    return None


# ── main ───────────────────────────────────────────────────────────────
DEFAULT_MODEL_ID = "AC49E200F8E041C1AF38D87B99EF19DC"


def run(model_id: str = DEFAULT_MODEL_ID, attr_name: str = ""):
    if not attr_name:
        raise ValueError("--attr-name is required")

    env = load_env()
    base = env.get("MSTR_BASE_URL", "https://studio.strategy.com/MicroStrategyLibrary")
    user = env["MSTR_USERNAME"]
    pwd = env["MSTR_PASSWORD"]
    token, cookies, pid = login(env, base, user, pwd)

    try:
        # 1. resolve attribute ID
        LOG.info("Resolving attribute '%s' in model %s ...", attr_name, model_id)
        attributes = get_attributes(base, token, cookies, pid, model_id)
        attr_id = find_attribute_id(attributes, attr_name)
        if not attr_id:
            LOG.error("Attribute '%s' not found in model %s", attr_name, model_id)
            return sys.exit(1)
        LOG.info("Found attribute '%s' → %s", attr_name, attr_id)

        # 2. create changeset
        cs = create_changeset(base, token, cookies, pid, model_id)

        # 3. DELETE attribute (direct, no batch needed)
        url = f"{base}/api/model/dataModels/{model_id}/attributes/{attr_id}"
        del_r = requests.delete(
            url,
            headers=model_headers(token, pid, cs),
            cookies=cookies,
            timeout=30,
        )
        LOG.info("DELETE status: %s", del_r.status_code)
        if del_r.status_code >= 400:
            body = del_r.text[:500] if del_r.text else "(empty)"
            LOG.error("DELETE failed: %s", body)
            abort_changeset(base, token, cookies, pid, cs)
            return sys.exit(1)

        # 4. commit
        if not commit_changeset(base, token, cookies, pid, cs):
            abort_changeset(base, token, cookies, pid, cs)
            return sys.exit(1)

        # 5. verify
        after = get_attributes(base, token, cookies, pid, model_id)
        names = [a.get("information", {}).get("name") for a in after]
        if attr_name not in names:
            LOG.info("✅ Attribute '%s' removed successfully.", attr_name)
        else:
            LOG.warning("Attribute '%s' still present after commit — model may need publish.", attr_name)

    finally:
        logout(base, token, cookies)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove an attribute from a Mosaic Model")
    parser.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    parser.add_argument("--attr-name", required=True, help="Exact name of the attribute to delete")
    args = parser.parse_args()
    run(args.model_id, args.attr_name)
