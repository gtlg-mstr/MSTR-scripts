
# Tested on Strategy One April 2026 release.

import json
from pathlib import Path
from mstrio.connection import Connection

def _load_env() -> dict:
    """Loads environment variables from the standard .env file."""
    env_path = Path("/home/support/dev-projects/Scripts/.env")
    env = {}
    if not env_path.exists():
        raise FileNotFoundError(f"Could not find .env file at: {env_path}")
    with open(env_path, "rb") as f:
        for line in f:
            line = line.decode().strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k] = v.strip('"').strip("'")
    return env

def main():
    """Fetches and prints the definition of a specific table in a model."""
    env = _load_env()
    conn = Connection(
        base_url=env["MSTR_BASE_URL"],
        username=env["MSTR_USERNAME"],
        password=env["MSTR_PASSWORD"],
        project_name=env.get("MSTR_PROJECT_NAME", "Shared Studio"),
        login_mode=int(env.get("MSTR_LOGIN_MODE", "1")),
    )

    model_id = "AC49E200F8E041C1AF38D87B99EF19DC"
    # Find table ID
    r = conn.get(endpoint=f"/api/model/dataModels/{model_id}/tables")
    tables = r.json().get("tables", r.json())
    table = next((t for t in tables if t.get("information", {}).get("name") == "GRASP_flight_trips"), None)
    tid = table["information"]["objectId"]

    # Get detailed table definition
    r = conn.get(endpoint=f"/api/model/dataModels/{model_id}/tables/{tid}")
    print(json.dumps(r.json(), indent=2))
    conn.close()

if __name__ == "__main__":
    main()
