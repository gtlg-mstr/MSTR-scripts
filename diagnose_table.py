
# Tested on Strategy One April 2026 release.

import requests, json, os
from pathlib import Path

env_path = Path("/home/support/dev-projects/Scripts/.env")
env = {}
with open(env_path, "rb") as f:
    for line in f:
        line = line.decode().strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        env[k] = v.strip('\"').strip("'")

base = env.get("MSTR_BASE_URL")
username = env["MSTR_USERNAME"]
password = env["MSTR_PASSWORD"]

r = requests.post(f"{base}/api/auth/login", json={"username": username, "password": password, "loginMode": 1})
token = r.headers["X-Mstr-Authtoken"]
cookies = r.cookies

# Find table ID
r = requests.get(f"{base}/api/model/dataModels/AC49E200F8E041C1AF38D87B99EF19DC/tables", headers={"X-Mstr-AuthToken": token}, cookies=cookies)
tables = r.json().get("tables", r.json())
table = next((t for t in tables if t.get("information", {}).get("name") == "GRASP_flight_trips"), None)
tid = table["information"]["objectId"]

# Get detailed table
r = requests.get(f"{base}/api/model/dataModels/AC49E200F8E041C1AF38D87B99EF19DC/tables/{tid}", headers={"X-Mstr-AuthToken": token}, cookies=cookies)
print(json.dumps(r.json(), indent=2))
