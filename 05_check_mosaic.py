import os
# Tested on Strategy One April 2026 release.

with open('.env') as f:
    for line in f:
        line=line.strip()
        if not line or line.startswith('#') or '=' not in line: continue
        k,v=line.split('=',1)
        os.environ[k]=v.strip('"').strip("'")

from mstrio import connection
conn = connection.Connection(
    base_url=os.getenv('MSTR_BASE_URL'),
    username=os.getenv('MSTR_USERNAME'),
    password=os.getenv('MSTR_PASSWORD'),
    project_name=os.getenv('MSTR_PROJECT_NAME','Shared Studio'),
    login_mode=int(os.getenv('MSTR_LOGIN_MODE','1')),
)

mosaic_id = 'AC49E200F8E041C1AF38D87B99EF19DC'

# Get the object definition
from mstrio.api.objects import get_object
resp = get_object(connection=conn, id=mosaic_id, type=3)
print("Status:", resp.status_code)
print(resp.text[:4000])

conn.close()
