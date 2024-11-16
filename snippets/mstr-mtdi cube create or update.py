from mstrio.connection import Connection
import getpass
from mstrio.project_objects.datasets import SuperCube, list_super_cubes
#
## Defining connection details
## YOU MUST CHANGE THIS TO YOUR ENVIRONMENT
## It uses getpass which is an interactive question for the password.
## comment this out and uncomment the other password if you want to set it to a standard password.
## Note: Authentication is standard or LDAP only for this approach.
#
base_url = 'https://env-XXXXXX.customer.cloud.microstrategy.com/MicroStrategyLibrary/'
username = ''

#base_url = 'https://env-<ENV ID>.customer.cloud.microstrategy.com/MicroStrategyLibrary/'
#username = '<YOUR USERNAME>'
#password = getpass.getpass(prompt='Password ')
password = ''
project_name = ''
date_string = "01-31-2020 14:45:37"
format_string = "%Y-%m-%d %H:%M:%S"

cube_upload_name=['VISUALIZATION CUBE']
cube_upload_name_str=''.join(cube_upload_name)


conn = Connection(base_url, username, password, project_name)

#connection = Connection(base_url, username, password,login_mode=1, project_name=project_name)
super_cubes_upload = list_super_cubes(conn,project_name=project_name)

super_cubes_upload = [x for x in super_cubes_upload if x in cube_upload_name]   
    

for i in super_cubes_upload:
    if i.name==cube_upload_name_str:
        print(f'cube found, updating data in cube: {i.name}')
        cube_upload_id = i.id
        ds = SuperCube(connection=conn, id=cube_upload_id)
        ds.add_table(name="data", data_frame=df, update_policy="replace")
        ds.update()

if len(super_cubes_upload) == 0:
    ds = SuperCube(connection=conn, name=cube_upload_name_str)
    ds.add_table(name="data", data_frame=df, update_policy="replace")
    ds.create()


conn.close()
