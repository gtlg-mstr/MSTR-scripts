#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
from mstrio.connection import get_connection,Connection
from mstrio.project_objects.datasets.cube import CubeStates, list_all_cubes
from mstrio.project_objects.datasets import OlapCube, SuperCube

#base_url can only connect to Library
#login mode is in the connection variable
base_url = "https://tutorial.microstrategy.com/MicroStrategyLibraryInsights/"
username = "glagrange"
password = $variable_name

#Default project - this is used for storing the final cube.
#Change it to a new value if need be
project_name = "MicroStrategy One U12 Tutorial"

# this method uses the inherent workstation connection
#conn = get_connection(workstationData, project_name)
# alternatively, use the rest API approach
# Uncomment if you want to use this alternatively, use the workstation approach
conn = Connection(base_url, username, password,login_mode=16, project_name=project_name)

# list of cubes to publish, IDs are better than names
#cubes_to_publish = ['082E2B4A4D65C28B998A5785431B8729','2603E4545F4DF8C5B183FBBD33ABE9DA']

cubes_to_publish_names = ['Intelligent Cube - Inventory','New Dataset']
cube_list_all = list_all_cubes(conn,project_name=project_name)
cubes_to_publish = [x for x in cube_list_all if x in cubes_to_publish_names]
print(f'{cubes_to_publish}')


for x in cubes_to_publish:
    try:
        example_cube = OlapCube(connection=conn, id=x.id)  
        example_cube.publish()
        # Wait for cube refresh to complete successfully.
        example_cube.refresh_status()
        while "Processing" in CubeStates.show_status(example_cube.status):
            time.sleep(1)
            example_cube.refresh_status()
    except Exception as e:
        print(f"**ERROR**An unexpected error occurred: {e}")

