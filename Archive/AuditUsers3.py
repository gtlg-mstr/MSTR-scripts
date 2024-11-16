"""
Audit current license usage
---------------------------


"""
#Import required modules
from typing import List
from mstrio.connection import Connection, get_connection
from mstrio.users_and_groups import list_users
from mstrio.access_and_security.privilege import Privilege
import pandas as pd

# Define your MicroStrategy server connection details when connecting outside of Workstation
# This requires a project_id (newer versions can work off the name)
# Only works with Library (MSTRIO)
base_url = "https://env-XXXXX.customer.cloud.microstrategy.com/MicroStrategyLibrary/"
username = "mstr"
password = ""
project_id = "0DDDDEC8C94B320B4E93498C1EE98D18"  #Add any project ID - platform analytics is a good bet

# Connect to the MicroStrategy server - Not workstation
conn = Connection(base_url, username, password, project_name="Platform Analytics")

# List Privileges and return objects or display in DataFrame
p=Privilege.list_privileges(conn, to_dataframe=True, is_project_level_privilege='True')
print(p)



#Close connection to MicroStrategy (Important or it will keep additional sessions open)
conn.close()
