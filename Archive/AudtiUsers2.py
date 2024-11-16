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
base_url = "https://env-XXXX.customer.cloud.microstrategy.com/MicroStrategyLibrary/"
username = "mstr"
password = ""
project_id = "0DDDDEC8C94B320B4E93498C1EE98D18"  #Add any project ID - platform analytics is a good bet

# Connect to the MicroStrategy server - Not workstation
conn = Connection(base_url, username, password, project_id=project_id)

# List Privileges and return objects or display in DataFrame
Privilege.list_privileges(conn, to_dataframe=True, is_project_level_privilege='True')

def list_active_user_privileges(connection: "Connection") -> List[dict]:
    """List user privileges for all active users.

    Args:
        connection: MicroStrategy connection object returned by
            `connection.Connection()`

    Returns:
        list of dicts where each of them is in given form:
        {
            'id' - id of user
            'name' - name of user
            'username' - username of user
            'enabled' - if the user is active or not
            'privileges' - list of privileges of user
        }
    """
    group_value=''
    security_role=''
    project=''
    all_users = list_users(connection=connection)
    active_users = [u for u in all_users if u.enabled]
    privileges_list = []
    for usr in all_users:
        p = {
            'id': usr.id,
            'name': usr.name,
            'username': usr.username,
            'enabled': usr.enabled,
            'privileges': usr.privileges,
        }
        for prvlg in p['privileges']:
            for source in prvlg['sources']:
                if 'group' in source:
                    group_value = source['group']['name']
                if 'securityRole' in source:
                    security_role = source['securityRole']['name']
                if 'project' in source:
                    project = source['project']['name'] 
                p = [usr.id,usr.name,usr.username,usr.enabled,prvlg['privilege']['name'],group_value,security_role,project]            
                privileges_list.append(p)
    return privileges_list


# list of privileges for all users
print('Retrieving list of privileges for all users ')
list1=list_active_user_privileges(conn)
#print(list1)


# Define column names for Dataframe
columns = ['userid', 'userfullname','username','enabled','prvlg','groupname','secrole','project']

# Create a DataFrame based on list and column names
df = pd.DataFrame(list1, columns=columns)

# Display the DataFrame (optional)
#print(df)

#Close connection to MicroStrategy (Important or it will keep additional sessions open)
conn.close()

#Export to Excel (can be changed to CSV)
print('Exporting to excel - ** file will dump to current working directory with name: licenses.xlsx **')
df.to_excel('licenses.xlsx', sheet_name='Sheet1')
