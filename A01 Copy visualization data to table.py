#!/usr/bin/env python
# coding: utf-8

'''
DEMO PURPOSES ONLY
This code is provided "AS IS" without warranty of any kind, either express or implied, including but not limited to the implied warranties of merchantability, fitness for a particular purpose, and non-infringement.
Limitations and Liability

In no event shall the author or copyright holder be liable for any claim, damages, or other liability arising from, out of, or in connection with the code or the use of this code, including but not limited to:
* Any errors or omissions in the code
* Any security vulnerabilities or breaches
* Any performance issues or degradation
* Any damages or losses resulting from the use of this software in production environments

Recommendations
* Use this code solely for demonstration, evaluation, and testing purposes.
* Thoroughly review and test the code in a non-production environment before deploying to production.
* Implement adequate security measures to protect against potential security vulnerabilities.
* Conduct regular performance monitoring and optimization as needed.

You acknowledge and agree that this code is not suitable for production use without significant modification, testing, and validation.
By using this code, you acknowledge that you have read, understood, and agreed to the terms and conditions outlined in this disclaimer.
'''
#
## MSTR VERSION: MicroStrategy 2021 Update 8+
## As of MicroStrategy One Update Sept 2024, the list_dossiers is still active
#


# ### Tools
# 1. Jupyter Notebook
# 2. Dossier in MSTR Web
# 3. MicroStrategy Library Swagger site for REST enpoints definitions

# ### Plan
# 1. Import Library
# 2. Connect to MSTR using `Connection` object
# 3. Create functions
# 4. List dossiers to find ID        -> get dossier ID
# 5. Create dossier instance         -> get instance ID
# 6. Get Dossier Instance definition -> get visualization ID
# 7. Import visualization data as a CSV file
# 8. Store data in pandas data frame


import pandas as pd
import csv
from io import StringIO
from mstrio.project_objects.dossier import list_dossiers, list_dossiers_across_projects
from mstrio.connection import Connection
import getpass
from mstrio.project_objects.datasets import SuperCube
import datetime
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text



#
## Defining connection details
## YOU MUST CHANGE THIS TO YOUR ENVIRONMENT
## It uses getpass which is an interactive question for the password.
## comment this out and uncomment the other password if you want to set it to a standard password.
## Note: Authentication is standard or LDAP only for this approach.
#
base_url = 'https://env-324395.customer.cloud.microstrategy.com/MicroStrategyLibrary/'
username = 'glagrange'

#base_url = 'https://env-<ENV ID>.customer.cloud.microstrategy.com/MicroStrategyLibrary/'
#username = '<YOUR USERNAME>'
#password = getpass.getpass(prompt='Password ')
password = '<YOUR MSTR PASSWORD>'
project_name = '<MSTR PROJECT>'
date_string = "01-31-2020 14:45:37"
format_string = "%Y-%m-%d %H:%M:%S"

tableNamefct = "<YOUR TABLE NAME>"

db_username='<YOUR DB USERNAME>'
db_password='<YOUR DB PASSWORD>'
db_server='<YOUR DB SERVER HOSTNAME OR IP>'
db_port='5432'  #postgresql std port
db='<YOUR DB>'
db_connectionstring=f'postgresql+psycopg2://{db_username}:{db_password}@{db_server}:{db_port}/{db}'


conn = Connection(base_url, username, password, project_name)

# Functions
def post_dossier_instance(connection, dossier_id):
    print('''Getting a Dashboard (Dossier) instance ID\nIf you get an error regarding MID--Check your Project name and Dashboard ID''')
    url_add=f"/api/dossiers/{dossier_id}/instances"
    res = connection.post(url=connection.base_url+url_add)
    return res

def get_dossier_instance_def(connection, dossierId, instanceId):
    print('''Using Dashboard definition to get the Visualization ID''')
    url_add=f"/api/v2/dossiers/{dossierId}/instances/{instanceId}/definition"
    res = connection.get(url=connection.base_url+url_add)
    return res

def csv_export_viz(connection, dossierId, instanceId, nodeKey):
    print('''Make sure your chapter,page and viz number is set''')
    url_add=f"/api/documents/{dossierId}/instances/{instanceId}/visualizations/{nodeKey}/csv"
    res = connection.post(url=connection.base_url+url_add)
    return res

# lambda: conn, dossierid: 
list_dossiers(conn)
dossier_id="9848C811574777DADB287686F8B770B7"

resp_instance=post_dossier_instance(conn, dossier_id)
#print(resp_instance.json())
instance_id=resp_instance.json()['mid']
#print(instance_id)

viz_id=get_dossier_instance_def(conn, dossier_id, instance_id).json()['chapters'][0]['pages'][0]['visualizations'][0]['key']

resp_viz = csv_export_viz(conn, dossier_id, instance_id, viz_id)
result = str(resp_viz.content, 'utf-16')
df = pd.read_csv(StringIO(result))


engine = create_engine(db_connectionstring)

#This will replace the entire table. If you dont want this change the 'if_exists' option
df.to_sql(name=tableNamefct, con=engine, if_exists='replace',index=False)


#Change of how sqlalchemy does the connection from 1.4 to 2.x
with engine.connect() as db_conn:
    print(f'{datetime.now()} - Checking uploaded rows')
    result=db_conn.execute(text("select count(*) from "+tableNamefct+";"))
    for row in result:
        print(str(row) + " Rows found")


conn.close()

