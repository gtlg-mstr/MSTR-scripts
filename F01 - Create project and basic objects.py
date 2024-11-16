#!/usr/bin/env python
# coding: utf-8

# # Example of importing data from Cube or Report to dataframe and save them back to SuperCube
# ### Defining connection details
import getpass
from mstrio.connection import Connection
from mstrio.server import Environment, Project
import pandas as pd
from time import sleep
from datetime import date, time, datetime
import json
from mstrio.modeling.schema.table import (
    list_logical_tables,
    list_physical_tables,
    list_tables_prefixes,
    list_warehouse_tables,
    LogicalTable,
    PhysicalTable,
)
from mstrio.modeling import DataType, SchemaManagement, SchemaUpdateType


base_url = 'https://env-XXXXX.customer.cloud.microstrategy.com/MicroStrategyLibrary/'
username = 'mstr'
password = ''
project_name = 'MicroStrategy Tutorial'
date_string = "01-31-2020 14:45:37"
format_string = "%Y-%m-%d %H:%M:%S"

NEW_PROJECT='Workshop3'
NEW_PRJ_DESC='This is a python created project'

TABLESREQ=['lu_item','lu_subcateg','lu_brand','lu_category','item_mnth_sls']

conn = Connection(base_url, username, password, project_name)

env = Environment(connection=conn)

# get list of all projects or those just loaded
all_projects = env.list_projects()
loaded_projects = env.list_loaded_projects()

print(all_projects)
print('#####')
print(loaded_projects)

x = datetime.now()
print(x.strftime(format_string))

# create a project and store it in variable to have immediate access to it
new_project = env.create_project(name=NEW_PROJECT, description=NEW_PRJ_DESC)

y = datetime.now()
#print(y.strftime(format_string))

timediff = y-x
total_seconds = timediff.total_seconds()
print(f"Project build time: {total_seconds}")

x = datetime.now()
print(x.strftime(format_string))
print(f'Switching to new project: {NEW_PROJECT}')
sleep(1)
conn.close()

conn = Connection(base_url, username, password, NEW_PROJECT)

sleep(5)
DATASOURCE_ID='A23BBC514D336D5B4FCE919FE19661A3'    
x = datetime.now()
print(x.strftime(format_string))
for z in TABLESREQ:
    print(f'adding table {z}')
    z_table= z+'_table'
    whtable = list_warehouse_tables(connection=conn, name=z, datasource_id=DATASOURCE_ID)[0]
    logical_table = whtable.add_to_project(logical_table_name=z)

    print(logical_table)
    print(logical_table.physical_table)
print('Completed adding tables')
y = datetime.now()
timediff = y-x
total_seconds = timediff.total_seconds()
print(f"Time taken to add to project: {total_seconds}")


# Any changes to a schema objects must be followed by schema_reload
# in order to use them in reports, dashboards and so on
schema_manager = SchemaManagement(connection=conn)
task = schema_manager.reload(update_types=[SchemaUpdateType.LOGICAL_SIZE])


# physical_tables = list_physical_tables(conn)
# print(physical_tables)
# for a in logical_tables:
#     print (a.id)
#     physical_table = PhysicalTable(connection=conn, id=a.id)
#     # Delete a physical table.
#     physical_table.delete(force=True)



# # delete a project
# #first connect to a different project
# conn.close()
# conn = Connection(base_url, username, password, project_name)

# # get project with a given name
# del_project = Project(connection=conn, name=NEW_PROJECT)

# # first, the project need to be unloaded
# del_project.unload()
# sleep(10)
# # then delete the unloaded project,
# # confirmation prompt will appear asking for a project name
# del_project.delete()

conn.close()

