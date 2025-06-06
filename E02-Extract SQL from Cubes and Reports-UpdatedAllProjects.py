#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8
import re #regex functions
import json
import pandas as pd
import datetime
import getpass
from mstrio.connection import Connection
from mstrio.project_objects.datasets.olap_cube import OlapCube, list_olap_cubes
from mstrio.project_objects.datasets.super_cube import SuperCube, list_super_cubes
from mstrio.project_objects.report import Report,list_reports
from mstrio.server import Environment, Project

# get connection to an environment
# ** YOU MUST PUT IN YOUR OWN DETAILS **

#base_url can only connect to Library
#login mode is in the connection variable
base_url = "https://env-349302.customer.cloud.microstrategy.com/MicroStrategyLibrary"
username = "mstr"
password = getpass.getpass(prompt='Password ')
#password =''

#Default project - this is used for storing the final cube.
#Change it to a new value if need be
project_name = "Platform Analytics"

#Initial connection used for getting a list of all projects
connection = Connection(base_url, username, password,login_mode=1)
env = Environment(connection=connection)

#Filter project list - use this list (comma seperated) with the project names that you want to filter for.
#For example, if you only want 2 projects it would be something like this:
#['MicroStrategy Tutorial','Platform Analytics']
projectlist=['MicroStrategy Tutorial','Gareth','Platform Analytics']
#projectlist=['CPM Reporting (Prod)','Internal Analytics (Prod)','Bounty Reporting (Prod)']

#track when last change occurred
rundt = datetime.datetime.now()
print(f'Datetime of collection:{rundt}')


# In[2]:


# get list of all projects or those just loaded, default is only loaded projects
#all_projects = env.list_projects()
loaded_projects = env.list_loaded_projects()
print (loaded_projects)

#used for debugging
#print(len(projectlist))
#print(type(loaded_projects))


# In[3]:


# Functions
# REST API functions for those that are not directly accessible by python module MSTRIO
#Note, not available in 11.3.4.x
def post_report_instance(connection, reportId):
    url_add=f"/api/model/reports/{reportId}/instances?executionStage=execute_data"
    res = connection.post(url=connection.base_url+url_add)
    res=res.json()
    #print(res)
    return res

def get_report_sql(connection, reportId, instanceId):
    url_add=f"/api/v2/reports/{reportId}/instances/{instanceId}/sqlView"
    res = connection.get(url=connection.base_url+url_add)
    return res.text


# In[4]:


#loop through all projects and filter if the filter is set.
if len(projectlist) > 0:
    loaded_projects = [x for x in loaded_projects if x in projectlist]
    print(loaded_projects)


# In[5]:


#connection = Connection(base_url, username, password,login_mode=1, project_name=project_name)
#Set the list variables that will be used to populate the dataframe
projectid=[]
projectname=[]
cubetype=[]
cubeid=[]
cubename=[]
cubesql=[]
status=[]
table_names=[]
join_table_names=[]
date_run=[]
location=[]

#Create a list of cubes,cube types and projects
#This will be used later for the SQL retrieval.
for i in loaded_projects:
    try:
        olap_cubes_= list_olap_cubes(connection, project_name=i.name)
        for x in olap_cubes_:
            projectid.append(i.id)
            projectname.append(i.name)
            cubetype.append("OlapCube")
            cubeid.append(x.id)
            cubename.append(x.name)
            date_run.append(rundt)
            
        super_cubes_ = list_super_cubes(connection,project_name=i.name)
        for x in super_cubes_:
            projectid.append(i.id)
            projectname.append(i.name)
            cubetype.append("SuperCube")
            cubeid.append(x.id)
            cubename.append(x.name)
            date_run.append(rundt)
        
    except Exception as e:
        #status.append(e)
        location.append('')
     

#print(projectid,projectname,cubeid,cubename,cubetype)
connection.close()


# In[6]:


#using the 3 lists above, loop through the index
#it creates index based variables based on the 3 lists
#Based on the cubetype, it adjusts to the correct module call
#regex used to find table names and joins
n = len(cubeid)
for i in range(n):
    projectid_=projectid[i]
    cubeid_=cubeid[i]
    cubetype_=cubetype[i]
    try:
        if cubetype_ == "OlapCube":
            connection = Connection(base_url, username, password,login_mode=1,project_id=projectid_)
            t=OlapCube(connection=connection,id=cubeid_)
            t=t.export_sql_view()
            cubesql.append(t)
            status.append("success")
            matches = re.findall(r'FROM\s+(\w.+|\"\w.+)', t, re.IGNORECASE)
            table_names.append(matches)
            matchesjoins = re.findall(r'join\s+(\w.+|\"\w.+)', t, re.IGNORECASE)
            join_table_names.append(matchesjoins)
            connection.close()
        if cubetype_ == "SuperCube":
            connection = Connection(base_url, username, password,login_mode=1,project_id=projectid_)
            t=SuperCube(connection=connection,id=cubeid_)
            t=t.export_sql_view()
            cubesql.append(t)
            status.append("success")
            matches = re.findall(r'FROM\s+(\w.+|\"\w.+)', t, re.IGNORECASE)
            table_names.append(matches)
            matchesjoins = re.findall(r'join\s+(\w.+|\"\w.+)', t, re.IGNORECASE)
            join_table_names.append(matchesjoins)
            connection.close()
    except Exception as e:
        cubesql.append(e)
        status.append('error')
        table_names.append('')
        join_table_names.append('')
        connection.close()


# In[7]:


#debugging
#print(join_table_names)
print(len(projectid))
print(len(projectname))
print(len(cubetype))
print(len(cubeid))
print(len(cubename))
print(len(cubesql))
print(len(status))
print(len(table_names))
print(len(join_table_names))


# In[8]:


# Create DataFrame
df = pd.DataFrame({
    'projectid':projectid,
    'projectname':projectname,
    'cubetype': cubetype,
    'cubeid': cubeid,
    'cubename':cubename,
    'cubesql':cubesql,
    'retrieve_status': status,
    'tables': table_names,
    'joins': join_table_names,
    'date_run':date_run
    
}) 

#Add row number and fix nested lists for table and joins
df['row_number'] = df.index + 1
df['tables'] = df['tables'].apply(lambda x: ', '.join(x))
df['tables'] = df['tables'].str.replace(",","\n")
df['joins'] = df['joins'].apply(lambda x: ', '.join(x))
df['joins'] = df['joins'].str.replace(",","\n")

print(df)


# In[25]:


import sys
sys.setrecursionlimit(10000)
'''
Critical
---------
Establish if cube exists. If it does, do not recreate just publish new data
If it does not, create the cube automatically.

'''
cube_upload_name=['Tables Used by Cube']
cube_upload_name_str=''.join(cube_upload_name)

connection = Connection(base_url, username, password,login_mode=1, project_name=project_name)
print(base_url)
super_cubes_upload = list_super_cubes(connection,project_name=project_name)
super_cubes_upload = [x for x in super_cubes_upload if x in cube_upload_name] 

print (super_cubes_upload)


for x in super_cubes_upload:
    cube_upload_id = i.id
    ds = SuperCube(connection=connection, id=cube_upload_id).list_properties()
    print(ds)
    

    
#print(super_cubes_upload)
#print(cube_upload_name_str)
for i in super_cubes_upload:
    if i.name==cube_upload_name_str:
        print(f'cube found in {project_name}, updating data in cube: {i.name}')
        cube_upload_id = i.id
        ds = SuperCube(connection=connection, id=cube_upload_id)
        ds.add_table(name="data", data_frame=df, update_policy="replace")
        ds.update()

if len(super_cubes_upload) == 0:
    ds = SuperCube(connection=connection, name=cube_upload_name_str, description='Table and SQL information from Python script')
    ds.add_table(name="data", data_frame=df, update_policy="replace")
    ds.create(folder_id='4A4EA73BCB42A90245E893B0B3FAEA8C')
#ds.update()
#F6F16BA5B7488F53FC237E9B4528D01E


# In[10]:


#close all connections to library
connection.close()


# In[13]:


##OPTIONAL - but good for testing.
# Export DataFrame to Excel
# df.to_excel('output.xlsx', index=False)  # Set index=False to exclude the index column
# display(df)

