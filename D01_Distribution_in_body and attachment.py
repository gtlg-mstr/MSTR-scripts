#!/usr/bin/env python
# coding: utf-8

# get connection to an environment
# ** YOU MUST PUT IN YOUR OWN DETAILS **
from datetime import date, time, datetime
import csv
import pandas as pd
from io import StringIO
from mstrio.project_objects.dossier import list_dossiers, list_dossiers_across_projects
from mstrio.project_objects import Report, Prompt, list_reports
from mstrio.connection import Connection
from redmail import gmail
from pretty_html_table import build_table
#base_url can only connect to Library
#login mode is in the connection variable
base_url = "https://tutorial.microstrategy.com/MicroStrategyLibraryInsights/"
username = ""
password = ""
#password = getpass.getpass(prompt='Password ')

#Default project - this is used for storing the final cube.
#Change it to a new value if need be
project_name = "MicroStrategy One U12 Tutorial"

#Initial connection used for getting a list of all projects
conn = Connection(base_url, username, password,login_mode=16, project_name=project_name)
#conn.close()


# # Functions
# def post_dossier_instance(connection, dossier_id):
#     url_add=f"/api/dossiers/{dossier_id}/instances"
#     res = connection.post(url=connection.base_url+url_add)
#     return res
# 
# def get_dossier_instance_def(connection, dossierId, instanceId):
#     url_add=f"/api/v2/dossiers/{dossierId}/instances/{instanceId}/definition"
#     res = connection.get(url=connection.base_url+url_add)
#     return res
# 
# def csv_export_viz(connection, dossierId, instanceId, nodeKey):
#     url_add=f"/api/documents/{dossierId}/instances/{instanceId}/visualizations/{nodeKey}/csv"
#     res = connection.post(url=connection.base_url+url_add)
#     return res
# 
# #lambda: conn, dossierid: 
# list_dossiers(conn)
# dossier_id="1620DB120B414E017F150B911E93F205"
# 
# resp_instance=post_dossier_instance(conn, dossier_id)
# print(resp_instance.json())
# instance_id=resp_instance.json()['mid']
# print(instance_id)
# 
# viz_id=get_dossier_instance_def(conn, dossier_id, instance_id).json()['chapters'][0]['pages'][0]['visualizations'][0]['key']
# print(viz_id)
# resp_viz = csv_export_viz(conn, dossier_id, instance_id, viz_id)
# result = str(resp_viz.content, 'utf-16')
# df = pd.read_csv(StringIO(result))

# In[2]:


# Check available attributes and metrics of a Report
sample_report_id = '3D9614C24DCC1F52624FB68C62DDB1FB'
sample_report = Report(conn, id=sample_report_id)
# Create a dataframe from a Report
dataframe = sample_report.to_dataframe()
dataframe_sample=dataframe.head(10)
print(dataframe_sample)

#print(df)
# Convert DataFrame to HTML
html_table = dataframe_sample.to_html(index=False, escape=False)
#print(html_table)


format_string = "%Y-%m-%d %H:%M:%S"
x = datetime.now()
print(x.strftime(format_string))
gmail.user_name = 'glagrange@gmail.com'
gmail.password = ''

# Let Red Mail to render the dataframe for you:
gmail.use_jinja = False
gmail.send(
    subject=f'Report Test 1 - {x}',
    receivers=['glagrange@microstrategy.com'],
    html='''
    <img src="https://i.imgur.com/XnlZCvC.jpg"><br>
    <span><a href="https://tutorial.microstrategy.com/MicroStrategyLibrary/app/9105B57627456A6AB4109C9D76E19764/1620DB120B414E017F150B911E93F205/W653CE7F2406641E5AD01F7F136386A83--K46/edit" target="_blank">
    Click here to open Report in MicroStrategy</a></span>
    <h1>Review Preview (10 Rows)</h1> {0}<br>

    <br>
    
    '''.format(build_table(dataframe_sample, 'blue_light')),
    body_tables={
        'mytable': dataframe_sample,
    },
    attachments={
        'myfile.xlsx': dataframe,
    }
)


conn.close()

