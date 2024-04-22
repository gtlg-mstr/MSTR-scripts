#!/usr/bin/env python
# coding: utf-8

# In[38]:


print(
'''
*****
* Please note that this script was designed for testing and POC environments. By default it uses the Platform Analytics Project
* It selects all the privileges in the License Category and this may be *MORE* then you want to give.
* AI is removed and is its own Security Role
* 
* This creates 1x user for Consumer, Power, Report and Architect Roles
*   If you want to add Transaction or AI, add that security role to those users
* 
* This script will prompt for a password so that it is not stored.
*
* At the end is a commented section for REMOVING all the security roles and Users
*****
'''

)

# Privileges
from mstrio.access_and_security.privilege import Privilege,PrivilegeList
from mstrio.access_and_security.security_role import list_security_roles, SecurityRole
from mstrio.connection import Connection
from mstrio.users_and_groups import (list_user_groups, list_users, User, UserGroup)
import json
import pandas as pd
import getpass



# get connection to an environment
# ** YOU MUST PUT IN YOUR OWN DETAILS **

#base_url can only connect to Library
#login mode is in the connection variable
base_url = "https://env-324395.customer.cloud.microstrategy.com/MicroStrategyLibrary"
username = "mstr"
#password = ""
password = getpass.getpass(prompt='Password ')

#Default project - required for connection string. 
#Change it to a new value if need be
PROJECT_NAME = "Platform Analytics"


conn = Connection(base_url, username, password,login_mode=1,project_name=PROJECT_NAME)
#conn = get_connection(workstationData, project_name=PROJECT_NAME) #use this if you are using Workstation to reuse your connection

#** Initial Test            
# List Privileges and return objects or display in DataFrame
#privs = Privilege.list_privileges(conn)
#print(privs)
# for priv in privs:
#     print("|" + priv.id + "|" + priv.name + "|")

p1 = Privilege.list_privileges(conn)   
#print(p1)  #Debugging

#create empty lists for dumping to dataframe
pid=[]
pname=[]
pcat=[]
for p in p1:
    pid.append(p.id)
    pname.append(p.name)
    pcat.append(p.categories)  #critical for matching the license allocation

pcat = [str(item).strip('[]') for item in pcat]  #annoying characters in categories
pcat = [str(item).strip("'") for item in pcat]   #annoying characters in categories

#Used for debugging to check values - also use len to make sure they match
# print(pid)
# print(pname)
# print(pcat)



# Create DataFrame
df = pd.DataFrame({
    'priv_id':pid,
    'Priv_name':pname,
    'Priv_cat': pcat
    
}) 

#Used for debugging
#df.to_excel("MSTR_Priv.xlsx")

#print(df)

del pid,pname,pcat   #remove lists - mostly needed for initial testing

#** Set lists
#** Exclude list is AI currently
ExcludeList=['311','312','313']
CloudPower=['Client - Application - API','Client - Application - Jupyter','Client - Application - RStudio','Client - Application - Office','Client - Application - PowerBI','Client - Application - Qlik','Client - Application - Tableau','Client - Hyper - Mobile','Client - Hyper - Office','Client - Hyper - SDK','Client - Hyper - Web','Client - Mobile','Client - Reporter','Client - Web','Drivers - Big Data','Drivers - OLAP','Gateway - EMM','Server - Analytics','Server - Collaboration','Server - Distribution','Server - Geospatial','Server - Intelligence','Server - Reporter']
CloudConsumer=['Client - Application - API','Client - Application - Office','Client - Application - PowerBI','Client - Application - Qlik','Client - Application - Tableau','Client - Hyper - Mobile','Client - Hyper - Office','Client - Hyper - SDK','Client - Hyper - Web','Client - Mobile','Client - Reporter','Drivers - Big Data','Drivers - OLAP','Gateway - EMM','Server - Collaboration','Server - Distribution','Server - Geospatial','Server - Reporter']
CloudReporter=['Server - Reporter','Client - Reporter','Server - Distribution','Drivers - Big Data','Drivers - OLAP']
CloudTransaction=['Server - Transaction']

# Filter the DataFrame based on the License List and then exclude AI (Part of Server-reporter)
cloudpowerdf = df[df['Priv_cat'].isin(CloudPower)]
cloudpowerdf = cloudpowerdf[~cloudpowerdf['priv_id'].isin(ExcludeList)]
#cloudpowerdf.to_excel("MSTR_CloudP_Priv.xlsx")

cloudconsumerdf = df[df['Priv_cat'].isin(CloudConsumer)]
cloudconsumerdf = cloudconsumerdf[~cloudconsumerdf['Priv_cat'].isin(ExcludeList)]
#cloudconsumerdf.to_excel("MSTR_CloudC_Priv.xlsx")

cloudreporterdf = df[df['Priv_cat'].isin(CloudReporter)]
cloudreporterdf = cloudreporterdf[~cloudreporterdf['Priv_cat'].isin(ExcludeList)]
#cloudreporterdf.to_excel("MSTR_CloudR_Priv.xlsx")

cloudtransactiondf = df[df['Priv_cat'].isin(CloudTransaction)]
#cloudtransactiondf.to_excel("MSTR_CloudT_Priv.xlsx")

cloudAIdf = df[df['priv_id'].isin(ExcludeList)]
#print(cloudAIdf)
#cloudAIdf.to_excel("MSTR_CloudAI_Priv.xlsx")

# print(cloudpowerdf)
# print(cloudconsumerdf)
#cloudconsumerdf.info()

conn.close()


# In[39]:


conn = Connection(base_url, username, password,login_mode=1,project_name=PROJECT_NAME)
#Change to your values if you dont want the default
user_password = 'MicroStrategy'
user_cloudReporter = 'test_CloudReporter'
user_cloudConsumer = 'test_CloudConsumer'
user_cloudPower = 'test_CloudPower'
user_cloudArchitect = 'test_CloudArchitect'
User.create(connection=conn, username=user_cloudConsumer, full_name=user_cloudConsumer, password=user_password)
User.create(connection=conn, username=user_cloudReporter, full_name=user_cloudReporter, password=user_password)
User.create(connection=conn, username=user_cloudPower, full_name=user_cloudPower, password=user_password)
User.create(connection=conn, username=user_cloudArchitect, full_name=user_cloudArchitect, password=user_password)
conn.close()


# In[40]:


# from mstrio import config
# config.verbose = False
# #probably not a great idea, but during testing I dont always want those msgs.

#Change to your values if you dont want the default
role_consumer='CloudConsumer_Test_TBD'
role_power='CloudPowerUser_Test_TBD'
role_reporter='CloudReporterUser_Test_TBD'
role_transaction='CloudTransactionUser_Test_TBD'
role_ai='CloudAIUser_Test_TBD'
role_architect='CloudArchitect_Test_TBD'
role_description='TESTING - Delete when done'
#start with Cloud Consumer

#** SecurityRoles
#** Create new SecurityRole
conn = Connection(base_url, username, password,login_mode=1,project_name=PROJECT_NAME)

#start with Cloud Consumer
user = User(conn, username=user_cloudConsumer)
consumer_role = SecurityRole.create(
    conn,
    name=role_consumer,
    description=role_description,
    privileges=list(cloudconsumerdf['priv_id'])
)
consumer_role.grant_to(members=user, project=PROJECT_NAME)

#Cloud Power
user = User(conn, username=user_cloudPower)
power_role = SecurityRole.create(
    conn,
    name=role_power,
    description=role_description,
    privileges=list(cloudpowerdf['priv_id'])
)
power_role.grant_to(members=user, project=PROJECT_NAME)

user = User(conn, username=user_cloudReporter)
reporter_role = SecurityRole.create(
    conn,
    name=role_reporter,
    description=role_description,
    privileges=list(cloudreporterdf['priv_id'])
)
reporter_role.grant_to(members=user, project=PROJECT_NAME)

new_role = SecurityRole.create(
    conn,
    name=role_transaction,
    description=role_description,
    privileges=list(cloudtransactiondf['priv_id'])
)

new_role = SecurityRole.create(
    conn,
    name=role_ai,
    description=role_description,
    privileges=list(cloudAIdf['priv_id'])
)

user = User(conn, username=user_cloudArchitect)
architect_role = SecurityRole.create(
    conn,
    name=role_architect,
    description=role_description,
    privileges=list(df['priv_id'])
)
architect_role.grant_to(members=user, project=PROJECT_NAME)


## So surprisingly there is a situation where the everyone group
## has the collaboration assigned by the normal users role
## here the everyone group is removed from that role on the project you specifiy
role_normal="Normal Users"


userlist=[]
user = User(connection=conn, name=user_cloudReporter)
userlist.append(user.id)
user = User(connection=conn, name=user_cloudConsumer)
userlist.append(user.id)

print(userlist)

role=SecurityRole(connection=conn, name=role_normal)

user_group = UserGroup(connection=conn, name="Everyone")
#user_group.remove_users(users=user.id)

try:
  role.revoke_from(members=user_group, project=PROJECT_NAME)
except:
  print("Something went wrong removing that group. Typically this is because it was not a member. Check manually")


conn.close()


# In[41]:


# # ##REMOVE WHAT WAS BUILT after testing
# #1
# conn = Connection(base_url, username, password,login_mode=1,project_name=PROJECT_NAME)
# user_cloudConsumer = User(conn, username=user_cloudConsumer)
# consumer_role=SecurityRole(connection=conn, name=role_consumer)
# user_cloudConsumer.delete(force=True)
# consumer_role.delete(force=True)

# #2
# user_cloudPower = User(conn, username=user_cloudPower)
# power_role=SecurityRole(connection=conn, name=role_power)
# user_cloudPower.delete(force=True)
# power_role.delete(force=True)

# #3
# user_cloudReporter = User(conn, username=user_cloudReporter)
# reporter_role=SecurityRole(connection=conn, name=role_reporter)
# user_cloudReporter.delete(force=True)
# reporter_role.delete(force=True)

# #4
# user_cloudArchitect = User(conn, username=user_cloudArchitect)
# architect_role=SecurityRole(connection=conn, name=role_architect)
# user_cloudArchitect.delete(force=True)
# architect_role.delete(force=True)

# #5
# transaction_role=SecurityRole(connection=conn, name=role_transaction)
# transaction_role.delete(force=True)

# #6
# ai_role=SecurityRole(connection=conn, name=role_ai)
# ai_role.delete(force=True)

# conn.close()


# In[ ]:




