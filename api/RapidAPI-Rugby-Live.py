#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


'''
This is the section that calls the remote API and returns the results
results are currently in json format. You may need to change this if its not.
THIS IS FOR TABLE 1
'''
import requests

url = "https://rugby-live-data.p.rapidapi.com/fixtures/1272/2024"

headers = {
	"X-RapidAPI-Key": "",  ##KEY REMOVED
	"X-RapidAPI-Host": "rugby-live-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

print(response.json())


# In[3]:


'''
Importing the json to a pandas dataframe
This is how MicroStrategy likes the data formatted for importing to cubes
'''
import pandas as pd

# Load the JSON data
json_data = response.json()

# Create a Pandas DataFrame from the JSON data
df = pd.DataFrame(json_data["results"])

# Print the DataFrame
print(df)


# In[4]:


'''
This is the section that calls the remote API and returns the results
results are currently in json format. You may need to change this if its not.
THIS IS FOR TABLE 2
'''
import requests

url = "https://rugby-live-data.p.rapidapi.com/standings/1266/2024"

headers = {
	"X-RapidAPI-Key": "", ##KEY REMOVED
	"X-RapidAPI-Host": "rugby-live-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

print(response.json())
json_data=response.json()

# Get the standings data
standings_data = json_data["results"]["standings"]

# Convert the standings data to a DataFrame
df_standings = pd.DataFrame(standings_data)

# Print the DataFrame
print(df_standings.to_string())


# In[5]:


'''Create an excel backup - for testing - this can be commented out if you dont want it'''
# create a excel writer object
with pd.ExcelWriter("6n24_results.xlsx") as writer:
   
    # use to_excel function and specify the sheet_name and index
    # to store the dataframe in specified sheet
    df.to_excel(writer, sheet_name="results", index=False)
    df_standings.to_excel(writer, sheet_name="standings", index=False)


# In[6]:


'''
MicroStrategy specific:
Import the 2 dataframes into 1 MTDI (Supercube) cube

The code will check if the cube name exists (by default in the executing users my reports folder)
If it does not exist, it creates it
If it exists, it currently replaces the tables. If you want to change that, then change the word "replace" with "append"
'''
from mstrio.connection import Connection
import getpass
from mstrio.project_objects.datasets import SuperCube,list_super_cubes


mstr_username = "glagrange"
#mstr_password = getpass.getpass(prompt='Password ')
mstr_password = ""  ##Password REMOVED
mstr_base_url = "https://env-324395.customer.cloud.microstrategy.com"
mstr_url_api  = mstr_base_url+"/MicroStrategyLibrary/api"
mstr_project  = "Gareth"

connection=Connection(mstr_url_api, mstr_username, mstr_password, login_mode=1, project_name=mstr_project)

cube_upload_name=['6nations_tbd']
cube_upload_name_str=''.join(cube_upload_name)  #need a string version because...


super_cubes_upload = list_super_cubes(connection,project_name=mstr_project)

super_cubes_upload = [x for x in super_cubes_upload if x in cube_upload_name]   
    

for i in super_cubes_upload:
    if i.name==cube_upload_name_str:
        print(f'cube found, updating data in cube: {i.name}')
        cube_upload_id = i.id
        ds = SuperCube(connection=connection, id=cube_upload_id)
        ds.add_table(name="Fixtures", data_frame=df, update_policy="replace")
        ds.add_table(name="Standings", data_frame=df_standings, update_policy="replace")
        ds.update()

if len(super_cubes_upload) == 0:
    ds = SuperCube(connection=connection, name=cube_upload_name_str)
    ds.add_table(name="Fixtures", data_frame=df, update_policy="replace")
    ds.add_table(name="Standings", data_frame=df_standings, update_policy="replace")
    ds.create()

connection.close()





