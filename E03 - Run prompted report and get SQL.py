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
## MSTR VERSION: MicroStrategy ONE June 2024 and MicroStrategy ONE September 2024
## The reports and prompts functions are older, so this could work with older versions
#


#
## Importing required modules
#
from mstrio.connection import Connection
from mstrio.project_objects import Report, list_reports
from mstrio.project_objects.prompt import Prompt
import pandas as pd
from time import sleep
from datetime import date, time, datetime
import json
import getpass #Optional


#
## Defining connection details
## YOU MUST CHANGE THIS TO YOUR ENVIRONMENT
## It uses getpass which is an interactive question for the password.
## comment this out and uncomment the other password if you want to set it to a standard password.
## Note: Authentication is standard or LDAP only for this approach.
#
base_url = 'https://env-324395.customer.cloud.microstrategy.com/MicroStrategyLibrary/'
username = 'glagrange'
password = getpass.getpass(prompt='Password ')
#password = 'YOUR PASSWORD'
project_name = 'MicroStrategy Tutorial'
date_string = "01-31-2020 14:45:37"
format_string = "%Y-%m-%d %H:%M:%S"

conn = Connection(base_url, username, password, project_name)

#
## Report IDs to be used
#
promptedreport='45192742874041E5F0ED028899334BDC'
objectpromptedreport='A07B5821F349CFFAF324B1941A3570E0'
valuepromptedreport='18C874C90243DE9B1F6B05844FBD5E28'
multiprompt='CA21E8A44BC2D9869046C6A46E082A4B'  #>Shared Reports>MicroStrategy Platform Capabilities>Ad hoc Reporting>Prompts


# ## EXAMPLE 1 - Single Object Report
# Script 

#
## Below is used if running multiple times and it gives a timestamp
## so that you can review what was run when
#
# x = datetime.now()
# print(x.strftime(format_string))

#
## Create a report object so that MicroStrategy knows the type and can apply functions
#
REPORT = Report(connection=conn, id=promptedreport)

#
## Getting a list or available prompts for a report
## NOTE: you will need to capture the prompt id from this output for later usage.
#
PROMPTS = REPORT.prompts
print(PROMPTS)

#
## Define variables which can be later used in a script for the actual prompts
#
PROMPT_TYPE = 'OBJECTS'
PROMPT_KEY = '6B867934AEFE4721B9B1693DBB718F70@0@10'
PROMPT_KEY_2 = '6B867934AEFE4721B9B1693DBB718F70@0@10'
PROMPT_ANSWERS = [{'name': 'Call Center', 'id': '8D679D3511D3E4981000E787EC6DE8A4', 'type': 'attribute'}]
#PROMPT_ANSWERS=[]

#
## Prepare a prompt
#
PROMPT = Prompt(type=PROMPT_TYPE, key=PROMPT_KEY, answers=PROMPT_ANSWERS)

#
## Answer prompts via to_dataframe method (note that you can pass multiple prompts in a list format)
#
df = REPORT.to_dataframe(prompt_answers=[PROMPT])#, PROMPT_2])


# Answer prompts via to_dataframe method
#df = REPORT.to_dataframe(prompt_answers=[PROMPT], PROMPT_2])
print (type(PROMPTS))

# Get sql property of a report
sql = REPORT.sql
print(df)
print(sql)

# ## Example 2 - 

x = datetime.now()
print(x.strftime(format_string))

# Managing report prompts
REPORT = Report(connection=conn, id=objectpromptedreport)

# Getting a list or available prompts for a report
PROMPTS = REPORT.prompts
print(PROMPTS)

# Define variables which can be later used in a script
PROMPT_TYPE = 'OBJECTS'
PROMPT_KEY = '6B867934AEFE4721B9B1693DBB718F70@0@10'
PROMPT_KEY_2 = '6B867934AEFE4721B9B1693DBB718F70@0@10'
PROMPT_ANSWERS = [{'name': 'Call Center', 'id': '8D679D3511D3E4981000E787EC6DE8A4', 'type': 'attribute'}]
#PROMPT_ANSWERS=[]

# Prepare a prompt
PROMPT = Prompt(type=PROMPT_TYPE, key=PROMPT_KEY, answers=PROMPT_ANSWERS)
# Prepare a prompt with default answer
#PROMPT_2 = Prompt(type=PROMPT_TYPE, key=PROMPT_KEY_2, use_default=True)
#prompt = Prompt(type='VALUE', key='CA6906D3499B6AEE259BFE9C308076D7@0@10', answers=100, use_default=False)

# Answer prompts via to_dataframe method
df = REPORT.to_dataframe(prompt_answers=[PROMPT])#, PROMPT_2])

# Answer prompts via to_dataframe method
#df = REPORT.to_dataframe(prompt_answers=[PROMPT], PROMPT_2])
print (type(PROMPTS))

# Get sql property of a report
sql = REPORT.sql
print(df)
print(sql)


# ## Example 3 - Value (Single Input)

x = datetime.now()
print(x.strftime(format_string))

# Managing report prompts
REPORT = Report(connection=conn, id=valuepromptedreport)

# Getting a list or available prompts for a report
PROMPTS = REPORT.prompts
print(PROMPTS)


# Define variables which can be later used in a script
PROMPT_TYPE = 'VALUE'
PROMPT_KEY = 'D8BA0AE2FA4475DB310DFF9B04CE0BED@0@10'
PROMPT_KEY_2 = 'D8BA0AE2FA4475DB310DFF9B04CE0BED@0@10'
#PROMPT_ANSWERS = [{'name': 'Call Center', 'id': '8D679D3511D3E4981000E787EC6DE8A4', 'type': 'attribute'}]
PROMPT_ANSWERS=[100]

# Prepare a prompt
PROMPT = Prompt(type=PROMPT_TYPE, key=PROMPT_KEY, answers=PROMPT_ANSWERS)
# Prepare a prompt with default answer
#PROMPT_2 = Prompt(type=PROMPT_TYPE, key=PROMPT_KEY_2, use_default=True)
#prompt = Prompt(type='VALUE', key='CA6906D3499B6AEE259BFE9C308076D7@0@10', answers=100, use_default=False)

# Answer prompts via to_dataframe method
df = REPORT.to_dataframe(prompt_answers=[PROMPT])#, PROMPT_2])

# Answer prompts via to_dataframe method
#df = REPORT.to_dataframe(prompt_answers=[PROMPT], PROMPT_2])
print (type(PROMPTS))

# Get sql property of a report
sql = REPORT.sql
print(df)
print(sql)

#
## Close connection to MicroStrategy. If you dont do this, you can get lots of login sessions. Be careful!
conn.close()

