#!/usr/bin/env python
# coding: utf-8

# In[4]:


'''
*** TESTED ON Strategy ONE (Sept 2025) ***

STEPS to Export:
1. Connect to Libary
2. Get Dashboard Definition (to get the filter key) and source ID for filters
3. Create in memory instance
4. Get elements of attribute for the filter (Based on single filter for this example)
5. Apply new filter values (put) using body request and key from step1
6. Call original ID and in Memory instance ID with either PDF or Excel
   file is returned in json format using data tag so encode/decode as necessary
'''

## Note: some modules are not required, but I have left them here for future usage
from mstrio.connection import Connection
from mstrio.project_objects.document import (Document, list_documents, list_documents_across_projects)
from mstrio.project_objects.dashboard import (Dashboard,list_dashboards,list_dashboards_across_projects)
import pandas as pd
import json
import base64

##Variables list
#base_url can only connect to Library
#login mode is in the connection variable
base_url = "https://tutorial.microstrategy.com/MicroStrategyLibraryInsights/"
username = "glagrange"
password = open('variable.txt').read().strip()

#Change it to a new value -- MUST BE CHANGED
project_name = "AI Auto"

#Dashboard (Dossier) ID -- MUST BE CHANGED
obj_id='2584E5D70D4AFE44A464C9BF79178FFD'
filename=''

#select AeaR from element list as the default is International
desired_name = "Jeep"


## Custom Functions for when the Python module does not exist
## or does not achieve output

#Step2
def doc_instance(conn, DocId):
    url_add=f"/api/documents/{DocId}/instances"
    res = conn.post(url=conn.base_url+url_add)
    doc_json = json.loads(res.text)
    doc_inst_id = doc_json["mid"]
    return doc_inst_id

#Step4
def doc_filter_elements(conn, DocId, doc_inst_id, filter_source_id):
    url_add=f"/api/dossiers/{DocId}/instances/{doc_inst_id}/elements?targetObjectId={filter_source_id}&targetObjectType=attribute"
    res = conn.get(url=conn.base_url+url_add)
    filter_element_list = json.loads(res.text)
    return filter_element_list

#Step5
def doc_change_filter(conn, DocId, doc_inst_id, body):
    url_add = f"/api/dossiers/{DocId}/instances/{doc_inst_id}/filters"
    # correct: put the JSON body with the `json` parameter
    res = conn.put(url=conn.base_url+url_add, json=body)
    return res

#Step6
def doc_instance_export(conn, DocId, doc_inst_id, pdfsettings, filename=f"output/{filename}.pdf"):
    url_add = f"/api/documents/{DocId}/instances/{doc_inst_id}/pdf"
    res = conn.post(url=conn.base_url + url_add, json=pdfsettings)
    
    if res.status_code == 200:
        # Parse JSON
        json_resp = res.json()
        pdf_base64 = json_resp["data"]
        # Decode and save
        pdf_bytes = base64.b64decode(pdf_base64)
        with open(filename, "wb") as f:
            f.write(pdf_bytes)
        print(f"PDF exported to {filename}")
        return filename
    else:
        print("Error:", res.status_code, res.text)
        return None


# In[2]:


'''Step1 - Connect to Library'''
conn = Connection(base_url, username, password,login_mode=16, project_name=project_name)

'''Step2 - Get Document/Dossier/Dashboard definition'''
# Get single document by id
# Doing this to get the name for the file. It can be done the other way around
# but ID are unique, Document names are not....
dashboard = Dashboard(connection=conn, id=obj_id)
#print(document.name)
filename=f"output/{dashboard.name}.pdf"
print(f'PDF will be exported as: {filename}')
print('')

# List dashboard properties to get the filters. If OFF Screen its Chapter Filters
# example Filters are under: dashboard['chapters'][0].pages[0].filters
properties = dashboard.list_properties()
#print(properties)

filters = []

for chapter in properties.get('chapters', []):
    for page in getattr(chapter, 'pages', []):
        # Page-level filters
        if hasattr(page, 'filters') and page.filters:
            for f in page.filters:
                filters.append({
                    'filter_id': f.get('key'),
                    'filter_name': f.get('name'),
                    'source_id': f.get('source', {}).get('id')
                })
    # Chapter-level filters
    if hasattr(chapter, 'filters') and chapter.filters:
        for f in chapter.filters:
            filters.append({
                'filter_id': f.get('key'),
                'filter_name': f.get('name'),
                'source_id': f.get('source', {}).get('id')
            })

# Print them
# For this example its 1 filter, so moving those to variables.
for f in filters:
    print(f"Filters found ** Filter ID: {f['filter_id']} | Name: {f['filter_name']} | Source ID: {f['source_id']}")
    filter_source_id = f['source_id']
    filter_id = f['filter_id']
    filter_name= f['filter_name']


# In[3]:


'''Step3 - Create Document/Dashboard instance'''
print(f'Creating instance ID for Document ID:{obj_id}')
doc=doc_instance(conn,obj_id)
print(f'Document Instance ID: {doc}')

'''Step4 - Get elements of attribute for filter'''
element_list = doc_filter_elements(conn, obj_id,doc,filter_source_id)
print('')
#print(element_list)
#print(type(element_list))


'''Step5 - change filter to values'''
element_id = None
for e in element_list:
    if e["name"] == desired_name:
        element_id = e["id"]
        break

if element_id is None:
    raise ValueError(f"No element found for {desired_name}")

    
filter_body = [
    {
        "key": f"{filter_id}",
        "name": f"{filter_name}",
        "selections": [
            {
                "id": f"{element_id}"
            }
        ]
    }
]

print('')
print(f'Changing filter to: {filter_body}')
print('')

filter_change = doc_change_filter(conn, obj_id, doc, filter_body)

print('')

'''Step6 - Export to PDF'''
pdfsettings={
    "pageHeight": 11.69,
    "pageWidth": 8.27,
    "orientation": "AUTO", #portrait for landscape
    "gridPagingMode":"none",
    "pageOption": "CURRENT",  #options are ALL or CURRENT
    "includeOverview": "true",
    "includeToc":"false", #table of contents
    "nodeKeys": ["Wb92de560610c11f096295f1dda2e4292"], #For multiple pages, use a list format.
    "includeDetailedPages": "false", #DONT CHANGE THIS OR IT WILL PRINT EACH VISUAL ON A PAGE
    "waitingTimeBeforeExport": 0, #I dont recommend changing this either
    "includeHeader": "false",
    "includeFooter": "false"
}

doc_export=doc_instance_export(conn,obj_id,doc, pdfsettings,filename)

conn.close()


# In[ ]:




