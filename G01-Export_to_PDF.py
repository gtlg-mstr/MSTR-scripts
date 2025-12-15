#!/usr/bin/env python
# coding: utf-8

from mstrio.connection import Connection
from mstrio.project_objects.document import (
    Document, list_documents, list_documents_across_projects
)
import pandas as pd
import json
import base64
#base_url can only connect to Library
#login mode is in the connection variable
base_url = "https://<SERVER>/MicroStrategyLibraryInsights/"
username = "<USER>"
password = open('variable.txt').read().strip()

#Change it to a new value if need be

project_name = "SnappyMart (SE Demo)"


doc_id='6268DD1611E6D28B107F0080EF453D0C'

conn = Connection(base_url, username, password,login_mode=16, project_name=project_name)

# Get single document by id
# Doing this to get the name for the file. It can be done the other way around
# but ID are unique, Document names are not....
document = Document(connection=conn, id=doc_id)
print(document.name)
filename=document.name


'''
STEPS to Export:
1. Create in memory instance
2. Call original ID and in Memory instance ID with either PDF or Excel
3. file is returned in json format using data tag
4. encode/decode as necessary
'''

#Step1
def doc_instance(conn, DocId):
    url_add=f"/api/documents/{DocId}/instances"
    res = conn.post(url=conn.base_url+url_add)
    doc_json = json.loads(res.text)
    doc_inst_id = doc_json["mid"]
    return doc_inst_id

#Step2
def doc_instance_export(conn, DocId, doc_inst_id, filename=f"output/{filename}.pdf"):
    url_add = f"/api/documents/{DocId}/instances/{doc_inst_id}/pdf"
    res = conn.post(url=conn.base_url + url_add)
    
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


print(f'Document ID:{doc_id}')

doc=doc_instance(conn,doc_id)
print(f'Document Instance ID: {doc}')

doc_export=doc_instance_export(conn,doc_id,doc)

conn.close()

