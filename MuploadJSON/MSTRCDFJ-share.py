### Load Python modules ###
from mstrio import microstrategy
import sys
import json
import pandas as pd


### Parameters ###
environmentId = '126178'
api_login = 'glagrange'
api_password = 'MMMMMMM'
baseurl = 'https://env-' + environmentId + '.customer.cloud.microstrategy.com/MicroStrategyLibrary/api'
api_project='MicroStrategy Tutorial'
jsonpath = (sys.argv[1]) #take file name from cmdline



#Used for testing the URL
# print(baseurl)
print(">>>")
print("Connecting to " + baseurl)
print(" ")
conn = microstrategy.Connection(base_url=baseurl, username=api_login, password=api_password, project_name=api_project)
conn.connect()



print(">>>")
print("Reading JSON file at " + jsonpath)  
with open(jsonpath) as json_file:  
    data = json.load(json_file)
 # Used for testing
 #    print(data)



print (">>>")
print ("Converting JSON to DF")
df = pd.DataFrame(data, columns=['USERNAME', 'ITEM','PRICE','RATE','DURATION','COST','MONTHLY'])



print (">>>")
print ("Creating Cube")
conn.create_dataset(data_frame=df, dataset_name='MicroStrategy AR', table_name='SCANNED_ITEMS')




print (">>>")
print ("Closing connection to Server")
conn.close()



