### Adding groups, users and adding a user to a group
### Gareth La Grange
### Tested with Python 3.6 / MSTR 10.11 / 2018-07-10
### With thanks to Robert Prochowicz for his initial sample script

#Load Python modules
import requests
import base64
import json


### Parameters ###
environmentId = '100245'
api_login = 'admin'
api_password = ''
base_url = "https://env-" + environmentId + ".customer.cloud.microstrategy.com/MicroStrategyLibrary/api/";


#### Get token ###
def login(base_url,api_login,api_password):
    print("Getting token...")
    data_get = {'username': api_login,
                'password': api_password,
                'loginMode': 1}
    r = requests.post(base_url + 'auth/login', data=data_get)
    if r.ok:
        authToken = r.headers['X-MSTR-AuthToken']
        cookies = dict(r.cookies)
        print ("Token: " + authToken)
        return authToken, cookies
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))

#### Get session info ###
def get_sessions(base_url, auth_token, cookies):
    print("Checking session...")
    header_gs = {'X-MSTR-AuthToken': auth_token,
                 'Accept': 'application/json'}
    r = requests.get(base_url + "sessions", headers=header_gs, cookies=cookies)
    if r.ok:
        print("Authenticated...")
        #print(r)
        print('>>>\n>>>')
        print('Using URL: ' + r.url)
        print('Using Header: ' + r.text)
        print("HTTP %i - %s" % (r.status_code, r.reason))
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


##
#Option 3 - retrieve group list
##
def get_groups(base_url, auth_token, cookies):
    headers_cc = {'X-MSTR-AuthToken': auth_token,
                  'Content-Type': 'application/json',#IMPORTANT!
                  'Accept': 'application/json',
                  }
    print("\nRetrieving group List of Top Level Groups...")
    #r = requests.get(base_url + "usergroups?limit=-1", headers=headers_cc, cookies=cookies) #Alternate to get all groups
    r = requests.get(base_url + "usergroups/topLevel", headers=headers_cc, cookies=cookies)
    r1=r.json() #serialize the JSON
    json_str = json.dumps(r1)
    data = json.loads(json_str) 

    if r.ok:
        print('>>>\n>>>')
        print('Using URL: ' + r.url)
        print('>>> \n>>> List of Groups in MicroStrategy <<<\n>>>')
        for element in data:
            print('  '+str(element['name']))

    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))    



##
#Option 4 - create group
##
def create_group(base_url, auth_token, cookies, input4):
    headers_cc = {'X-MSTR-AuthToken': auth_token,
                  'Content-Type': 'application/json',#IMPORTANT!
                  'Accept': 'application/json',
                  }
    inputbody = ('{"name": "'+input4+'"}')
   # print (inputbody)
    
    r = requests.post(base_url + 'usergroups', headers=headers_cc, cookies=cookies, data=inputbody)

    if r.ok:
        print('>>>\n>>>')
        print('Using URL: ' + r.url)
        print('>>>\n>>>')
        #print("Error: " + str(r.raise_for_status()) + "   ||   HTTP Status Code: " + str(r.status_code))
        print("usergroup: " + input4 + " created")

    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
        print(r.request.headers)
        print(r.request.body)



##
#Option 5 - create users
##
def create_user(base_url, auth_token, cookies, input5):
    headers_cc = {'X-MSTR-AuthToken': auth_token,
                  'Content-Type': 'application/json',#IMPORTANT!
                  'Accept': 'application/json',
                  }
    inputbody = ('{"fullName": "'+input5+'","username": "' +input5.replace(" ", "")+'","requireNewPassword": true,"passwordModifiable": true}')
 #   print (inputbody)
    
    r = requests.post(base_url + 'users', headers=headers_cc, cookies=cookies, data=inputbody)

    if r.ok:
        #print("Error: " + str(r.raise_for_status()) + "   ||   HTTP Status Code: " + str(r.status_code))
        print('>>>\n>>>')
        print('Using URL: ' + r.url)
        print('>>>\n>>>')
        print("User: " + input5 + " created with login name: " +input5.replace(" ", ""))
        print("\nThe password is empty - please note that the user will be required to change it on first login")

    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
        print(r.request.headers)
        print(r.request.body)


##
#Option 6 - add user to group
##
def groupadd(base_url, auth_token, cookies, input6a, input6b):
    headers_cc = {'X-MSTR-AuthToken': auth_token,
                  'Content-Type': 'application/json',#IMPORTANT!
                  'Accept': 'application/json',
                  }
    #usergroups?nameBegins=TestA&limit=-1
    
    getid = requests.get(base_url + 'usergroups?nameBegins='+input6b+'&limit=-1', headers=headers_cc, cookies=cookies)
    
  
    getid1=getid.json() #serialize the JSON
    getid_json_str = json.dumps(getid1)
    getid_data = json.loads(getid_json_str)
    for element in getid_data:
        groupname = (str(element['name']))
        gid = (str(element['id']))
    print('Group id: '+ gid)

    #Get userid
    getuid = requests.get(base_url + 'users?abbreviationBegins='+input6a.replace(" ", "")+'&limit=-1', headers=headers_cc, cookies=cookies)
    getuid1=getuid.json() #serialize the JSON
    getuid_json_str = json.dumps(getuid1)
    getuid_data = json.loads(getuid_json_str)
    for element in getuid_data:
        uname = (str(element['name']))
        uid = (str(element['id']))
    print('User Id: '+ uid)

   
        
    if getid.ok & getuid.ok :

        print('>>>Adding user')
        inputbody = ('{"membersAddition": [ "'+ uid +'" ] }')
        r = requests.put(base_url + 'usergroups/' + gid, headers=headers_cc, cookies=cookies, data=inputbody)
        print('>>>\n>>>')
        print('Using URL: ' + r.url)
        print('>>>\n>>>')
        print('Adding user %s to Group %s' % (uname , groupname))


    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
        print(r.request.headers)
        print(r.request.body)


        
        


def main():
    authToken, cookies = login(base_url,api_login,api_password)
    choice = None
    while choice != "0":
        print \
        ("""
        ---MENU---
        
        0 - Exit
        1 - Generate and Print Token
        2 - Check session
        3 - List all groups
        4 - Create user group
        5 - Create user
        6 - Add user to group
        """)

        choice = input("Your choice: ") # What To Do ???
        print()
    
        if choice == "0":
            print("Good bye!")  
        elif choice == "1":
            authToken, cookies = login(base_url,api_login,api_password)
        elif choice == "2":
            get_sessions(base_url, authToken, cookies)
        elif choice == "3":
            get_groups(base_url, authToken, cookies)
        elif choice == "4":
            input4 = input("Please input the name of the group that you want to create: ")
            create_group(base_url, authToken, cookies, input4)
        elif choice == "5":
            input5 = input("Please input the Fullname of the user that you want to create (Ex. John Doe: ")
            create_user(base_url, authToken, cookies, input5)
        elif choice == "6":
            input6a = input("Please input the Fullname of the user that you want to add to the group (Ex. John Doe: ")
            input6b = input("Please input the Group name:")
            groupadd(base_url, authToken, cookies, input6a, input6b)
        else:
            print(" ### Wrong option ### ")

### Main program    
main()

