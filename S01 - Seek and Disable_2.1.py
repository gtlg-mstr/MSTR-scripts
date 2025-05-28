#!/Users/glagrange/.pyenv/versions/dev2501/bin/python
# coding: utf-8


# This script retrieves a list of users from a Google Sheet, disables inactive users, and closes the database connection.
from mstrio.connection import Connection
from mstrio.users_and_groups import list_users, User
import requests
import csv

#base_url can only connect to Library
#login mode is in the connection variable
## PLEASE CHANGE AS NEEDED
base_url = "https://env-352170.customer.cloud.microstrategy.com/MicroStrategyLibrary"
username = ""
password =""

#Default project - this is used as a generic connection
#Change it to a new value if need be
project_name = "Platform Analytics"

#Initial connection used for getting a list of all projects 
# LDAP LOGIN IS 16
conn = Connection(base_url, username, password,login_mode=1)

#option1: Use a live url (google spreadhsheet, o365 excel) to store the list:
userlist = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQAh7twlMkJ6qA42JQu8sMoDXWnblwulSUFysTl8l3wjKt88sXkLB_cOZyiLuBqBdOFoinzns1QGBxH/pub?gid=0&single=true&output=csv'

#option2: Type the list out (why though?)
#if you want to try this out, remember to comment out option 1 and 3
#userlist =['','']

#Was used only for testing
#all_users = list_users(connection=conn)
#active_users = [u for u in all_users if u.enabled]



# In[2]:


def fetch_csv_as_list(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        # Raise an exception if there's an issue with the request (e.g., 404, 500)
        response.raise_for_status()
        
        # Decode the content of the CSV file (assuming it's UTF-8 encoded)
        csv_content = response.content.decode('utf-8')
        
        # Parse the CSV content
        csv_data = list(csv.reader(csv_content.splitlines()))
        
        return csv_data
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch the CSV from URL: {e}")
        return None




def diable_users(users: list[User]) -> None: 
    """Disable users
    Args:
        users (list[str]): List of usernames to disable.
    """
    try:
        for username in users:
            user_instance = User(conn, username=username)
            user_instance.alter(enabled=False)
            print(f"User {username} disabled successfully.") # Added for feedback
    except Exception as e:
        print(f"An error occurred while disabling users: {e}") # Better error message
        # Consider logging the error to a file or monitoring system for debugging.
        

#1. Start with getting list of users
print(f"**INFO**Getting list of users")
csv_as_list = fetch_csv_as_list(userlist)

if csv_as_list:
    print("CSV data as list:")
    for row in csv_as_list:
        print(row)
    
#2. Using the CSV List
print(f"**INFO**Looping through list and disabling user")
diable_users(csv_as_list)

#3. Close connection to Strategy Software.
#This is required otherwise it will keep the session open...
print(f"**INFO**Closing connection to Server")
conn.close()

