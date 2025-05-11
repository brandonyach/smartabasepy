# Single Event Payload ----

# import requests
# import json
# import logging

# # Set up logging for raw HTTP requests
# logging.basicConfig(level=logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

# # Smartabase API base URL
# base_url = "https://learn.smartabase.com/ankle"
# username = "python.connector"
# password = "Connector123!"

# # First, login to get session headers
# login_url = f"{base_url}/api/v2/user/loginUser?informat=json&format=json"
# login_payload = {
#     "username": username,
#     "password": password,
#     "loginProperties": {
#         "appName": "ankle",
#         "clientTime": "2025-04-13T12:00"
#     }
# }
# login_headers = {
#     "Content-Type": "application/json",
#     "X-GWT-Permutation": "HostedMode",
#     "User-Agent": "smartabaseR",
#     "Accept": "application/json"
# }

# # Perform login request
# try:
#     login_response = requests.post(login_url, headers=login_headers, auth=(username, password), data=json.dumps(login_payload))
#     print(f"Login Status Code: {login_response.status_code}")
#     print(f"Login Response: {login_response.text}")
#     login_data = login_response.json()
#     session_header = login_response.headers.get("session-header")
#     cookie = login_response.headers.get("Set-Cookie")
#     logged_in_user_id = login_data["user"]["id"]
# except Exception as e:
#     print(f"Login Error: {str(e)}")
#     exit()

# # Headers for event import
# headers = {
#     "Content-Type": "application/json",
#     "X-GWT-Permutation": "HostedMode",
#     "User-Agent": "smartabaseR",
#     "Accept": "application/json",
#     "Accept-Encoding": "gzip, deflate",
#     "X-Requested-With": "XMLHttpRequest",
#     "X-GWT-Module-Base": f"{base_url}/",
#     "Connection": "keep-alive",
#     "Origin": "https://learn.smartabase.com",
#     "Referer": "https://learn.smartabase.com/ankle",
#     "Cookie": cookie if cookie else "",
#     "session-header": session_header if session_header else ""
# }

# # Single event payload with logged-in user_id
# payload = {
#     "formName": "Test API",
#     "startDate": "01/04/2025",
#     "startTime": "1:59 PM",
#     "finishDate": "01/04/2025",
#     "finishTime": "2:59 PM",
#     "userId": {"userId": logged_in_user_id},  # Use logged-in user_id
#     "enteredByUserId": logged_in_user_id,
#     "rows": [
#         {
#             "row": 0,
#             "pairs": [
#                 {"key": "Field 1", "value": "12.31"},
#                 {"key": "Field 2", "value": "Test Single Event"}
#             ]
#         }
#     ]
# }

# url = f"{base_url}/api/v1/eventsimport?informat=json&format=json"
# print(f"\nTesting endpoint: {url}")
# print(f"Headers: {headers}")
# print(f"Payload: {json.dumps(payload, separators=(',', ':'))}")

# try:
#     response = requests.post(url, headers=headers, auth=(username, password), data=json.dumps(payload, separators=(',', ':')))
#     print(f"Status Code: {response.status_code}")
#     print(f"Response: {response.text}")
# except Exception as e:
#     print(f"Error: {str(e)}")



#  Multi-Event Payload ----


import requests
import json
import logging
import numpy as np

# Set up logging for raw HTTP requests
logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

# Smartabase API base URL
base_url = "https://learn.smartabase.com/ankle"
username = "python.connector"
password = "Connector123!"

# First, login to get session headers
login_url = f"{base_url}/api/v2/user/loginUser?informat=json&format=json"
login_payload = {
    "username": username,
    "password": password,
    "loginProperties": {
        "appName": "ankle",
        "clientTime": "2025-04-13T12:00"
    }
}
login_headers = {
    "Content-Type": "application/json",
    "X-GWT-Permutation": "HostedMode",
    "User-Agent": "smartabaseR",
    "Accept": "application/json"
}

# Perform login request
try:
    login_response = requests.post(login_url, headers=login_headers, auth=(username, password), data=json.dumps(login_payload))
    print(f"Login Status Code: {login_response.status_code}")
    print(f"Login Response: {login_response.text}")
    login_data = login_response.json()
    session_header = login_response.headers.get("session-header")
    cookie = login_response.headers.get("Set-Cookie")
    logged_in_user_id = login_data["user"]["id"]
except Exception as e:
    print(f"Login Error: {str(e)}")
    exit()

# Headers for event import
headers = {
    "Content-Type": "application/json",
    "X-GWT-Permutation": "HostedMode",
    "User-Agent": "smartabaseR",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
    "X-Requested-With": "XMLHttpRequest",
    "X-GWT-Module-Base": f"{base_url}/",
    "Connection": "keep-alive",
    "Origin": "https://learn.smartabase.com",
    "Referer": "https://learn.smartabase.com/ankle",
    "Cookie": cookie if cookie else "",
    "session-header": session_header if session_header else ""
}

# Multi-event payload with events wrapper (SmartabaseR style)
payload = {
    "events": [
        {
            "formName": "Test API",
            "startDate": "01/04/2025",
            "startTime": "1:59 PM",
            "finishDate": "01/04/2025",
            "finishTime": "2:59 PM",
            "userId": {"userId": 72827},
            "enteredByUserId": 120855,
            "existingEventId": np.nan,
            "rows": [
                {
                    "row": 0,
                    "pairs": [
                        {"key": "Field 1", "value": "12.31"},
                        {"key": "Field 2", "value": "Test Multi Event 1"}
                    ]
                }
            ]
        },
        {
            "formName": "Test API",
            "startDate": "01/04/2025",
            "startTime": "1:59 PM",
            "finishDate": "01/04/2025",
            "finishTime": "2:59 PM",
            "userId": {"userId": 72828},
            "enteredByUserId": 120855,
            "existingEventId": 2284560,
            "rows": [
                {
                    "row": 0,
                    "pairs": [
                        {"key": "Field 1", "value": "15.45"},
                        {"key": "Field 2", "value": "Test Multi Event 2"}
                    ]
                }
            ]
        }
    ]
}

url = f"{base_url}/api/v1/eventsimport?informat=json&format=json"
print(f"\nTesting endpoint: {url}")
print(f"Headers: {headers}")
print(f"Payload: {json.dumps(payload, separators=(',', ':'))}")

try:
    response = requests.post(url, headers=headers, auth=(username, password), data=json.dumps(payload, separators=(',', ':')))
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {str(e)}")


# Testing An Update Event call ----

import requests
import json
import logging

# Set up logging for raw HTTP requests
logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

# Smartabase API base URL
base_url = "https://learn.smartabase.com/ankle"
username = "python.connector"
password = "Connector123!"

# First, login to get session headers
login_url = f"{base_url}/api/v2/user/loginUser?informat=json&format=json"
login_payload = {
    "username": username,
    "password": password,
    "loginProperties": {
        "appName": "ankle",
        "clientTime": "2025-04-13T12:00"
    }
}
login_headers = {
    "Content-Type": "application/json",
    "X-GWT-Permutation": "HostedMode",
    "User-Agent": "smartabaseR",
    "Accept": "application/json"
}

# Perform login request
try:
    login_response = requests.post(login_url, headers=login_headers, auth=(username, password), data=json.dumps(login_payload))
    print(f"Login Status Code: {login_response.status_code}")
    print(f"Login Response: {login_response.text}")
    login_data = login_response.json()
    session_header = login_response.headers.get("session-header")
    cookie = login_response.headers.get("Set-Cookie")
    logged_in_user_id = login_data["user"]["id"]
except Exception as e:
    print(f"Login Error: {str(e)}")
    exit()

# Headers for event import (minimal, matching SmartabaseR)
headers = {
    "Content-Type": "application/json",
    "X-GWT-Permutation": "HostedMode",
    "User-Agent": "smartabaseR",
    "Cookie": cookie if cookie else "",
    "session-header": session_header if session_header else ""
}

# Single-event update payload with events wrapper (SmartabaseR style)
payload = {
    "events": [
        {
            "formName": "Test Import Form",
            "startDate": "14/04/2025",
            "startTime": "5:33 AM",
            "finishDate": "14/04/2025",
            "finishTime": "6:33 AM",
            "userId": {"userId": 72827},
            "enteredByUserId": 120855,
            "existingEventId": 2284266,
            "rows": [
                {
                    "row": 0,
                    "pairs": [
                        {"key": "Field 1", "value": "1.1"},
                        {"key": "Field 2", "value": "1.1"},
                        {"key": "Field 3", "value": "Test - Update"}
                    ]
                }
            ]
        }
    ]
}

url = f"{base_url}/api/v1/eventsimport?informat=json&format=json"
print(f"\nTesting endpoint: {url}")
print(f"Headers: {headers}")
print(f"Payload: {json.dumps(payload, separators=(',', ':'))}")

try:
    response = requests.post(url, headers=headers, auth=(username, password), data=json.dumps(payload, separators=(',', ':')))
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {str(e)}")
    
    
    
    
# Retesting an Import call with omitting existingEventId

import requests
import json
import logging

# Set up logging for raw HTTP requests
logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

# Smartabase API base URL
base_url = "https://learn.smartabase.com/ankle"
username = "python.connector"
password = "Connector123!"

# First, login to get session headers
login_url = f"{base_url}/api/v2/user/loginUser?informat=json&format=json"
login_payload = {
    "username": username,
    "password": password,
    "loginProperties": {
        "appName": "ankle",
        "clientTime": "2025-04-13T12:00"
    }
}
login_headers = {
    "Content-Type": "application/json",
    "X-GWT-Permutation": "HostedMode",
    "User-Agent": "smartabaseR",
    "Accept": "application/json"
}

# Perform login request
try:
    login_response = requests.post(login_url, headers=login_headers, auth=(username, password), data=json.dumps(login_payload))
    print(f"Login Status Code: {login_response.status_code}")
    print(f"Login Response: {login_response.text}")
    login_data = login_response.json()
    session_header = login_response.headers.get("session-header")
    cookie = login_response.headers.get("Set-Cookie")
    logged_in_user_id = login_data["user"]["id"]
except Exception as e:
    print(f"Login Error: {str(e)}")
    exit()

# Headers for event import (minimal, matching SmartabaseR)
headers = {
    "Content-Type": "application/json",
    "X-GWT-Permutation": "HostedMode",
    "User-Agent": "smartabaseR",
    "Cookie": cookie if cookie else "",
    "session-header": session_header if session_header else ""
}

# Single-event insert payload with events wrapper, omitting existingEventId
payload = {
    "events": [
        {
            "formName": "Test Import Form",
            "startDate": "14/04/2025",
            "startTime": "8:25 AM",
            "finishDate": "14/04/2025",
            "finishTime": "9:25 AM",
            "userId": {"userId": 72827},
            "enteredByUserId": 120855,
            "rows": [
                {
                    "row": 0,
                    "pairs": [
                        {"key": "Field 1", "value": "16.31"},
                        {"key": "Field 2", "value": "21"},
                        {"key": "Field 3", "value": "This is a test"}
                    ]
                }
            ]
        }
    ]
}

url = f"{base_url}/api/v1/eventsimport?informat=json&format=json"
print(f"\nTesting endpoint: {url}")
print(f"Headers: {headers}")
print(f"Payload: {json.dumps(payload, separators=(',', ':'))}")

try:
    response = requests.post(url, headers=headers, auth=(username, password), data=json.dumps(payload, separators=(',', ':')))
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {str(e)}")