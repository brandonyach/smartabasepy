from smartabasepy import get_user, get_group, UserFilter, UserOption, GroupOption

# Username filter
get_user(
    url="https://learn.smartabase.com/ankle",
    filter=UserFilter(
        user_key="username", 
        user_value="riley.jones"
    ),
    username="python.connector",
    password="Connector123!",
    option=UserOption(
        columns=["user_id", 
                 "about", 
                 "phone_number", 
                 "role", 
                 "athlete_group"
                ]
        )
)


# About filter
get_user(
    url="https://learn.smartabase.com/ankle",
    filter=UserFilter(user_key="about", user_value="Riley Jones"),
    username="python.connector",
    password="Connector123!",
    option=UserOption(columns=["user_id", "about", "phone_number", "role", "athlete_group"])
)

# Warning for groupsandroles
get_user(
    url="https://learn.smartabase.com/ankle",
    filter=UserFilter(user_key="about", user_value="Brandon Yach"),
    username="python.connector",
    password="Connector123!",
    option=UserOption(columns=["user_id", "about", "phone_number", "role", "athlete_group", "coach_group"])
)

# Fetching groupsandroles
get_user(
    url="https://learn.smartabase.com/ankle",
    filter=UserFilter(user_key="username", user_value="brandon.yach"),
    username="python.connector",
    password="Connector123!",
    option=UserOption(columns=["user_id", "about", "phone_number", "role", "athlete_group", "coach_group"])
)


# Group filter
get_user(
    url="https://learn.smartabase.com/ankle",
    filter=UserFilter(
        user_key="group", 
        user_value="Test Group"
    ),
    username="python.connector",
    password="Connector123!"
)


# Groups
get_group(
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=GroupOption(guess_col_type=True, interactive_mode=True)
)



# Incorrect URL
get_user(
    url="https://learn.smartabase.com/ankl",
    filter=UserFilter(user_key="username", user_value="python.connector"),
    username="python.connector",
    password="Connector123!"
)

# Incorrect credentials
get_user(
    url="https://learn.smartabase.com/ankle",
    filter=UserFilter(user_key="username", user_value="python.connector"),
    username="python.connector",
    password="Connector123"
)



# SAVE USER (Creation) ----

from pandas import DataFrame
from smartabasepy.user_ import create_user
from smartabasepy.user_option import UserOption

# Create mapping DataFrame
mapping_data = {
    "first_name": ["John", "Jane", "Bob", "Karen"],
    "last_name": ["Smith", "Smith", "Smith", "Smith"],
    "username": ["john.smith", "jane.smith", "bob.smith", "karen.smith"],
    "email": ["john.smith@ams.com", "jane.smith@ams.com", "bob.smith@ams.com", "karen.smith@ams.com"],
    "dob": ["01/11/2000", "02/12/1995", "06/07/2003", "04/09/2002"],
    "password": ["SecurePass123!", "StrongPass456!", "Password123", "Password123!"],
    "active": [True, False, True, True],
    "uuid": ["787878", "898989", "090909", "505050"],
    "known_as": ["Johnny", "", "", ""],
    "middle_names": ["Michael", "", "", ""],
    "language": ["English", "English", "English", "English"],
    "sidebar_width": ["250px", "", "", ""],
    "sex": ["Male", "Female", "Male", "Female"]
}
mapping_df = DataFrame(mapping_data)

# Save (create) users
create_user(
    user_df=mapping_df,
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)



# Step 1: Get user IDs
# user_df = get_user(
#     url="https://learn.smartabase.com/ankle",
#     username="python.connector",
#     password="Connector123!",
#     option=UserOption(interactive_mode=True,
#                       cache=False),
#     filter=UserFilter(user_key="username", user_value=["john.doe", "john.smith"])
# )


# Step 2: Update users
# users_data = {
#     "user_id": ["120958", "120968"],  # IDs from previous creation
#     "first_name": ["JohnUpdated", "TestJohn"],
#     "last_name": ["Doe", "Smithboy"],
#     "username": ["john.updated", "johnnyboy.smith"],
#     "email": ["johnupdated@ams.com", "smith.test.update2.api@ams.com"],
#     "dob": ["01/01/1990", "10/07/2023"],
#     "sex": ["Female", "Unspecified"],
#     "uuid": ["234223", "3548235"]
#     # Optional fields omitted: middle_name, known_as, phoneNumbers, addresses
# }

# users_df = DataFrame(users_data)



from smartabasepy import get_user, edit_user
from smartabasepy import UserOption, UserFilter
from pandas import DataFrame
import pandas as pd
import numpy as np


users_data = {
    # "about": ["John Smith", "Jane Smith", "Bob Smith", "Karen Smith"],
    # "first_name": ["Johnwrew", "Janewew", "Bobwewe", "Karenweww"],
    # "last_name": ["Smith", "Smith", "Smith", "Smith"],
    # "username": ["john.wretrwsmith", "jane.werfsmith", "bob.werqwesmith", "karen.wesdfwesmith"],
    "email": ["john.smith@ams.com", "jane.smith@ams.com", "bob.smith@ams.com", "karen.smith@ams.com"],
    # "email": ["john.ssdsdsmith@ams.com", "jane.sdsdssmith@ams.com", "bob.sretwersmith@ams.com", "karen.ergdfsmith@ams.com"],
    # "dob": ["01/11/2020", "02/12/2020", "06/07/2020", "04/09/2020"],
    # "active": [False, True, False, True],
    "uuid": ["update11", "update22", "update33", "update44"],
    # "uuid": [37980809, np.nan, 12343322, 97977823],
    # "known_as": ["wew", "wew", "wew", "wew"],
    # "middle_names": ["Michael", "", "", ""],
    # "language": ["English", "English", "English", "English"],
    # "sidebar_width": ["250px", "", "", ""],
    # "sex": ["Unspecified", "Unspecified", "Unspecified", "Unspecified"]
}
users_df = DataFrame(users_data)



failed_df = edit_user(
    mapping_df=users_df,
    user_key="email",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)



# ------

users_data = {
    "email": ["john.smith@ams.com", "jane.smith@ams.com", "bob.smith@ams.com", "karen.smith@ams.com"],
    "uuid": [37980809, np.nan, 12343322, 97977823],
}
users_df = DataFrame(users_data)

users_df['uuid'] = users_df['uuid'].astype('Int64')