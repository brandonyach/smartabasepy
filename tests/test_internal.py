
# DEACTIVATING USERS ----

from pandas import DataFrame
from internal_scripts.deactivate_users import deactivate_users
from smartabasepy.user_option import UserOption

# Create mapping DataFrame
mapping_data = {
    "about": ["Riley Jones", "Samantha Fields"]
}
mapping_df = DataFrame(mapping_data)

# Deactivate users
deactivate_users(
    mapping_df=mapping_df,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)


# UPDATING PASSWORDS ----

from pandas import DataFrame
from internal_scripts.update_passwords import update_user_passwords
from smartabasepy.user_option import UserOption

# Create mapping DataFrame
mapping_data = {
    "about": ["Riley Jones", "Samantha Fields"],
    "password": ["Test123456", "Test123456"]
}
mapping_df = DataFrame(mapping_data)


mapping_data = {
    "about": ["Athlete Test"],
    "password": ["New123456789"]
}
mapping_df = DataFrame(mapping_data)

# Update passwords
update_user_passwords(
    mapping_df=mapping_df,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)


# UPDATING DATE OF BIRTH ----

from pandas import DataFrame
from internal_scripts.update_date_of_birth import update_user_date_of_birth
from smartabasepy.user_option import UserOption

# Create mapping DataFrame
mapping_data = {
    "about": ["Riley Jones", "Samantha Fields", "Dane Cook"],
    "date_of_birth": ["01/12/1990", "02/10/1985", "01/01/2000"]
}
mapping_df = DataFrame(mapping_data)

# Update date of birth
update_user_date_of_birth(
    mapping_df=mapping_df,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)



# UPDATING EMAIL ADDRESS ----

from pandas import DataFrame
from internal_scripts.update_email_address import update_user_email_address
from smartabasepy.user_option import UserOption

# Create mapping DataFrame
mapping_data = {
    "about": ["Riley Jones", "Samantha Fields"],
    "email_address": ["riley.jones@newdomain.com", "sam.fields@newdomain.com"]
}
mapping_df = DataFrame(mapping_data)

# Update email address
failed_updates = update_user_email_address(
    mapping_df=mapping_df,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)


# UPDATING UUID ----

from pandas import DataFrame
from internal_scripts.update_uuid import update_user_uuid
from smartabasepy.user_option import UserOption

# Create mapping DataFrame
mapping_data = {
    "about": ["Riley Jones", "Samantha Fields"],
    "uuid": ["new123", "new456"]
}
mapping_df = DataFrame(mapping_data)

# Update UUID
failed_updates = update_user_uuid(
    mapping_df=mapping_df,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)



# Test again

users_data = {
    "about": ["John Smith", "Jane Smith", "Bob Smith", "Karen Smith"],
    # "first_name": ["John", "Jane", "Bob", "Karen"],
    # "last_name": ["Smith", "Smith", "Smith", "Smith"],
    # "username": ["john.smith", "jane.smith", "bob.smith", "karen.smith"],
    # "email": ["john.smith@ams.com", "jane.smith@ams.com", "bob.smith@ams.com", "karen.smith@ams.com"],
    # "dob": ["01/11/2000", "02/12/1995", "06/07/2003", "04/09/2002"],
    # "active": [True, False, True, True],
    "uuid": ["update1", "update2", "update3", "update4"],
    # "known_as": ["Johnny", "", "", ""],
    # "middle_names": ["Michael", "", "", ""],
    # "language": ["English", "English", "English", "English"],
    # "sidebar_width": ["250px", "", "", ""],
    # "sex": ["Male", "Female", "Male", "Female"]
}
users_df = DataFrame(users_data)

failed_updates = update_user_uuid(
    mapping_df=users_df,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)