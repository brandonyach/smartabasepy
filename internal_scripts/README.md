Internal Scripts for SmartabasePy
These scripts are for internal use only and should not be distributed publicly. They provide functionality for sensitive operations on Smartabase user data, leveraging the smartabasepy package.
Dependencies

smartabasepy package (install via pip install smartabasepy or locally with pip install -e . from the project root).

Scripts
1. deactivate_users.py

Purpose: Deactivates users by setting active to False via the /api/v2/person/save endpoint.
Input: A DataFrame with a user identifier column (e.g., about, email).
Output: A DataFrame of failed deactivations with columns user_id, user_key, reason.

2. update_user_passwords.py

Purpose: Attempts to update user passwords via the /api/v2/person/save endpoint.
Input: A DataFrame with a user identifier column and a password column for new passwords.
Output: A DataFrame of failed updates with columns user_id, user_key, reason.
Note: This script may not work due to potential restrictions on updating passwords via /api/v2/person/save. The endpoint accepts requests but does not update passwords. Clarification from Smartabase support is pending.

3. update_user_date_of_birth.py

Purpose: Updates user date of birth via the /api/v2/person/save endpoint.
Input: A DataFrame with a user identifier column and a date_of_birth column (format: DD/MM/YYYY).
Output: A DataFrame of failed updates with columns user_id, user_key, reason.

4. update_user_email_address.py

Purpose: Updates user email address via the /api/v2/person/save endpoint.
Input: A DataFrame with a user identifier column and an email_address column.
Output: A DataFrame of failed updates with columns user_id, user_key, reason.

5. update_user_uuid.py

Purpose: Updates user UUID via the /api/v2/person/save endpoint.
Input: A DataFrame with a user identifier column and a uuid column.
Output: A DataFrame of failed updates with columns user_id, user_key, reason.

Usage
Each script follows a similar interface. Example for update_user_date_of_birth.py:
from pandas import DataFrame
from internal_scripts.update_user_date_of_birth import update_user_date_of_birth
from smartabasepy.user_option import UserOption

# Create mapping DataFrame
mapping_data = {
    "about": ["Riley Jones", "Samantha Fields"],
    "date_of_birth": ["01/01/1990", "02/02/1985"]
}
mapping_df = DataFrame(mapping_data)

# Update date of birth
failed_updates = update_user_date_of_birth(
    mapping_df=mapping_df,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)

print("Failed updates:")
print(failed_updates)

Replace the script name and input columns as needed for other scripts.
Security

Storage: Store these scripts in a private repository or local directory, excluded from public distribution (e.g., not included in setup.py for PyPI).
Access: Restrict access to authorized team members, as these scripts perform sensitive operations (deactivating users, updating personal data).
Testing: Test in a sandbox environment to avoid unintended changes to production data.

Known Issues

Password Updates: The update_user_passwords.py script may not update passwords due to potential restrictions in the /api/v2/person/save endpoint. Awaiting clarification from Smartabase support.

Extending Scripts
To add new internal operations (e.g., updating other user fields):

Create a new script in internal_scripts/ following the pattern of existing scripts.
Use smartabasepy helpers (get_all_user_data, _match_users_to_mapping, _prepare_user_payload, _update_user).
Update this README with the new script’s details.

