import pandas as pd
from pandas import DataFrame
from typing import Optional
from smartabasepy.utils import get_client, AMSError, AMSClient
from smartabasepy.user_ import get_all_user_data
from smartabasepy.user_option import UserOption
from smartabasepy.file_utils import _match_users_to_mapping, _prepare_user_payload, _update_user

def update_user_email_address(
    mapping_df: DataFrame,
    user_key: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UserOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Update user email address in an AMS instance.

    Uses complete user objects from /api/v2/person/get to preserve existing fields and updates
    the emailAddress field via the /api/v2/person/save endpoint. For internal use only.

    Args:
        mapping_df (DataFrame): A DataFrame containing:
            - A column with user identifiers (e.g., 'username', 'email', 'about', 'uuid').
            - An 'email_address' column with the new email address.
        user_key (str): The user identifier column in mapping_df ('username', 'email', 'about', or 'uuid').
        url (str): The URL of the AMS instance (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[UserOption]): A UserOption object for customization (e.g., cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing failed updates with columns ['user_id', 'user_key', 'reason'].

    Raises:
        AMSError: If the mapping_df is invalid, users cannot be retrieved, or the update operation fails.
    """
    option = option or UserOption(interactive_mode=True)
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)

    # Validate mapping_df and user_key
    from smartabasepy.file_validate import _validate_user_key
    _validate_user_key(user_key)
    if user_key not in mapping_df.columns:
        raise AMSError(f"Column '{user_key}' not found in mapping_df")
    if "email_address" not in mapping_df.columns:
        raise AMSError("Column 'email_address' not found in mapping_df")

    # Step 1: Fetch all user data
    user_values = mapping_df[user_key].unique().tolist()
    if option.interactive_mode:
        print(f"ℹ Retrieving user data for {len(user_values)} users using {user_key}...")

    try:
        user_df = get_all_user_data(
            url=url,
            username=username,
            password=password,
            user_ids=None,  # Fetch all users
            option=option,
            client=client
        )
    except AMSError as e:
        if option.interactive_mode:
            print(f"⚠️ Failed to retrieve user data: {str(e)}")
        return DataFrame({
            "user_id": mapping_df[user_key],
            "user_key": mapping_df[user_key],
            "reason": f"Failed to retrieve user data: {str(e)}"
        })

    if user_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No users found")
        return DataFrame({
            "user_id": mapping_df[user_key],
            "user_key": mapping_df[user_key],
            "reason": "No users found"
        })

    # Step 2: Match users to mapping
    mapping_df, failed_matches = _match_users_to_mapping(
        mapping_df,
        user_df,
        user_key,
        interactive_mode=option.interactive_mode
    )
    failed_df = failed_matches.rename(columns={"file_name": "user_key"})

    if mapping_df.empty:
        return failed_df

    # Step 3: Update each user's email address
    failed_operations = []
    if option.interactive_mode:
        print(f"ℹ Updating email address for {len(mapping_df)} users...")

    for _, row in mapping_df.iterrows():
        user_id = row["user_id"]
        new_email = row["email_address"]
        identifier = row[user_key]
        
        # Find the user’s complete object
        user_data = user_df[user_df["id"] == user_id].to_dict("records")
        if not user_data:
            failed_operations.append({
                "user_id": user_id,
                "user_key": identifier,
                "reason": f"User ID {user_id} not found in user data"
            })
            if option.interactive_mode:
                print(f"⚠️ Failed to update email address for user ID {user_id}: User not found")
            continue
        
        # Prepare payload
        user_data = _prepare_user_payload(user_data[0], {"emailAddress": new_email})
        
        # Update email address
        error_msg = _update_user(
            user_data,
            client,
            user_id,
            identifier,
            interactive_mode=option.interactive_mode
        )
        if error_msg:
            failed_operations.append({
                "user_id": user_id,
                "user_key": identifier,
                "reason": error_msg
            })

    # Step 4: Create DataFrame for failed operations
    failed_df = pd.concat([failed_df, DataFrame(failed_operations)], ignore_index=True)

    # Step 5: Report results
    if failed_df.empty:
        if option.interactive_mode:
            print(f"\n✔ Successfully updated email address for all {len(mapping_df)} users.")
    else:
        if option.interactive_mode:
            print(f"\n⚠️ Failed to update email address for {len(failed_df)} users:")
            print(failed_df.to_string(index=False))

    return failed_df