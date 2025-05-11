import pandas as pd
from pandas import DataFrame
from typing import Optional, List, Dict
from tqdm import tqdm
from .utils import AMSClient, AMSError, get_client, _raise_ams_error
from .user_build import _build_group_payload, _build_user_save_payload, _build_user_edit_payload
from .user_fetch import _fetch_user_data, _fetch_user_save, _get_all_user_data
from .user_clean import _clean_user_data, _transform_group_data, _clean_user_data_for_save, _get_update_columns
from .user_process import _filter_by_about, _flatten_user_response, _match_users_to_mapping, _map_user_updates, _process_users
from .user_print import _print_user_status, _print_group_status, _report_user_results
from .user_filter import UserFilter
from .user_option import UserOption, GroupOption
from .user_validate import _validate_user_data_for_edit, _validate_user_data_for_save

# from .user_utils import _fetch_user_data, _flatten_user_response, _filter_by_about, _build_group_payload, _build_user_save_payload, _get_all_user_ids, _print_user_status, _print_group_status, _fetch_user_save, _process_users, _report_user_results, _build_user_edit_payload, _get_all_user_data, _get_update_columns



def get_user(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    filter: Optional[UserFilter] = None,
    option: Optional[UserOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Retrieve user data from an AMS instance.

    Fetches user data based on optional filters (e.g., by username, email, group, or about field).
    Returns a DataFrame with user details such as user ID, name, email, and group affiliations.

    Args:
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        filter (Optional[UserFilter]): A UserFilter object to narrow results (e.g., by username, email, group, about).
        option (Optional[UserOption]): A UserOption object for customization (e.g., columns, cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing user data with columns such as 'user_id', 'first_name', 'last_name', etc.

    Raises:
        AMSError: If the API request fails or no users are found.
    """
    option = option or UserOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    filter_type = filter.user_key if filter else None
    
    if option.interactive_mode:
        print("ℹ Fetching user data...")
    
    # Fetch and process user data
    data = _fetch_user_data(client, filter, cache=option.cache)
    user_df = pd.DataFrame(_flatten_user_response(data))
    
    # Apply about filter post-fetch
    if filter and filter.user_key == "about" and filter.user_value:
        user_df = _filter_by_about(user_df, filter.user_value)
    
    user_df = _clean_user_data(user_df, option.columns, filter_type)
    _print_user_status(user_df, option)
    
    return user_df



def get_group(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[GroupOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Retrieve group data from an AMS instance.

    Fetches a list of groups accessible to the authenticated user and returns a DataFrame with group names.

    Args:
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[GroupOption]): A GroupOption object for customization (e.g., guess_col_type, cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing group data with a 'name' column.

    Raises:
        AMSError: If the API request fails or no groups are found.
    """
    option = option or GroupOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    payload = _build_group_payload(username)
    
    if option.interactive_mode:
        print("ℹ Fetching group data...")
    
    data = client._fetch("listgroups", method="POST", payload=payload, cache=option.cache, api_version="v1")
    
    if not data or "name" not in data or not data["name"]:
        _raise_ams_error("No groups returned from server", function="get_group", endpoint="listgroups")
    
    group_df = pd.DataFrame({"name": data["name"]})
    
    group_df = _transform_group_data(group_df, option.guess_col_type)
    
    _print_group_status(group_df, option)
    
    return group_df



def edit_user(
    mapping_df: DataFrame,
    user_key: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UserOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Update user fields in an AMS instance using /api/v2/person/save.

    Updates specified fields for users identified by user_key, preserving unchanged fields. Accepts a DataFrame
    with a user_key column and any combination of updatable columns. Empty values are uploaded as empty strings.

    Args:
        mapping_df (DataFrame): A DataFrame containing:
            - A column with user identifiers (e.g., 'username', 'email', 'about', 'uuid').
            - Any updatable columns (e.g., 'first_name', 'last_name', 'email', 'dob', 'sex', 'active', 'uuid').
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

    # Step 1: Validate input
    _validate_user_data_for_edit(mapping_df, user_key)

    # Step 2: Fetch all user data
    user_values = mapping_df[user_key].unique().tolist()
    if option.interactive_mode:
        print(f"ℹ Retrieving user data for {len(user_values)} users using {user_key}...")

    try:
        user_df = _get_all_user_data(
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
            "user_id": [None] * len(mapping_df),
            "user_key": mapping_df[user_key],
            "reason": f"Failed to retrieve user data: {str(e)}"
        })

    if user_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No users found")
        return DataFrame({
            "user_id": [None] * len(mapping_df),
            "user_key": mapping_df[user_key],
            "reason": "No users found"
        })

    # Step 3: Match users to mapping
    mapping_df, failed_matches = _match_users_to_mapping(
        mapping_df,
        user_df,
        user_key,
        interactive_mode=option.interactive_mode
    )
    failed_df = failed_matches.rename(columns={"user_key": "user_key"})

    if mapping_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No users matched for update")
        return failed_df

    # Step 4: Clean the mapping DataFrame
    df = _clean_user_data_for_save(mapping_df, preserve_columns=[user_key, "user_id"])

    # Step 5: Get update columns
    column_mapping, update_columns = _get_update_columns(df, user_key)

    if not update_columns:
        if option.interactive_mode:
            print(f"⚠️ No updatable columns provided in mapping_df")
        return failed_df

    # Step 6: Process updates
    if option.interactive_mode:
        print(f"ℹ Updating {len(df)} users...")

    def payload_builder(row: pd.Series) -> Dict:
        return _build_user_edit_payload(row, user_df, {k: v for k, v in column_mapping.items() if k in update_columns})

    failed_operations, user_ids = _process_users(
        df,
        client,
        payload_builder,
        _fetch_user_save,
        user_key,  # Use user_key for reporting
        interactive_mode=option.interactive_mode
    )

    # Step 7: Map user_ids back to user_key for reporting
    failed_operations_with_user_key = []
    for op in failed_operations:
        user_key_val = op["user_key"]
        user_id_row = mapping_df[mapping_df[user_key] == user_key_val]
        user_id = user_id_row["user_id"].iloc[0] if not user_id_row.empty else None
        failed_operations_with_user_key.append({
            "user_id": user_id,
            "user_key": user_key_val,
            "reason": op["reason"]
        })

    # Step 8: Report results
    failed_df = pd.concat([failed_df, DataFrame(failed_operations_with_user_key)], ignore_index=True)
    return _report_user_results(len(df), failed_operations_with_user_key, user_ids, "updated", interactive_mode=option.interactive_mode)



def create_user(
    user_df: DataFrame,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UserOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Create new users in an AMS instance using /api/v2/person/save.

    Creates new users by setting id to "-1" and allows control over the active status. Accepts a DataFrame
    where each row represents a user with required fields.

    Args:
        user_df (DataFrame): A DataFrame containing user data. Required columns: 'first_name', 'last_name',
                              'username', 'email', 'dob', 'password', 'active'. Optional columns: 'uuid',
                              'known_as', 'middle_names', 'language', 'sidebar_width', 'sex'.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[UserOption]): A UserOption object for customization (e.g., cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing failed creations with columns ['user_key', 'reason'].

    Raises:
        AMSError: If the DataFrame is missing required columns or the API request fails.
    """
    option = option or UserOption(interactive_mode=True)
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)

    # Validate the DataFrame
    _validate_user_data_for_save(user_df)

    # Clean and prepare the user data
    df = _clean_user_data_for_save(user_df)

    # Process users
    failed_operations, user_ids = _process_users(
        df,
        client,
        _build_user_save_payload,
        _fetch_user_save,
        "username",
        interactive_mode=option.interactive_mode
    )

    # Report results
    return _report_user_results(len(df), failed_operations, user_ids, "created", interactive_mode=option.interactive_mode)