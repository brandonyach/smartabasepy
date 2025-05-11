import pandas as pd
from pandas import DataFrame
from typing import Optional, Dict, List, Union, Tuple, Callable
from tqdm import tqdm
from .utils import AMSClient, AMSError


def _flatten_groups_and_roles(df: DataFrame) -> DataFrame:
    """Flatten the 'groupsAndRoles' column into separate columns for single-user data.

    Extracts roles, athlete groups, and coach groups from the 'groupsAndRoles' column and creates
    new columns 'role', 'athlete_group', and 'coach_group'. Drops the original 'groupsAndRoles' column.

    Args:
        df (DataFrame): The DataFrame containing user data with a 'groupsAndRoles' column.

    Returns:
        DataFrame: The DataFrame with new columns 'role', 'athlete_group', and 'coach_group'.
    """
    if "groupsAndRoles" not in df.columns or df.empty:
        return df
    
    row = df.iloc[0]
    groups_and_roles = row["groupsAndRoles"] if pd.notna(row["groupsAndRoles"]) else {}
    
    roles = groups_and_roles.get("role", [])
    role_names = "; ".join([r["name"] for r in roles if "name" in r]) if roles else ""
    
    athlete_groups = groups_and_roles.get("athleteGroups", [])
    athlete_group_names = "; ".join([g["name"] for g in athlete_groups if "name" in g]) if athlete_groups else ""
    
    coach_groups = groups_and_roles.get("coachGroups", [])
    coach_group_names = "; ".join([g["name"] for g in coach_groups if "name" in g]) if coach_groups else ""
    
    df["role"] = role_names
    df["athlete_group"] = athlete_group_names
    df["coach_group"] = coach_group_names
    
    df = df.drop(columns=["groupsAndRoles"])
    return df



def _flatten_user_response(data: Dict) -> List[Dict]:
    """Flatten nested user results from an API response into a list of dictionaries.

    Extracts user data from the nested 'results' structure in the API response.

    Args:
        data (Dict): The raw API response containing user data.

    Returns:
        List[Dict]: A list of dictionaries, each representing a user.
    """
    all_users = []
    for result_group in data["results"]:
        if "results" in result_group and result_group["results"]:
            all_users.extend(result_group["results"])
    return all_users



def _filter_by_about(
        df: DataFrame, 
        user_value: Union[str, List[str]]
    ) -> DataFrame:
    """Filter a DataFrame by exact 'about' values.

    Constructs an 'about' column by combining 'firstName' and 'lastName', then filters the DataFrame
    to include only rows where 'about' matches the specified value(s).

    Args:
        df (DataFrame): The DataFrame containing user data.
        user_value (Union[str, List[str]]): The value(s) to filter the 'about' column by.

    Returns:
        DataFrame: The filtered DataFrame containing only matching users.
    """
    df["about"] = df["firstName"] + " " + df["lastName"]
    filter_values = [v.strip() for v in (user_value if isinstance(user_value, list) else [user_value])]
    return df[df["about"].isin(filter_values)]


def _map_user_updates(
    row: pd.Series,
    user_data: Dict,
    column_mapping: Dict[str, str]
) -> Dict:
    """Map columns from a DataFrame row to API fields for updating a user.

    Args:
        row (pd.Series): A row from the mapping DataFrame containing update values.
        user_data (Dict): The existing user data from /api/v2/person/get to preserve unchanged fields.
        column_mapping (Dict[str, str]): Mapping of DataFrame columns to API field names (e.g., {'first_name': 'firstName'}).

    Returns:
        Dict: The updated user data dictionary with new values from the row.
    """
    updated_data = user_data.copy()
    for df_col, api_field in column_mapping.items():
        if df_col in row:
            if api_field == "active" and pd.notna(row[df_col]):
                # Preserve boolean type for active
                updated_data[api_field] = bool(row[df_col])
            elif pd.notna(row[df_col]):
                updated_data[api_field] = str(row[df_col])
            else:
                updated_data[api_field] = ""
    return updated_data



def _match_users_to_mapping(
    mapping_df: DataFrame,
    user_df: DataFrame,
    user_key: str,
    interactive_mode: bool = False
) -> tuple[DataFrame, DataFrame]:
    """Match users from user_df to mapping_df based on user_key.

    Args:
        mapping_df (DataFrame): DataFrame with user identifiers (e.g., 'username', 'email', 'about', 'uuid')
            and a secondary column (e.g., 'file_name' for file operations, 'date_of_birth' for updates).
        user_df (DataFrame): DataFrame with complete user data from /api/v2/person/get.
        user_key (str): The user identifier column in mapping_df ('username', 'email', 'about', or 'uuid').
        interactive_mode (bool): Whether to print status messages.

    Returns:
        tuple[DataFrame, DataFrame]: Updated mapping_df with user_id and user_key preserved, and failed_df with unmapped users.
            The failed_df has columns ['user_id', 'user_key', 'reason'], where 'user_key' is the value from user_key.
    """
    failed_df = DataFrame(columns=["user_id", "user_key", "reason"])
    mapping_df = mapping_df.copy()

    # Create key column for matching
    if user_key == "about":
        user_df = user_df.copy()
        user_df["about"] = user_df["firstName"].str.strip() + " " + user_df["lastName"].str.strip()
        user_df["about"] = user_df["about"].str.lower()  # Normalize case
        mapping_df[user_key] = mapping_df[user_key].str.strip().str.lower()  # Normalize case
        key_column = "about"
    elif user_key == "email":
        key_column = "emailAddress"
    else:
        key_column = user_key

    # Merge mapping_df with user_df to get user_id, preserving user_key
    mapping_df = mapping_df.merge(
        user_df[["id", key_column]].rename(columns={"id": "user_id"}),
        left_on=user_key,
        right_on=key_column,
        how="left"
    )

    # Debug: Log matched users and user_id types
    # if interactive_mode:
    #     print(f"ℹ Debug: Matched users: {mapping_df[['user_id', user_key]].to_dict('records')}")
    #     print(f"ℹ Debug: user_id type in mapping_df: {mapping_df['user_id'].dtype}")

    # Check for unmapped users
    unmapped = mapping_df[mapping_df["user_id"].isna()]
    if not unmapped.empty:
        failed_df = pd.concat([
            failed_df,
            DataFrame({
                "user_id": [None] * len(unmapped),
                "user_key": unmapped[user_key],
                "reason": f"User not found for {user_key} value"
            })
        ], ignore_index=True)
        mapping_df = mapping_df[mapping_df["user_id"].notna()]

    if mapping_df.empty and interactive_mode:
        print(f"⚠️ No users could be mapped to user_ids")

    return mapping_df, failed_df



def _process_users(
    df: DataFrame,
    client: AMSClient,
    payload_builder: Callable[[pd.Series], Dict],
    api_caller: Callable[[Dict, AMSClient, bool], Tuple[Dict, Optional[str]]],
    user_key_col: str,
    interactive_mode: bool = False
) -> Tuple[List[Dict], List[str]]:
    """Process a DataFrame of users, calling the API for each and collecting results.

    Args:
        df (DataFrame): The cleaned DataFrame containing user data.
        client (AMSClient): The AMSClient instance.
        payload_builder (Callable): Function to build the API payload from a DataFrame row.
        api_caller (Callable): Function to make the API call and return (response, user_id).
        user_key_col (str): The column name to use as the user identifier (e.g., 'username', 'user_id').
        interactive_mode (bool): Whether to print status messages.

    Returns:
        Tuple[List[Dict], List[str]]: List of failed operations (user_key, reason) and list of successful user_ids.
    """
    failed_operations = []
    user_ids = []

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing users", disable=not interactive_mode):
        try:
            user_data = payload_builder(row)
            user_key = str(row[user_key_col]) if user_key_col != "user_id" else str(row["user_id"])
            response, user_id = api_caller(user_data, client, interactive_mode)
            if user_id:
                user_ids.append(user_id)
        except AMSError as e:
            failed_operations.append({"user_key": user_key, "reason": str(e)})
            if interactive_mode:
                print(f"⚠️ Failed to process user {user_key}: {str(e)}")

    return failed_operations, user_ids



def _prepare_user_payload(
    user_data: Dict,
    updates: Dict
) -> Dict:
    """Prepare the user payload by applying updates to the user object.

    Args:
        user_data (Dict): Complete user object from /api/v2/person/get.
        updates (Dict): Dictionary of field updates (e.g., {'avatarId': '123', 'active': False}).

    Returns:
        Dict: Updated user object with new field values.
    """
    user_data = user_data.copy() 
    user_data.update(updates)
    return user_data