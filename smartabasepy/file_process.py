import pandas as pd
from pandas import DataFrame
from typing import List, Dict, Tuple
from .user_fetch import _fetch_user_ids
from .utils import AMSError, AMSClient


def _merge_upload_results(file_df: DataFrame, upload_results: List[Dict]) -> DataFrame:
    """Merge upload results into the file DataFrame.

    Args:
        file_df (DataFrame): The DataFrame with user_key, file_name, and attachment_id.
        upload_results (List[Dict]): The results from upload_files, containing 'file_name', 'file_id', and 'server_file_name'.

    Returns:
        DataFrame: The merged DataFrame with file_id and server_file_name added.

    Raises:
        AMSError: If upload_results is empty or doesn't contain required columns.
    """
    upload_results_df = DataFrame(upload_results)
    if upload_results_df.empty or "file_name" not in upload_results_df.columns:
        AMSError("upload_results is empty or does not contain 'file_name' column", function="merge_upload_results")

    file_df = file_df.merge(
        upload_results_df[["file_name", "file_id", "server_file_name"]],
        on="file_name",
        how="left"
    )
    return file_df



def _format_file_reference(file_df: DataFrame, file_field_name: str) -> DataFrame:
    """Format the file reference in the format 'file_id|server_file_name'.

    Args:
        file_df (DataFrame): The DataFrame containing file_id and server_file_name.
        file_field_name (str): The name of the file upload field (e.g., 'Upload Attachment').

    Returns:
        DataFrame: The DataFrame with the formatted file reference column.
    """
    file_df = file_df.copy()
    file_df[file_field_name] = file_df["file_id"].astype(str) + "|" + file_df["server_file_name"]
    return file_df



def _map_user_ids_to_file_df(
    file_df: DataFrame,
    user_key: str,
    client: AMSClient,
    interactive_mode: bool,
    cache: bool
) -> Tuple[DataFrame, DataFrame]:
    """Map user IDs to a file DataFrame and return updated DataFrame and failed mappings.

    Args:
        file_df (DataFrame): DataFrame with user identifiers and file data.
        user_key (str): The user identifier column in file_df.
        client (AMSClient): The authenticated AMSClient instance.
        interactive_mode (bool): Whether to print status messages.
        cache (bool): Whether to cache API responses.

    Returns:
        Tuple[DataFrame, DataFrame]: Updated file_df with user_id, and failed_df with unmapped users.
    """
    failed_df = DataFrame(columns=["user_id", "file_name", "reason"])
    
    user_values = file_df[user_key].unique().tolist()
    
    if interactive_mode:
        print(f"ℹ Retrieving user IDs for {len(user_values)} users using {user_key}...")
    try:
        user_ids, user_data = _fetch_user_ids(client=client, cache=cache)
    except AMSError as e:
        if interactive_mode:
            print(f"⚠️ Failed to retrieve user data: {str(e)}")
        return file_df, pd.concat([
            failed_df,
            DataFrame({
                "user_id": file_df[user_key],
                "file_name": file_df["file_name"],
                "reason": f"Failed to retrieve user data: {str(e)}"
            })
        ], ignore_index=True)
    if not user_ids:
        if interactive_mode:
            print(f"⚠️ No users found for the provided {user_key} values")
        return file_df, pd.concat([
            failed_df,
            DataFrame({
                "user_id": file_df[user_key],
                "file_name": file_df["file_name"],
                "reason": f"No users found for the provided {user_key} values"
            })
        ], ignore_index=True)
    if user_data is None:
        user_data = DataFrame()
    user_data = user_data.rename(columns={"userId": "user_id"})
    file_df = file_df.merge(
        user_data[["user_id", user_key]],
        on=user_key,
        how="left"
    )
    unmapped = file_df[file_df["user_id"].isna()]
    if not unmapped.empty:
        failed_df = pd.concat([
            failed_df,
            DataFrame({
                "user_id": unmapped[user_key],
                "file_name": unmapped["file_name"],
                "reason": f"User not found for {user_key} value"
            })
        ], ignore_index=True)
        file_df = file_df[file_df["user_id"].notna()]
    return file_df, failed_df