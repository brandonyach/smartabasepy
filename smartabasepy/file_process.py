from pandas import DataFrame
from typing import List, Dict
from .utils import AMSError, _raise_ams_error


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
        _raise_ams_error("upload_results is empty or does not contain 'file_name' column", function="merge_upload_results")

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