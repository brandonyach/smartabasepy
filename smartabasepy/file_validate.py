from pandas import DataFrame
from .utils import AMSError, _raise_ams_error


def _validate_file_df(df: DataFrame, user_key: str, require_attachment_id: bool = True) -> None:
    """Validate the file DataFrame for required columns and duplicates.

    Args:
        df (DataFrame): The DataFrame to validate.
        user_key (str): The user identifier column name (e.g., 'username').
        require_attachment_id (bool): Whether to require the 'attachment_id' column (default: True).

    Raises:
        AMSError: If required columns are missing or duplicates are found.
    """
    required_columns = [user_key, "file_name"]
    if require_attachment_id:
        required_columns.append("attachment_id")
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        _raise_ams_error(f"Missing required columns in file_df: {missing_columns}", function="validate_file_df")

    # Check for duplicates in file_name and attachment_id
    if df["file_name"].duplicated().any():
        duplicates = df[df["file_name"].duplicated(keep=False)]["file_name"].tolist()
        _raise_ams_error(f"Duplicate file names found in file_df: {duplicates}", function="validate_file_df")
    if require_attachment_id and df["attachment_id"].duplicated().any():
        duplicates = df[df["attachment_id"].duplicated(keep=False)]["attachment_id"].tolist()
        _raise_ams_error(f"Duplicate attachment IDs found in file_df: {duplicates}", function="validate_file_df")