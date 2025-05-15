import os
from pandas import DataFrame
from pathlib import Path
from typing import Optional, List, Tuple
from .utils import AMSError


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
        AMSError(f"Missing required columns in file_df: {missing_columns}", function="validate_file_df")

    # Check for duplicates in file_name and attachment_id
    if df["file_name"].duplicated().any():
        duplicates = df[df["file_name"].duplicated(keep=False)]["file_name"].tolist()
        AMSError(f"Duplicate file names found in file_df: {duplicates}", function="validate_file_df")
    if require_attachment_id and df["attachment_id"].duplicated().any():
        duplicates = df[df["attachment_id"].duplicated(keep=False)]["attachment_id"].tolist()
        AMSError(f"Duplicate attachment IDs found in file_df: {duplicates}", function="validate_file_df")
        
        

def _validate_output_directory(output_dir: Optional[str], function: str) -> str:
    """Validate and prepare the output directory for file operations.

    Args:
        output_dir (Optional[str]): The directory to validate. If None, uses the current working directory.
        function (str): The name of the calling function for error reporting.

    Returns:
        str: The validated directory path.

    Raises:
        AMSError: If the directory is not writable or cannot be created.
    """
    if output_dir is None:
        output_dir = os.getcwd()
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        elif not os.access(output_dir, os.W_OK):
            raise AMSError(
                f"Directory '{output_dir}' is not writable. Please specify a writable directory.",
                function=function
            )
    except OSError as e:
        raise AMSError(
            f"Failed to create or access directory '{output_dir}': {str(e)}. Ensure the path is valid and writable.",
            function=function
        )
    return output_dir



def _validate_file_path(file_path: str, function: str) -> List[Tuple[Path, str]]:
    """Validate the file path and return a list of files to upload.

    Args:
        file_path (str): The path to a single file or directory containing files.
        function (str): The name of the calling function for error reporting.

    Returns:
        List[Tuple[Path, str]]: A list of tuples containing the resolved Path and file name.

    Raises:
        AMSError: If the path does not exist or contains no files.
    """
    path = Path(file_path).resolve()
    if not path.exists():
        raise AMSError(
            f"Path '{path}' does not exist",
            function=function
        )
    if path.is_file():
        files_to_upload = [(path, path.name)]
    else:
        files_to_upload = [(f, f.name) for f in path.iterdir() if f.is_file()]
    if not files_to_upload:
        raise AMSError(
            f"No files found at '{path}'",
            function=function
        )
    return files_to_upload