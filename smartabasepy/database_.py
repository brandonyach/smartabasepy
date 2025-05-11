from pandas import DataFrame
from typing import Optional, Dict, Union
from .database_fetch import _fetch_database_data
from .database_process import _process_database_entries
from .form_ import _fetch_form_id_and_type
from .form_option import FormOption
from .utils import AMSClient, AMSError, get_client, _raise_ams_error


def get_database(
    form_name: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    limit: int = 10000,
    offset: int = 0,
    option: Optional[FormOption] = None,
    client: Optional[AMSClient] = None
) -> Union[DataFrame, Dict]:
    """Retrieve database entries from an AMS database form.

    Fetches entries from a specified database form, using the provided form name to look up the form ID.
    Returns a DataFrame containing the database entries, or the raw API response if raw_output is True.

    Args:
        form_name (str): The name of the AMS database form to retrieve data from.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        limit (int): The maximum number of entries to return (default: 100).
        offset (int): The offset of the first item to return (default: 0).
        option (Optional[FormOption]): A FormOption object for customization (e.g., cache, interactive mode, raw_output).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        Union[DataFrame, Dict]: A DataFrame containing database entries with columns such as 'id', 'form_name', and form-specific fields.
                                If option.raw_output is True, returns the raw API response as a dictionary.

    Raises:
        AMSError: If the API request fails, the form is not found, or the response is invalid.
        ValueError: If limit or offset is invalid.
    """
    option = option or FormOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    if not form_name:
        _raise_ams_error("Form name is required", function="get_database")
    
    if limit < 1:
        raise ValueError("Limit must be a positive integer.")
    if offset < 0:
        raise ValueError("Offset must be a non-negative integer.")
    
    # Step 1: Fetch the form ID and type using get_forms
    form_id, form_type = _fetch_form_id_and_type(form_name, url, username, password, option, client)
    
    # Ensure the form is a database form
    if form_type != "database":
        _raise_ams_error(f"Form '{form_name}' is not a database form (type: {form_type})", function="get_database")
    
    # Step 2: Fetch the database entries
    if option.interactive_mode:
        print(f"ℹ Fetching database entries for form '{form_name}' (ID: {form_id})...")
    
    response = _fetch_database_data(form_id, limit, offset, client, cache=option.cache)
    
    # Step 3: Process the response into a DataFrame
    df = _process_database_entries(response, form_name, option)
    
    # Step 4: Print status and return
    if isinstance(df, DataFrame) and option.interactive_mode:
        if df.empty:
            print(f"ℹ No database entries found for form '{form_name}' with limit {limit} and offset {offset}.")
        else:
            print(f"✔ Retrieved {len(df)} database entries for form '{form_name}'.")
    
    return df, len(df)

def delete_database_entry(
    database_entry_id: int,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    client: Optional[AMSClient] = None
) -> bool:
    """Delete a specific database entry from an AMS instance.

    Deletes a database entry identified by its ID using the /api/v2/userdefineddatabase/delete endpoint.

    Args:
        database_entry_id (int): The ID of the database entry to delete.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        bool: True if the deletion was successful, False otherwise.

    Raises:
        AMSError: If the API request fails or the response indicates an error.
        ValueError: If the database_entry_id is invalid.
    """
    client = client or get_client(url, username, password, cache=True, interactive_mode=False)
    
    if not isinstance(database_entry_id, int) or database_entry_id < 0:
        raise ValueError("database_entry_id must be a non-negative integer.")
    
    # Construct the payload
    payload = {"id": database_entry_id}
    
    # Make the API call to delete the entry
    response = client._fetch(
        "userdefineddatabase/delete",
        method="POST",
        payload=payload,
        cache=False,
        api_version="v2"
    )
    
    # The API returns "null" on success
    if response is None:
        return True
    
    # If the response is not null, it indicates an error
    raise AMSError(f"Failed to delete database entry with ID {database_entry_id}: {response}")
