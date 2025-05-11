from pandas import DataFrame
from typing import Dict, Union, Optional
from .form_option import FormOption
from .utils import AMSClient, get_client, _raise_ams_error
from .form_fetch import _fetch_form_id_and_type, _fetch_form_schema
from .form_print import _print_forms_status
from .form_process import _parse_forms_response, _parse_form_schema, _create_forms_df, _format_form_summary

def get_forms(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[FormOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Fetch a list of forms the user has access to from an AMS instance.

    Retrieves metadata about accessible forms, including their IDs, names, and types (e.g., event, profile).

    Args:
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[FormOption]): A FormOption object for customization (e.g., cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame with columns 'form_id', 'form_name', 'type', etc., for all accessible forms.

    Raises:
        AMSError: If the API request fails or no forms are found.
    """
    option = option or FormOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    if option.interactive_mode:
        print("ℹ Requesting list of accessible forms...")
    
    data = client._fetch("forms/summaries", method="GET", cache=option.cache, api_version="v3")
    if not isinstance(data, dict) or all(data.get(key) is None for key in ["event", "linkedOnlyEvent", "linkedOnlyProfile"]):
        _raise_ams_error("No valid forms data returned from server", 
                           function="get_forms", endpoint="forms/summaries")
    
    forms = _parse_forms_response(data)
    if not forms:
        _raise_ams_error("No accessible forms found", function="get_forms", endpoint="forms/summaries")
    
    forms_df = _create_forms_df(forms)
    
    _print_forms_status(forms_df, option)
    
    return forms_df




def get_form_schema(
    form_name: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[FormOption] = None,
    client: Optional[AMSClient] = None
) -> Union[str, Dict]:
    """Fetch the schema of a specific form from an AMS instance and print a formatted text summary.

    Retrieves the schema for a form identified by its name, including details about its sections,
    required fields, fields that default to the last known value, linked fields, and form item types.
    The form ID and type are retrieved internally using the get_forms function. The output is formatted
    as a human-readable text string with headers and structured information, printed directly to the console
    for proper rendering in Jupyter notebooks, unless raw_output is True.

    Args:
        form_name (str): The name of the form to retrieve the schema for.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[FormOption]): A FormOption object for customization (e.g., cache, interactive mode, raw_output).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        Union[str, Dict]: If option.raw_output is True, returns the raw API response as a dictionary.
                          Otherwise, returns the formatted text string summarizing the form schema, including:
                          - Form details (name and ID).
                          - Sections (count and names).
                          - Required fields (count and list of field names).
                          - Fields that default to the last known value (count and list of field names).
                          - Linked fields (count and list of field names).
                          - Form item types (count of unique types, count per type, and field names per type).
                          If option.include_instructions is True, includes instructions for sections and fields.
                          If option.field_details is True, includes field details (options, scores, dateSelection).

    Raises:
        AMSError: If the API request fails, the form is not found, or the response is invalid.
    """
    option = option or FormOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    if not form_name:
        _raise_ams_error("Form name is required", function="get_form_summary")
    
    # Step 1: Fetch the form ID and type
    form_id, form_type = _fetch_form_id_and_type(form_name, url, username, password, option, client)
    
    # Step 2: Fetch the form schema
    if option.interactive_mode:
        print(f"ℹ Fetching summary for form '{form_name}' (ID: {form_id}, Type: {form_type})...")
    
    schema_data = _fetch_form_schema(form_id, form_type, client, option)
    
    # Step 3: If raw_output is True, return the raw schema data
    if option.raw_output:
        return schema_data
    
    # Step 4: Parse the schema
    schema_info = _parse_form_schema(schema_data)
    
    # Step 5: Format and print the summary
    if option.interactive_mode:
        print(f"✔ Retrieved summary for form '{form_name}'.")
    
    formatted_output = _format_form_summary(
        schema_info,
        include_instructions=option.include_instructions,
        field_details=option.field_details
    )
    print(formatted_output)
    
    return formatted_output