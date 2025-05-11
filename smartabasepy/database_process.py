from pandas import DataFrame
from typing import Dict, Union
from .form_option import FormOption
from .utils import AMSError


def _process_database_entries(
    response: Dict,
    form_name: str,
    option: FormOption
) -> Union[DataFrame, Dict]:
    """Process the raw database entries response into a DataFrame.

    Args:
        response (Dict): The raw API response from the findTableByDatabaseFormId endpoint.
        form_name (str): The name of the database form.
        option (FormOption): The FormOption object for configuration.

    Returns:
        Union[DataFrame, Dict]: A DataFrame containing the processed database entries, or the raw response if raw_output is True.

    Raises:
        AMSError: If the response format is invalid or contains an error.
    """
    if not response:
        if option.interactive_mode:
            print(f"ℹ Debug: Raw response is empty for form '{form_name}'.")
        raise AMSError(f"No database entries found for form '{form_name}'")

    # Check for error in the response
    if response.get("error", False):
        error_message = response.get("message", "Unknown error")
        raise AMSError(f"Failed to fetch database entries for form '{form_name}': {error_message}")

    # Return raw response if requested
    if option.raw_output:
        return response

    # Extract the value section of the response
    value = response.get("value", {})
    
    # Extract entries, IDs, and field mappings
    entries = value.get("rows", [])
    entry_ids = value.get("ids", [])
    index_to_name = value.get("indexToName", {})
    
    if not isinstance(entries, list):
        if option.interactive_mode:
            print(f"ℹ Debug: Expected a list of entries, got: {type(entries)}. Full response: {response}")
        entries = []

    if not entries:
        if option.interactive_mode:
            print(f"ℹ Debug: No entries found in response for form '{form_name}'. Full response: {response}")
        return DataFrame()

    # Ensure the number of IDs matches the number of entries
    if len(entry_ids) != len(entries):
        if option.interactive_mode:
            print(f"ℹ Debug: Mismatch between number of IDs ({len(entry_ids)}) and entries ({len(entries)}). Full response: {response}")
        entry_ids = [None] * len(entries)  # Fallback to None if IDs don't match

    # Process each entry into a row
    rows = []
    for entry_idx, entry in enumerate(entries):
        if not isinstance(entry, list):
            if option.interactive_mode:
                print(f"ℹ Debug: Skipping invalid entry format at index {entry_idx}: {entry}")
            continue
        
        row = {
            "id": entry_ids[entry_idx] if entry_idx < len(entry_ids) else None,
            "form_name": form_name
        }
        # Map values to field names using indexToName
        for col_idx, value in enumerate(entry):
            field_name = index_to_name.get(str(col_idx), f"field_{col_idx}")
            row[field_name] = value
        rows.append(row)

    df = DataFrame(rows)
    if df.empty and option.interactive_mode:
        print(f"ℹ Debug: Processed DataFrame is empty for form '{form_name}'. Full response: {response}")
    
    return df