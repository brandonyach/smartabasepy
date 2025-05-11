from typing import Optional
from pandas import DataFrame
from .utils import AMSClient, get_client, AMSError
from .import_option import InsertEventOption, UpdateEventOption, UpsertEventOption, UpsertProfileOption
from .import_build import _build_import_payload, _build_profile_payload
from .import_clean import _clean_import_df, _clean_profile_df
from .import_fetch import _fetch_import_payloads
from .import_print import _print_import_status
from .import_process import _map_id_col_to_user_id
from .import_validate import _validate_import_df


def insert_event_data(
    df: DataFrame,
    form: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[InsertEventOption] = None,
    client: Optional[AMSClient] = None
) -> None:
    
    """Imports events into an AMS Event Form.

    This function inserts new event data into a specified AMS Event Form. It processes
    the input DataFrame, maps user identifiers to user IDs, validates the data, builds
    the payload, and sends it to the AMS API for insertion.

    Args:
        df: DataFrame containing the event data to insert.
        form: The name of the AMS Event Form to insert data into.
        url: The URL of the AMS instance (e.g., "https://example.smartabase.com/site").
        username: The username for authentication. If None, uses environment variable AMS_USERNAME.
        password: The password for authentication. If None, uses environment variable AMS_PASSWORD.
        option: An InsertEventOption object specifying options like id_col, interactive_mode,
            cache, and table_fields. If None, defaults are used.
        client: An optional AMSClient instance. If None, a new client is created.

    Raises:
        AMSError: If authentication fails, the payload cannot be built, or the API request fails.
    """
    
    option = option or InsertEventOption()
    
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    df_clean = _clean_import_df(df)
    
    df_clean = _map_id_col_to_user_id(df_clean, option.id_col, client)
    
    _validate_import_df(df_clean, form, overwrite_existing=False, table_fields=option.table_fields)
    
    entered_by_user_id = client.login_data["user"]["id"]
    
    payloads = _build_import_payload(
        df_clean,
        form,
        option.table_fields,
        entered_by_user_id,
        overwrite_existing=False
    )
    
    event_count = len(payloads[0]["events"])
    if option.interactive_mode:
        print(f"ℹ Inserting {event_count} events for '{form}'")
    
    results = _fetch_import_payloads(client, payloads, "insert", option.interactive_mode, option.cache)
    
    if option.interactive_mode:
        print(f"✔ Processed {event_count} events for '{form}'")
    
    _print_import_status(results, form, "inserted", option.interactive_mode)



def update_event_data(
    df: DataFrame,
    form: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UpdateEventOption] = None,
    client: Optional[AMSClient] = None
) -> None:
    
    """Update existing event records within an AMS Event Form.

    This function updates existing event data in a specified AMS Event Form. It processes
    the input DataFrame, maps user identifiers to user IDs, validates the data, builds
    the payload, and sends it to the AMS API for updating. If interactive_mode is True,
    it prompts the user to confirm the update operation.

    Args:
        df: DataFrame containing the event data to update, including an 'event_id' column.
        form: The name of the AMS Event Form to update data in.
        url: The URL of the AMS instance (e.g., "https://example.smartabase.com/site").
        username: The username for authentication. If None, uses environment variable AMS_USERNAME.
        password: The password for authentication. If None, uses environment variable AMS_PASSWORD.
        option: An UpdateEventOption object specifying options like id_col, interactive_mode,
            cache, and table_fields. If None, defaults are used.
        client: An optional AMSClient instance. If None, a new client is created.

    Raises:
        AMSError: If authentication fails, the payload cannot be built, the API request fails,
            or the user cancels the update operation in interactive mode.
    """
    
    option = option or UpdateEventOption()
    
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    df_clean = _clean_import_df(df)
    
    df_clean = _map_id_col_to_user_id(df_clean, option.id_col, client)
    
    _validate_import_df(df_clean, form, overwrite_existing=True, table_fields=option.table_fields)
    
    entered_by_user_id = client.login_data["user"]["id"]
    
    payloads = _build_import_payload(
        df_clean,
        form,
        option.table_fields,
        entered_by_user_id,
        overwrite_existing=True
    )
    
    event_count = len(payloads[0]["events"])
    if option.interactive_mode:
        print(f"ℹ Updating {event_count} events for '{form}'")
        confirm = input(f"Are you sure you want to update {event_count} existing events in '{form}'? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes']:
            raise AMSError("Update operation cancelled by user.")
    
    results = _fetch_import_payloads(client, payloads, "update", option.interactive_mode, option.cache)
    
    if option.interactive_mode:
        print(f"✔ Processed {event_count} events for '{form}'")
    
    _print_import_status(results, form, "updated", option.interactive_mode)



def upsert_event_data(
    df: DataFrame,
    form: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UpsertEventOption] = None,
    client: Optional[AMSClient] = None
) -> None:
    
    """Inserts and updates event records in an AMS Event Form.
    
    This function performs an upsert operation, splitting the input DataFrame into updates
    (records with an 'event_id') and inserts (records without an 'event_id'). It processes
    each group separately, updating existing records and inserting new ones. If interactive_mode
    is True, it prompts the user to confirm the update operation for existing records.

    Args:
        df: DataFrame containing the event data to upsert. Records with an 'event_id' are updated;
            those without are inserted.
        form: The name of the AMS Event Form to upsert data into.
        url: The URL of the AMS instance (e.g., "https://example.smartabase.com/site").
        username: The username for authentication. If None, uses environment variable AMS_USERNAME.
        password: The password for authentication. If None, uses environment variable AMS_PASSWORD.
        option: An UpsertEventOption object specifying options like id_col, interactive_mode,
            cache, and table_fields. If None, defaults are used.
        client: An optional AMSClient instance. If None, a new client is created.

    Raises:
        AMSError: If authentication fails, the payload cannot be built, the API request fails,
            or the user cancels the update operation in interactive mode.
    """
    
    option = option or UpsertEventOption()
    
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    df_clean = _clean_import_df(df)
    
    df_clean = _map_id_col_to_user_id(df_clean, option.id_col, client)
    
    _validate_import_df(df_clean, form, overwrite_existing=True, table_fields=option.table_fields)
    
    entered_by_user_id = client.login_data["user"]["id"]
    
    # Split DataFrame into updates (with event_id) and inserts (without event_id)
    updates_df = df_clean[df_clean["event_id"].notna()]
    
    inserts_df = df_clean[df_clean["event_id"].isna()]
    
    all_results = []
    
    # Process updates
    if not updates_df.empty:
        update_payloads = _build_import_payload(
            updates_df,
            form,
            option.table_fields,
            entered_by_user_id,
            overwrite_existing=True
        )
        
        update_count = len(update_payloads[0]["events"])
        if option.interactive_mode:
            print(f"ℹ Updating {update_count} existing events for '{form}'")
            confirm = input(f"Are you sure you want to update {update_count} existing events in '{form}'? (y/n): ").strip().lower()
            if confirm not in ['y', 'yes']:
                raise AMSError("Update operation cancelled by user.")
            
        update_results = _fetch_import_payloads(client, update_payloads, "update", option.interactive_mode, option.cache)
        
        all_results.extend(update_results)
    
    # Process inserts
    if not inserts_df.empty:
        insert_payloads = _build_import_payload(
            inserts_df,
            form,
            option.table_fields,
            entered_by_user_id,
            overwrite_existing=False
        )
        
        insert_count = len(insert_payloads[0]["events"])
        if option.interactive_mode:
            print(f"ℹ Inserting {insert_count} new events for '{form}'")
            
        insert_results = _fetch_import_payloads(client, insert_payloads, "insert", option.interactive_mode, option.cache)
        
        all_results.extend(insert_results)
    
    if option.interactive_mode:
        total_events = len(df_clean)
        print(f"✔ Processed {total_events} events for '{form}'")
    
    _print_import_status(all_results, form, "upserted", option.interactive_mode)
    
    
    
def upsert_profile_data(
    df: DataFrame,
    form: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UpsertProfileOption] = None,
    client: Optional[AMSClient] = None
) -> None:
    """Upsert profile data in an AMS Profile Form.

    This function upserts profile data into a specified AMS Profile Form, updating
    existing records or inserting new ones. Profile forms allow only one record per user
    and do not support table fields or date/time fields. The input DataFrame is processed,
    user identifiers are mapped to user IDs, the data is validated, and the payload is sent
    to the AMS API for upserting. If interactive_mode is True, it prompts the user
    to confirm the operation.

    Args:
        df: DataFrame containing the profile data to upsert.
        form: The name of the AMS Profile Form to upsert data into.
        url: The URL of the AMS instance (e.g., "https://example.smartabase.com/site").
        username: The username for authentication. If None, uses environment variable AMS_USERNAME.
        password: The password for authentication. If None, uses environment variable AMS_PASSWORD.
        option: An UpsertProfileOption object specifying options like id_col, interactive_mode,
            and cache. If None, defaults are used.
        client: An optional AMSClient instance. If None, a new client is created.

    Raises:
        AMSError: If authentication fails, the payload cannot be built, the API request fails,
            or the user cancels the operation in interactive mode.
    """
    option = option or UpsertProfileOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    df_clean = _clean_profile_df(df)
    df_clean = _map_id_col_to_user_id(df_clean, option.id_col, client)
    
    _validate_import_df(df_clean, form, overwrite_existing=False, table_fields=None)
    
    entered_by_user_id = client.login_data["user"]["id"]
    
    payload = _build_profile_payload(
        df_clean,
        form,
        entered_by_user_id
    )
    
    profile_count = len(payload)
    if option.interactive_mode:
        print(f"ℹ Upserting {profile_count} profile records for '{form}'")
        confirm = input(f"Are you sure you want to upsert {profile_count} profile records in '{form}'? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes']:
            raise AMSError("Upsert operation cancelled by user.")
    
    results = _fetch_import_payloads(client, payload, "upsert", option.interactive_mode, option.cache, is_profile=True)
    
    if option.interactive_mode:
        print(f"✔ Processed {profile_count} profile records for '{form}'")
    
    _print_import_status(results, form, "upserted", option.interactive_mode)