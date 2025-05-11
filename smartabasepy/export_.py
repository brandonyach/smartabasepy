import pandas as pd
from pandas import DataFrame
from datetime import datetime
from typing import Optional, Tuple
from tqdm import tqdm
from .utils import AMSClient, AMSError, get_client, _raise_ams_error
from .export_filter import EventFilter, SyncEventFilter, ProfileFilter
from .export_option import EventOption, SyncEventOption, ProfileOption
from .export_clean import _reorder_columns, _transform_event_data, _transform_profile_data, _sort_event_data, _sort_profile_data
from .export_process import _process_events_to_rows, _process_profile_rows, _append_user_data
from .export_validate import _validate_event_filter, _validate_dates
from .export_build import _build_event_payload, _build_sync_event_payload, _build_profile_payload
from .export_fetch import _fetch_user_ids
from .export_print import _print_event_status, _print_profile_status



def get_event_data(
    form: str,
    start_date: str,
    end_date: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    filter: Optional[EventFilter] = None,
    option: Optional[EventOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Retrieve event data from an AMS Event Form within a date range.

    Fetches event data for the specified form and date range, optionally applying filters for users
    or data fields. Supports downloading attachments associated with events if enabled in options.

    Args:
        form (str): The name of the AMS Event Form to retrieve data from.
        start_date (str): The start date for the event range (format: DD/MM/YYYY).
        end_date (str): The end date for the event range (format: DD/MM/YYYY).
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        filter (Optional[EventFilter]): An EventFilter object to narrow results (e.g., by user, data field).
        option (Optional[EventOption]): An EventOption object for customization (e.g., download attachments, cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing event data with columns such as 'event_id', 'user_id', 'start_date', etc.

    Raises:
        AMSError: If the API request fails, no events are found, or input validation fails.
    """
    option = option or EventOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    if not form:
        _raise_ams_error("Form name is required", function="get_event_data")
    
    start, end = _validate_dates(start_date, end_date)
    start_date_api = start.strftime("%d/%m/%Y")
    end_date_api = end.strftime("%d/%m/%Y")
    
    if filter:
        _validate_event_filter(filter.user_key, filter.user_value, filter.data_key, filter.data_value, filter.data_condition, filter.events_per_user)
    
    user_ids, user_df = _fetch_user_ids(client, filter, option.cache)
    if not user_ids:
        _raise_ams_error("No users found to fetch events for", function="get_event_data")
    
    endpoint, payload = _build_event_payload(form, start_date_api, end_date_api, user_ids, filter, filter.events_per_user if filter else None)
    
    if option.interactive_mode:
        print(f"ℹ Requesting event data for '{form}' between {start_date} and {end_date}")
    
    data = client._fetch(endpoint, method="POST", payload=payload, cache=option.cache, api_version="v1")
    if not data or "events" not in data or not data["events"]:
        _raise_ams_error(f"No events found for form '{form}' in the date range {start_date} to {end_date}", 
                           function="get_event_data", endpoint=endpoint)
    
    if option.interactive_mode:
        print(f"ℹ Processing {len(data['events'])} events...")
    rows = _process_events_to_rows(
        data["events"], 
        start, 
        end, 
        filter, 
        download_attachment=option.download_attachment, 
        client=client, 
        option=option
    )
    if not rows:
        _raise_ams_error(f"No event data found for form '{form}' between {start_date} and {end_date}", 
                           function="get_event_data", endpoint=endpoint)
    event_df = pd.DataFrame(rows)
    
    event_df = _transform_event_data(event_df, option.clean_names, option.guess_col_type, option.convert_dates)
    event_df = _append_user_data(event_df, user_df, option.include_missing_users)
    event_df = _reorder_columns(['about', 'user_id'], event_df, ['end_date', 'start_time', 'end_time', 'entered_by_user_id'])
    event_df = _sort_event_data(event_df)
    
    _print_event_status(event_df, form, option)
    
    return event_df
    
    

def sync_event_data(
    form: str,
    last_synchronisation_time: int,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    filter: Optional[SyncEventFilter] = None,
    option: Optional[SyncEventOption] = None,
    client: Optional[AMSClient] = None
) -> Tuple[DataFrame, int]:
    """Retrieve event data from an AMS event form that has been modified since the last synchronisation time.

    Fetches event data for the specified form using the `synchronise` API endpoint, returning only events
    that have been inserted or updated since the provided `last_synchronisation_time`. The function also
    returns the new synchronisation time for use in subsequent calls. If no new events are found, returns
    an empty DataFrame.

    Args:
        form (str): The name of the AMS form to retrieve data from.
        last_synchronisation_time (int): The last synchronisation time in milliseconds since 1970-01-01.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        filter (Optional[SyncEventFilter]): A SyncEventFilter object to narrow results (e.g., by user).
        option (Optional[SyncEventOption]): A SyncEventOption object for customization (e.g., cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        Tuple[DataFrame, int]: A tuple containing:
            - A DataFrame with event data (columns: 'event_id', 'user_id', 'start_date', etc.). Returns an empty DataFrame if no new events are found.
            - The new `lastSynchronisationTimeOnServer` value (in milliseconds) for use in subsequent calls.

    Raises:
        AMSError: If the API request fails or no users are found.
        ValueError: If last_synchronisation_time is invalid.
    """
    option = option or SyncEventOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    if not form:
        _raise_ams_error("Form name is required", function="sync_event")
    
    if not isinstance(last_synchronisation_time, int) or last_synchronisation_time < 0:
        raise ValueError("last_synchronisation_time must be a non-negative integer (milliseconds since 1970-01-01).")
    
    user_ids, user_df = _fetch_user_ids(client, filter, option.cache)
    if not user_ids:
        _raise_ams_error("No users found to fetch events for", function="sync_event")
    
    payload = _build_sync_event_payload(form, last_synchronisation_time, user_ids)
    
    if option.interactive_mode:
        print(f"ℹ Requesting event data for form '{form}' since last synchronisation time {datetime.fromtimestamp((last_synchronisation_time / 1000)).strftime('%Y-%m-%d %H:%M:%S')}")
    
    data = client._fetch("synchronise", method="POST", payload=payload, cache=option.cache, api_version="v1")
    
    export_data = data.get("export", {}) if data else {}
    events = export_data.get("events", [])
    new_sync_time = data.get("lastSynchronisationTimeOnServer", last_synchronisation_time) if data else last_synchronisation_time
    deleted_event_ids = data.get("idsOfDeletedEvents", []) if data else []
    
    rows = _process_events_to_rows(
        events,
        start=datetime(1970, 1, 1),  
        end=datetime.now(),          
        filter=None,                 
        download_attachment=False,  
        client=client,
        option=option
    )
    
    event_df = pd.DataFrame(rows) if rows else pd.DataFrame()
    
    if not event_df.empty:
        event_df = _transform_event_data(event_df, clean_names=False, guess_col_type=option.guess_col_type, convert_dates=False)
        
        if option.include_user_data:
            event_df = _append_user_data(event_df, user_df, option.include_missing_users)
        
        if option.include_uuid and user_df is not None:
            user_df = user_df.rename(columns={"userId": "user_id", "uuid": "uuid"})
            event_df = event_df.merge(user_df[["user_id", "uuid"]], on="user_id", how="left")
        
        front_cols = ['about', 'user_id'] if option.include_user_data else ['user_id']
        if option.include_uuid:
            front_cols.append('uuid')
        event_df = _reorder_columns(front_cols, event_df, ['end_date', 'start_time', 'end_time', 'entered_by_user_id'])
        
        event_df = _sort_event_data(event_df)
    
    event_df.attrs['deleted_event_ids'] = deleted_event_ids
    
    if option.interactive_mode:
        if len(events) == 0:
            print(f"ℹ No new data found for form '{form}' since last synchronization time {datetime.fromtimestamp((last_synchronisation_time / 1000)).strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"✔ Retrieved {len(event_df)} event records for form '{form}' since last synchronisation time {datetime.fromtimestamp((last_synchronisation_time / 1000)).strftime('%Y-%m-%d %H:%M:%S')}")
    
    return event_df, new_sync_time



def get_profile_data(
    form: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    filter: Optional[ProfileFilter] = None,
    option: Optional[ProfileOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Retrieve profile data from an AMS Profile form.

    Fetches profile data for the specified form, optionally applying filters for users.
    Returns a DataFrame with profile details such as user ID and form-specific fields.

    Args:
        form (str): The name of the AMS profile form to retrieve data from.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        filter (Optional[ProfileFilter]): A ProfileFilter object to narrow results (e.g., by user).
        option (Optional[ProfileOption]): A ProfileOption object for customization (e.g., cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing profile data with columns such as 'profile_id', 'user_id', etc.

    Raises:
        AMSError: If the API request fails, no profiles are found, or input validation fails.
    """
    option = option or ProfileOption()
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)
    
    if not form:
        _raise_ams_error("Form name is required", function="get_profile_data")
    
    user_ids, user_df = _fetch_user_ids(client, filter, option.cache)
    if not user_ids:
        _raise_ams_error("No users found to fetch profiles for", function="get_profile_data")
    
    payload = _build_profile_payload(form, user_ids)
    if option.interactive_mode:
        print(f"ℹ Requesting profile data for form '{form}'")
    
    data = client._fetch("profilesearch", method="POST", payload=payload, cache=option.cache, api_version="v1")
    if not data or "profiles" not in data or not data["profiles"]:
        _raise_ams_error(f"No profiles found for form '{form}'", function="get_profile_data", endpoint="profilesearch")
    
    if option.interactive_mode:
        print(f"ℹ Processing {len(data['profiles'])} profiles...")
    rows = _process_profile_rows(data["profiles"], filter, option)
    if not rows:
        _raise_ams_error(f"No profile data found for form '{form}'", function="get_profile_data", endpoint="profilesearch")
    profile_df = pd.DataFrame(rows)
    
    profile_df = _transform_profile_data(profile_df, option.clean_names, option.guess_col_type)
    profile_df = _append_user_data(profile_df, user_df, option.include_missing_users)
    profile_df = _reorder_columns(['about', 'user_id'], profile_df, ['entered_by_user_id'])
    profile_df = _sort_profile_data(profile_df)
    
    _print_profile_status(profile_df, form, option)
    
    return profile_df