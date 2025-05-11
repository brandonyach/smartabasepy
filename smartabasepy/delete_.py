from typing import Optional, Dict, List
from .utils import AMSClient, AMSError, get_client
from .delete_option import DeleteEventOption


def delete_event_data(
    event_id: int,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[DeleteEventOption] = None,
    client: Optional[AMSClient] = None
) -> str:
    """Delete a single event from an AMS instance.

    Deletes an event specified by its event ID using the AMS API's deleteevent endpoint.

    Args:
        event_id (int): The ID of the event to delete.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[DeleteEventOption]): A DeleteEventOption object for customization (e.g., interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        str: A message indicating the result of the deletion (e.g., "SUCCESS: Deleted 134273").

    Raises:
        AMSError: If the API request fails or input validation fails.
        ValueError: If event_id is not a single positive integer.
    """
    option = option or DeleteEventOption()
    client = client or get_client(url, username, password, cache=True, interactive_mode=option.interactive_mode)
    
    if not isinstance(event_id, int) or event_id <= 0:
        raise ValueError("event_id must be a single positive integer.")
    
    payload = _build_delete_event_payload(event_id)
    
    if option.interactive_mode:
        confirm = input(f"Are you sure you want to delete event '{event_id}'? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes']:
            raise AMSError("Delete operation cancelled by user.")
        print(f"ℹ Deleting event with ID {event_id}...")
    
    try:
        response = client._fetch("deleteevent", method="POST", payload=payload, cache=False, api_version="v1")
    except AMSError as e:
        if option.interactive_mode:
            print(f"✖ Failed to delete event with ID {event_id}: {str(e)}")
        raise
    
    if not isinstance(response, dict) or "state" not in response or "message" not in response:
        raise AMSError(f"Invalid response from deleteevent endpoint: {response}")
    
    state = response["state"]
    message = response["message"]
    full_message = f"{state}: {message}"
    
    if option.interactive_mode:
        if state == "SUCCESS":
            print(f"✔ {full_message}")
        else:
            print(f"✖ {full_message}")
    
    return full_message



def _build_delete_event_payload(event_id: int) -> Dict:
    """Build the payload for the deleteevent API endpoint.

    Constructs a payload for deleting a single event by its ID.

    Args:
        event_id (int): The ID of the event to delete.

    Returns:
        Dict: The payload dictionary for the API request.
    """
    return {"eventId": event_id}



def delete_multiple_events(
    event_ids: List[int],
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[DeleteEventOption] = None,
    client: Optional[AMSClient] = None
) -> str:
    """Delete multiple events from an AMS instance.

    Deletes a list of events specified by their event IDs using the AMS API's event/deleteAll endpoint.

    Args:
        event_ids (List[int]): The list of event IDs to delete.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[DeleteEventOption]): A DeleteEventOption object for customization (e.g., interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        str: A message indicating the result of the deletion (e.g., "SUCCESS: Deleted 1 events").

    Raises:
        AMSError: If the API request fails.
        ValueError: If event_ids is not a non-empty list of positive integers.
    """
    option = option or DeleteEventOption()
    client = client or get_client(url, username, password, cache=True, interactive_mode=option.interactive_mode)
    
    if not isinstance(event_ids, list) or not event_ids:
        raise ValueError("event_ids must be a non-empty list of integers.")
    
    for event_id in event_ids:
        if not isinstance(event_id, int) or event_id <= 0:
            raise ValueError("All event_ids must be positive integers.")
    
    payload = _build_delete_multiple_events_payload(event_ids)
    
    if option.interactive_mode:
        confirm = input(f"Are you sure you want to delete {len(event_ids)} events with IDs {event_ids}? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes']:
            raise AMSError("Delete operation cancelled by user.")
        print(f"ℹ Deleting {len(event_ids)} events with IDs {event_ids}...")
        
    try:
        response = client._fetch("event/deleteAll", method="POST", payload=payload, cache=False, api_version="v2")
    except AMSError as e:
        if option.interactive_mode:
            print(f"✖ Failed to delete events with IDs {event_ids}: {str(e)}")
        raise
    
    if response is None:
        full_message = f"SUCCESS: Deleted {len(event_ids)} events"
    else:
        full_message = f"FAILURE: Could not delete events with IDs {event_ids}"
        if option.interactive_mode:
            print(f"✖ {full_message}")
        raise AMSError(full_message)
    
    if option.interactive_mode:
        print(f"✔ {full_message}")
    
    return full_message



def _build_delete_multiple_events_payload(event_ids: List[int]) -> Dict:
    """Build the payload for the event/deleteAll API endpoint.

    Constructs a payload for deleting multiple events by their IDs.

    Args:
        event_ids (List[int]): The list of event IDs to delete.

    Returns:
        Dict: The payload dictionary for the API request.
    """
    return {"eventIds": [str(event_id) for event_id in event_ids]}