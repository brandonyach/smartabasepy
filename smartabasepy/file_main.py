import os
import pandas as pd
from typing import Optional, Dict, List
from requests_toolbelt.multipart.encoder import MultipartEncoder
import mimetypes
from pandas import DataFrame
from datetime import datetime
from .utils import AMSClient, AMSError, get_client
from .import_main import update_event_data
from .import_option import UpdateEventOption
from .file_option import FileUploadOption
from .file_validate import _validate_file_df, _validate_file_path
from .file_process import _merge_upload_results, _format_file_reference, _map_user_ids_to_file_df
from .user_process import _match_user_ids, _map_user_updates
from .user_fetch import _fetch_all_user_data, _update_single_user
from .user_validate import _validate_user_key
from .user_option import UserOption


def upload_files(
    file_path: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    processor_key: str = "avatar-key",
    option: Optional[FileUploadOption] = None,
    client: Optional[AMSClient] = None
) -> List[Dict[str, Optional[str]]]:
    """Upload one or more files to an AMS instance and retrieve their file IDs.

    Uploads a single file or all files in a specified directory to the AMS API's `/x/fileupload`
    endpoint and retrieves the file IDs using the `/api/v2/fileupload/getUploadStatus` endpoint.
    Requires valid authentication credentials. Supports interactive feedback, caching, and saving
    results to a CSV file. Stores results in the client’s `last_uploaded_files` attribute for
    subsequent operations.

    Args:
        file_path (str): The path to a single file or a directory containing files to upload.
            Must exist and be accessible.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses the
            AMS_USERNAME environment variable. Defaults to None.
        password (Optional[str]): The password for authentication. If None, uses the
            AMS_PASSWORD environment variable. Defaults to None.
        processor_key (str): The processor key for the file upload, determining how the AMS
            server handles the file (e.g., 'document-key' for general document or 'avatar-key' for profile avatars). Defaults to
            'avatar-key'.
        option (Optional[FileUploadOption]): Configuration options for the upload, including
            interactive_mode (for status messages), cache (for API response caching), and
            save_to_file (to save results to a CSV file). If None, uses default
            FileUploadOption. Defaults to None.
        client (Optional[AMSClient]): A pre-authenticated AMSClient instance. If None,
            a new client is created using the provided url, username, and password.
            Defaults to None.

    Returns:
        List[Dict[str, Optional[str]]]: A list of dictionaries, each containing:
            - 'file_name': The name of the uploaded file.
            - 'file_id': The server-assigned file ID (None if upload failed).
            - 'server_file_name': The server-assigned file name (None if upload failed).

    Raises:
        AMSError: If the file or directory does not exist, is not accessible, authentication
            fails, the upload fails, or the API response is invalid (e.g., missing file ID).

    Side Effects:
        - Stores results in `client.last_uploaded_files` if a client is provided.
        - Saves results to a CSV file if `option.save_to_file` is specified.

    Examples:
        >>> from smartabasepy import upload_files
        >>> from smartabasepy import FileUploadOption
        >>> results = upload_files(
        ...     file_path = "/path/to/files/document.pdf",
        ...     url = "https://example.smartabase.com/site",
        ...     username = "user",
        ...     password = "pass",
        ...     processor_key = "document-key",
        ...     option = FileUploadOption(interactive_mode=True, save_to_file = "upload_results.csv")
        ... )
        ℹ Uploading 1 files: 'document.pdf'
        ✔ Successfully Uploaded 1 files with file ID's 987654
        Upload results:
           file_name   file_id server_file_name
        0 document.pdf 987654   document_2025.pdf
        ℹ Saved upload results to 'upload_results.csv'
    """
    
    option = option or FileUploadOption()
    
    client = client or get_client(url, username, password, interactive_mode=option.interactive_mode)

    files_to_upload = _validate_file_path(file_path, function="upload_files")

    if not client.authenticated:
        client.login()

    skype_name = client.login_data.get("user", {}).get("skypeName")
    if not skype_name:
        AMSError("skypeName not found in login response. Cannot set session-token for file upload", function="upload_files")

    if option.interactive_mode:
        file_names = [file_name for _, file_name in files_to_upload]
        file_list_str = ", ".join(f"'{name}'" for name in file_names[:3])
        if len(file_names) > 3:
            file_list_str += ", ..."
        print(f"ℹ Uploading {len(files_to_upload)} files: {file_list_str}")

    results = []
    for file_path, file_name in files_to_upload:
        try:
            # Use the base URL without the site name 
            base_url = client.url.rsplit('/', 1)[0]
            file_upload_url = f"{base_url}/x/fileupload"
            
            mime_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            
            with open(file_path, 'rb') as f:
                multipart_data = MultipartEncoder(
                    fields={
                        'session-token': skype_name,
                        'filename': file_name,
                        'processorkey': processor_key,
                        'filedata': (file_name, f, mime_type)
                    }
                )
                
                headers = client.headers.copy()
                headers['Content-Type'] = multipart_data.content_type
                
                response = client.session.post(file_upload_url, data=multipart_data, headers=headers)
            
            if response.status_code != 200:
                AMSError(
                    f"Failed to upload file '{file_name}'",
                    function="upload_files",
                    status_code=response.status_code,
                    endpoint=file_upload_url
                )
            
            # Retrieve the upload status and file ID
            status_url = client._AMS_url("fileupload/getUploadStatus", api_version="v2")
            status_response = client.session.post(status_url, json={}, headers=client.headers)
            
            if status_response.status_code != 200:
                AMSError(
                    f"Failed to retrieve upload status for '{file_name}'",
                    function="upload_files",
                    status_code=status_response.status_code,
                    endpoint=status_url
                )
            
            try:
                status_data = status_response.json()
            except ValueError:
                AMSError(
                    f"Invalid response format when retrieving upload status for '{file_name}'",
                    function="upload_files"
                )
            
            if status_data.get("uploadStatus", {}).get("error", True) or not status_data.get("data"):
                error_message = status_data.get("uploadStatus", {}).get("message", "Unknown error")
                AMSError(
                    f"Upload failed for '{file_name}': {error_message}",
                    function="upload_files"
                )
            
            file_id = status_data["data"][0]["value"].get("id")
            if file_id is None:
                AMSError(
                    f"Failed to retrieve file ID for '{file_name}'",
                    function="upload_files"
                )
            
            server_file_name = status_data["data"][0]["value"].get("name")
            
            results.append({
                "file_name": file_name,
                "file_id": file_id,
                "server_file_name": server_file_name
            })

        except AMSError as e:
            if option.interactive_mode:
                print(f"⚠️ Failed to upload '{file_name}': {str(e)}")
            results.append({
                "file_name": file_name,
                "file_id": None,
                "server_file_name": None
            })

    # Store results in client.last_uploaded_files if client is provided
    if client is not None:
        client.last_uploaded_files = results

    # Save results to file if save_to_file is provided
    if option.save_to_file:
        DataFrame(results).to_csv(option.save_to_file, index=False)
        if option.interactive_mode:
            print(f"ℹ Saved upload results to '{option.save_to_file}'")

    if option.interactive_mode:
        file_ids = [str(result["file_id"]) for result in results if result["file_id"] is not None]
        file_id_str = ", ".join(file_ids[:3])
        if len(file_ids) > 3:
            file_id_str += ", ..."
        print(f"✔ Successfully Uploaded {len(file_ids)} files with file ID's {file_id_str}")
        print("Upload results:")
        print(DataFrame(results))

    return results



def attach_files_to_events(
    file_df: DataFrame,
    upload_results: List[Dict[str, Optional[str]]],
    user_key: str,
    form: str,
    file_field_name: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UpdateEventOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Attach uploaded files to existing events in an AMS Event Form.

    Links uploaded files to events by updating the specified file upload field in the event
    form with file IDs from the upload results. The function retrieves event data for the
    specified form, matches events to files using an 'Attachment ID' field, merges file IDs
    into the event DataFrame, and updates the events via the AMS API. Returns a DataFrame
    of failed attachments with reasons for failure. Supports interactive feedback and
    confirmation prompts.

    Args:
        file_df (DataFrame): A pandas DataFrame containing:
            - A column with user identifiers (named by `user_key`, e.g., 'username', 'email').
            - A 'file_name' column with names matching those in `upload_results`.
            - An 'attachment_id' column corresponding to the 'Attachment ID' field in the
              event form.
        upload_results (List[Dict]): The results from `upload_files`, containing 'file_name',
            'file_id', and 'server_file_name' for each uploaded file.
        user_key (str): The name of the user identifier column in `file_df`, one of
            'username', 'email', 'about', or 'uuid'.
        form (str): The name of the AMS Event Form containing the events. Must be a non-empty
            string and correspond to a valid event form.
        file_field_name (str): The name of the file upload field in the event form (e.g.,
            'attachment') to receive the file IDs.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses the
            AMS_USERNAME environment variable. Defaults to None.
        password (Optional[str]): The password for authentication. If None, uses the
            AMS_PASSWORD environment variable. Defaults to None.
        option (Optional[UpdateEventOption]): Configuration options for the update,
            including interactive_mode (for status messages and confirmation),
            cache (for API response caching), and id_col (for user ID mapping).
            If None, uses default UpdateEventOption. Defaults to None.
        client (Optional[AMSClient]): A pre-authenticated AMSClient instance. If None,
            a new client is created using the provided url, username, and password.
            Defaults to None.

    Returns:
        DataFrame: A pandas DataFrame containing failed attachments with columns:
            - 'file_name': The name of the file that failed to attach.
            - 'attachment_id': The attachment ID that failed to match an event.
            - 'reason': The reason for the failure (e.g., 'File not found in upload_results',
              'No matching event found'). Returns an empty DataFrame if all attachments succeed.

    Raises:
        AMSError: If `file_df` is invalid (e.g., missing required columns), `user_key` is
            invalid, no events are found, authentication fails, or the API request fails.
        ValueError: If `file_df` is empty or contains invalid data.

    Examples:
        >>> import pandas as pd
        >>> from smartabasepy import upload_files, attach_files_to_events
        >>> from smartabasepy import FileUploadOption
        >>> from smartabasepy import UpdateEventOption
        >>> file_df = pd.DataFrame({
        ...     "username": ["john.doe", "jane.smith"],
        ...     "file_name": ["doc1.pdf", "doc2.pdf"],
        ...     "attachment_id": ["ATT123", "ATT124"]
        ... })
        >>> upload_results = upload_files(
        ...     file_path = "/path/to/files",
        ...     url = "https://example.smartabase.com/site",
        ...     username = "user",
        ...     password = "pass"
        ... )
        >>> failed_df = attach_files_to_events(
        ...     file_df = file_df,
        ...     upload_results = upload_results,
        ...     user_key = "username",
        ...     form = "Training Log",
        ...     file_field_name = "attachment",
        ...     url = "https://example.smartabase.com/site",
        ...     username = "user",
        ...     password = "pass",
        ...     option = UpdateEventOption(interactive_mode = True)
        ... )
        ℹ Retrieving user IDs for 2 users using username...
        ℹ Retrieving events for form 'Training Log' between 01/01/1970 and [current_date] for 2 users...
        ✔ Successfully updated events with file references
        ✔ Successfully updated events for 2 files.
    """
    option = option or UpdateEventOption(interactive_mode=True)
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)

    # Validate file_df and user_key
    _validate_file_df(file_df, user_key, require_attachment_id=True)
    _validate_user_key(user_key)

    # Merge upload_results into file_df
    file_df = _merge_upload_results(file_df, upload_results)

    # Check for files that didn't match upload_results
    failed_df = DataFrame(columns=["file_name", "attachment_id", "reason"])
    unmatched_files = file_df[file_df["file_id"].isna()]
    if not unmatched_files.empty:
        failed_df = DataFrame({
            "file_name": unmatched_files["file_name"],
            "attachment_id": unmatched_files["attachment_id"],
            "reason": "File not found in upload_results"
        })
        file_df = file_df[file_df["file_id"].notna()]

    if file_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No files matched the upload_results")
        return failed_df

    file_df, user_failed_df = _map_user_ids_to_file_df(
        file_df, user_key, client, option.interactive_mode, option.cache
    )

    if file_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No users could be mapped to user_ids")
        return failed_df
    
    user_values = file_df[user_key].unique().tolist()

    end_date = datetime.now().strftime("%d/%m/%Y")
    start_date = "01/01/1970" 

    if option.interactive_mode:
        print(f"ℹ Retrieving events for form '{form}' between {start_date} and {end_date} for {len(user_values)} users...")
        
    from .user_filter import UserFilter
    from .export_filter import EventFilter
    from .export_option import EventOption
    from .export_main import get_event_data

    try:
        event_df = get_event_data(
            form=form,
            start_date=start_date,
            end_date=end_date,
            url=url,
            username=username,
            password=password,
            filter=EventFilter(user_key=user_key, user_value=user_values),
            option=EventOption(interactive_mode=option.interactive_mode, cache=option.cache),
            client=client
        )
    except AMSError as e:
        if option.interactive_mode:
            print(f"⚠️ Failed to retrieve events: {str(e)}")
        return pd.concat([
            failed_df,
            DataFrame({
                "file_name": file_df["file_name"],
                "attachment_id": file_df["attachment_id"],
                "reason": f"Failed to retrieve events: {str(e)}"
            })
        ], ignore_index=True)

    if event_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No events found for form '{form}' between {start_date} and {end_date}")
        return pd.concat([
            failed_df,
            DataFrame({
                "file_name": file_df["file_name"],
                "attachment_id": file_df["attachment_id"],
                "reason": f"No events found for form '{form}' between {start_date} and {end_date}"
            })
        ], ignore_index=True)

    # Merge file_df into event_df using the "Attachment ID" field
    if "Attachment ID" not in event_df.columns:
        if option.interactive_mode:
            print(f"⚠️ Event form '{form}' does not have an 'Attachment ID' field")
        return pd.concat([
            failed_df,
            DataFrame({
                "file_name": file_df["file_name"],
                "attachment_id": file_df["attachment_id"],
                "reason": f"Event form '{form}' does not have an 'Attachment ID' field"
            })
        ], ignore_index=True)

    # Create the file reference in the format "file_id|server_file_name"
    file_df = _format_file_reference(file_df, file_field_name)

    event_df = event_df.drop(columns=[file_field_name], errors='ignore')  # Remove the file upload field if it exists
    event_df = event_df.merge(
        file_df[["attachment_id", file_field_name, "file_name"]],
        left_on="Attachment ID",
        right_on="attachment_id",
        how="left"
    )

    # Identify events that didn't match (i.e., file_name is NaN after merge)
    failed_mappings = event_df[event_df["file_name"].isna()]
    if not failed_mappings.empty:
        failed_mappings_df = DataFrame({
            "file_name": failed_mappings["file_name"],
            "attachment_id": failed_mappings["Attachment ID"],
            "reason": "No matching event found for attachment_id"
        })
        failed_df = pd.concat([failed_df, failed_mappings_df], ignore_index=True)
        event_df = event_df[event_df["file_name"].notna()]

    if event_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No events matched the provided attachment_ids after merging")
        return failed_df

    # Drop the file_name and attachment_id columns as they're not needed for the update
    event_df = event_df.drop(columns=["file_name", "attachment_id"])

    # Update the events with the merged DataFrame
    total_updates = len(event_df)
    try:
        update_event_data(
            df=event_df,
            form=form,
            url=url,
            username=username,
            password=password,
            option=option,
            client=client
        )
        if option.interactive_mode:
            print(f"✔ Successfully updated events with file references")
    except AMSError as e:
        if option.interactive_mode:
            print(f"⚠️ Failed to update events: {str(e)}")
        failed_df = pd.concat([
            failed_df,
            DataFrame({
                "file_name": file_df["file_name"],
                "attachment_id": file_df["attachment_id"],
                "reason": f"Failed to update event: {str(e)}"
            })
        ], ignore_index=True)

    # Print failed attachments if any
    successful_updates = total_updates - len(failed_df)
    if option.interactive_mode:
        print(f"\n✔ Successfully updated events for {successful_updates} files.")
        if not failed_df.empty:
            print(f"⚠️ Failed to update events for {len(failed_df)} files:")
            print(failed_df.to_string(index=False))

    return failed_df



def attach_files_to_avatars(
    mapping_df: DataFrame,
    upload_results: List[Dict[str, Optional[str]]],
    user_key: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[UserOption] = None,
    client: Optional[AMSClient] = None
) -> DataFrame:
    """Attach uploaded files as avatars to user profiles in an AMS instance.

    Updates user profiles by setting the `avatarId` field with file IDs from the upload results,
    using the AMS API’s `/api/v2/person/save` endpoint. The function retrieves complete user
    objects to preserve existing fields, matches users to files via a user identifier, and
    applies the updates. Returns a DataFrame of failed updates with reasons for failure.
    Supports interactive feedback and caching.

    Args:
        mapping_df (DataFrame): A pandas DataFrame containing:
            - A column with user identifiers (named by `user_key`, e.g., 'about', 'username', 
        'email', or 'uuid').
            - A 'file_name' column with names matching those in `upload_results`.
        upload_results (List[Dict]): The results from `upload_files`, containing 'file_name',
            'file_id', and 'server_file_name' for each uploaded file.
        user_key (str): The name of the user identifier column in `mapping_df`, one of
            'username', 'email', 'about', or 'uuid'.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses the
            AMS_USERNAME environment variable. Defaults to None.
        password (Optional[str]): The password for authentication. If None, uses the
            AMS_PASSWORD environment variable. Defaults to None.
        option (Optional[UserOption]): Configuration options for the update, including
            interactive_mode (for status messages), cache (for API response caching), and
            other user-related settings. If None, uses default UserOption. Defaults to None.
        client (Optional[AMSClient]): A pre-authenticated AMSClient instance. If None,
            a new client is created using the provided url, username, and password.
            Defaults to None.

    Returns:
        DataFrame: A pandas DataFrame containing failed updates with columns:
            - 'user_id': The user identifier that failed to update.
            - 'file_name': The name of the file that failed to attach.
            - 'reason': The reason for the failure (e.g., 'File not found in upload_results',
              'User not found'). Returns an empty DataFrame if all updates succeed.

    Raises:
        AMSError: If `mapping_df` is invalid (e.g., missing required columns), `user_key` is
            invalid, no users are found, authentication fails, or the API request fails.
        ValueError: If `mapping_df` is empty or contains invalid data.

    Examples:
        >>> import pandas as pd
        >>> from smartabasepy import upload_files, attach_files_to_avatars
        >>> from smartabasepy import FileUploadOption
        >>> from smartabasepy import UserOption
        >>> mapping_df = pd.DataFrame({
        ...     "username": ["john.doe", "jane.smith"],
        ...     "file_name": ["avatar1.jpg", "avatar2.jpg"]
        ... })
        >>> upload_results = upload_files(
        ...     file_path = "/path/to/avatars",
        ...     url = "https://example.smartabase.com/site",
        ...     username = "user",
        ...     password = "pass"
        ... )
        >>> failed_df = attach_files_to_avatars(
        ...     mapping_df = mapping_df,
        ...     upload_results = upload_results,
        ...     user_key = "username",
        ...     url = "https://example.smartabase.com/site",
        ...     username = "user",
        ...     password = "pass",
        ...     option = UserOption(interactive_mode = True)
        ... )
        ℹ Retrieving user data for 2 users using username...
        ℹ Updating avatars for 2 users...
        ✔ Successfully updated avatars for 2 users.
    """
    option = option or UserOption(interactive_mode=True)
    client = client or get_client(url, username, password, cache=option.cache, interactive_mode=option.interactive_mode)

    # Validate mapping_df and user_key
    _validate_file_df(mapping_df, user_key, require_attachment_id=False)
    _validate_user_key(user_key)

    # Merge upload_results into mapping_df
    mapping_df = _merge_upload_results(mapping_df, upload_results)

    # Check for files that didn't match upload_results
    failed_df = DataFrame(columns=["user_id", "file_name", "reason"])
    unmatched_files = mapping_df[mapping_df["file_id"].isna()]
    if not unmatched_files.empty:
        failed_df = DataFrame({
            "user_id": unmatched_files[user_key],
            "file_name": unmatched_files["file_name"],
            "reason": "File not found in upload_results"
        })
        mapping_df = mapping_df[mapping_df["file_id"].notna()]

    if mapping_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No files matched the upload_results - Function: attach_files_to_avatars")
        return failed_df

    # Fetch all user data
    user_values = mapping_df[user_key].unique().tolist()
    if option.interactive_mode:
        print(f"ℹ Retrieving user data for {len(user_values)} users using {user_key}...")

    try:
        user_df = _fetch_all_user_data(
            url=url,
            username=username,
            password=password,
            user_ids=None, 
            option=option,
            client=client
        )
    except AMSError as e:
        if option.interactive_mode:
            print(f"⚠️ Failed to retrieve user data: {str(e)}")
        return pd.concat([
            failed_df,
            DataFrame({
                "user_id": mapping_df[user_key],
                "file_name": mapping_df["file_name"],
                "reason": f"Failed to retrieve user data: {str(e)}"
            })
        ], ignore_index=True)

    if user_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No users found")
        return pd.concat([
            failed_df,
            DataFrame({
                "user_id": mapping_df[user_key],
                "file_name": mapping_df["file_name"],
                "reason": "No users found"
            })
        ], ignore_index=True)

    # Match users to mapping
    mapping_df, failed_matches = _match_user_ids(
        mapping_df,
        user_df,
        user_key,
        interactive_mode=option.interactive_mode
    )
    failed_df = pd.concat([failed_df, failed_matches], ignore_index=True)

    if mapping_df.empty:
        return failed_df

    # Update each user's avatar
    failed_operations = []
    total_updates = len(mapping_df)
    if option.interactive_mode:
        print(f"ℹ Updating avatars for {total_updates} users...")

    for _, row in mapping_df.iterrows():
        user_id = row["user_id"]
        file_id = row["file_id"]
        file_name = row["file_name"]
        
        # Find the user’s complete object
        user_data = user_df[user_df["id"] == user_id].to_dict("records")
        if not user_data:
            failed_operations.append({
                "user_id": user_id,
                "file_name": file_name,
                "reason": f"User ID {user_id} not found in user data"
            })
            if option.interactive_mode:
                print(f"⚠️ Failed to update avatar for user ID {user_id}: User not found")
            continue
        
        # Prepare payload
        user_data = _map_user_updates({"avatarId": file_id}, user_data[0])
        
        # Update avatar
        error_msg = _update_single_user(
            user_data,
            client,
            user_id,
            file_name,
            interactive_mode=option.interactive_mode
        )
        if error_msg:
            failed_operations.append({
                "user_id": user_id,
                "file_name": file_name,
                "reason": error_msg
            })

    # Create DataFrame for failed operations
    failed_df = pd.concat([failed_df, DataFrame(failed_operations)], ignore_index=True)
  
    successful_updates = total_updates - len(failed_operations)
    if option.interactive_mode:
        print(f"\n✔ Successfully updated avatars for {successful_updates} users.")
        if not failed_df.empty:
            print(f"⚠️ Failed to update avatars for {len(failed_df)} users:")
            print(failed_df.to_string(index=False))

    return failed_df