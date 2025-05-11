import os
from pathlib import Path
import pandas as pd
from typing import Optional, Dict, List
from requests_toolbelt.multipart.encoder import MultipartEncoder
import mimetypes
from pandas import DataFrame
from datetime import datetime
from .utils import AMSClient, AMSError, get_client, _raise_ams_error
from .import_ import update_event_data
from .import_option import UpdateEventOption
from .export_fetch import _fetch_user_ids
from .file_option import FileUploadOption
from .file_validate import _validate_file_df
from .file_process import _merge_upload_results, _format_file_reference
from .user_process import _match_users_to_mapping, _prepare_user_payload
from .user_fetch import _get_all_user_data, _update_user
from .user_validate import _validate_user_key
from .user_option import UserOption


def _download_attachment(
        client: AMSClient, 
        attachment_url: str, 
        file_name: str, 
        output_dir: Optional[str] = None
    ) -> str:
    """Download an attachment from a URL and save it to a local file.

    Args:
        client (AMSClient): The authenticated AMSClient instance to use for the download.
        attachment_url (str): The URL of the attachment to download.
        file_name (str): The name to give the downloaded file.
        output_dir (Optional[str]): The directory to save the file in. If None, uses the current working directory.

    Returns:
        str: The full path to the downloaded file.

    Raises:
        AMSError: If the directory is not writable or the download fails.
    """
    # Default to CWD if output_dir is None
    if output_dir is None:
        output_dir = os.getcwd()
    
    # Ensure output_dir is writable
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        elif not os.access(output_dir, os.W_OK):
            raise AMSError(f"Attachment directory '{output_dir}' is not writable. Please specify a writable directory via 'attachment_directory' in EventOption.")
    except OSError as e:
        raise AMSError(f"Failed to create or access attachment directory '{output_dir}': {str(e)}. Ensure the path is valid and writable.")

    full_path = os.path.join(output_dir, file_name)
    response = client.session.get(attachment_url)
    if response.status_code == 200:
        with open(full_path, "wb") as f:
            f.write(response.content)
        return full_path
    else:
        raise AMSError(f"Failed to download attachment from {attachment_url}: {response.status_code}")



def upload_files(
    file_path: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    processor_key: str = "document-key",
    option: Optional[FileUploadOption] = None,
    client: Optional[AMSClient] = None
) -> List[Dict[str, Optional[str]]]:
    """Upload one or more files to the AMS server and retrieve their file IDs.

    Uploads a file or all files in a directory to the /x/fileupload endpoint and retrieves
    the file IDs using the /api/v2/fileupload/getUploadStatus endpoint. Uses the session
    headers for authentication.

    Args:
        file_path (str): The path to a single file or a directory containing files to upload.
        url (str): The URL of the AMS instance (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        processor_key (str): The processor key to use for the upload (default: "document-key").
        option (Optional[FileUploadOption]): Configuration options for the upload operation. If None, defaults are used.
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        List[Dict[str, Optional[str]]]: A list of dictionaries, each containing the file name, its file ID,
            and the server-assigned file name (if available).

    Side Effects:
        - If client is provided, stores the results in client.last_uploaded_files.
        - If option.save_to_file is provided, saves the results to the specified file path as a CSV.

    Raises:
        AMSError: If the file or directory does not exist, or if the upload fails.
    """
    # Set default options if none provided
    option = option or FileUploadOption()
    
    # Create a client if none is provided
    client = client or get_client(url, username, password, interactive_mode=option.interactive_mode)

    # Resolve the file path
    path = Path(file_path).resolve()
    if not path.exists():
        _raise_ams_error(f"Path '{path}' does not exist", function="upload_files")

    # Determine if it's a single file or directory
    if path.is_file():
        files_to_upload = [(path, path.name)]
    else:
        files_to_upload = [(f, f.name) for f in path.iterdir() if f.is_file()]
    
    if not files_to_upload:
        _raise_ams_error(f"No files found at '{path}'", function="upload_files")

    # Ensure the client is authenticated
    if not client.authenticated:
        client.login()

    # Check if skypeName is available in login_data
    skype_name = client.login_data.get("user", {}).get("skypeName")
    if not skype_name:
        _raise_ams_error("skypeName not found in login response. Cannot set session-token for file upload", function="upload_files")

    if option.interactive_mode:
        file_names = [file_name for _, file_name in files_to_upload]
        file_list_str = ", ".join(f"'{name}'" for name in file_names[:3])
        if len(file_names) > 3:
            file_list_str += ", ..."
        print(f"ℹ Uploading {len(files_to_upload)} files: {file_list_str}")

    results = []
    for file_path, file_name in files_to_upload:
        try:
            # Use the base URL without the site name (e.g., remove '/ankle')
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
                _raise_ams_error(
                    f"Failed to upload file '{file_name}'",
                    function="upload_files",
                    status_code=response.status_code,
                    endpoint=file_upload_url
                )
            
            # Retrieve the upload status and file ID
            status_url = client._AMS_url("fileupload/getUploadStatus", api_version="v2")
            status_response = client.session.post(status_url, json={}, headers=client.headers)
            
            if status_response.status_code != 200:
                _raise_ams_error(
                    f"Failed to retrieve upload status for '{file_name}'",
                    function="upload_files",
                    status_code=status_response.status_code,
                    endpoint=status_url
                )
            
            try:
                status_data = status_response.json()
            except ValueError:
                _raise_ams_error(
                    f"Invalid response format when retrieving upload status for '{file_name}'",
                    function="upload_files"
                )
            
            if status_data.get("uploadStatus", {}).get("error", True) or not status_data.get("data"):
                error_message = status_data.get("uploadStatus", {}).get("message", "Unknown error")
                _raise_ams_error(
                    f"Upload failed for '{file_name}': {error_message}",
                    function="upload_files"
                )
            
            file_id = status_data["data"][0]["value"].get("id")
            if file_id is None:
                _raise_ams_error(
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

    Retrieves the full event data for the specified events, merges the file IDs into the event DataFrame,
    and updates the events with the new file upload field values. The event form must have a field named
    "Attachment ID" that matches the 'attachment_id' column in file_df.

    Args:
        file_df (DataFrame): A DataFrame containing:
            - A column with user identifiers (e.g., 'username', 'email', 'about', 'uuid').
            - A 'file_name' column with the names of the uploaded files (must match names in upload_results).
            - An 'attachment_id' column matching the "Attachment ID" field in the event form.
        upload_results (List[Dict]): The results from upload_files, containing 'file_name', 'file_id',
            and 'server_file_name' for each uploaded file.
        user_key (str): The name of the user identifier column in file_df ('username', 'email', 'about', or 'uuid').
        form (str): The name of the AMS Event Form containing the events.
        file_field_name (str): The name of the file upload field in the event form (e.g., 'attachment').
        url (str): The URL of the AMS instance (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[UpdateEventOption]): An UpdateEventOption object for customization.
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing failed attachments with columns ['file_name', 'attachment_id', 'reason'].

    Raises:
        AMSError: If the file_df is invalid, events cannot be retrieved, or the update operation fails.
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

    # Map user identifiers to user IDs using _fetch_user_ids
    user_values = file_df[user_key].unique().tolist()
    if option.interactive_mode:
        print(f"ℹ Retrieving user IDs for {len(user_values)} users using {user_key}...")

    # Import UserFilter inside the function to avoid circular import
    from .user_filter import UserFilter
    from .export_filter import EventFilter
    from .export_option import EventOption
    from .export_ import get_event_data

    try:
        user_ids, user_data = _fetch_user_ids(
            client=client,
            filter=UserFilter(user_key=user_key, user_value=user_values),
            cache=option.cache
        )
    except AMSError as e:
        if option.interactive_mode:
            print(f"⚠️ Failed to retrieve user data: {str(e)}")
        return pd.concat([
            failed_df,
            DataFrame({
                "file_name": file_df["file_name"],
                "attachment_id": file_df["attachment_id"],
                "reason": f"Failed to retrieve user data: {str(e)}"
            })
        ], ignore_index=True)

    if not user_ids:
        if option.interactive_mode:
            print(f"⚠️ No users found for the provided {user_key} values")
        return pd.concat([
            failed_df,
            DataFrame({
                "file_name": file_df["file_name"],
                "attachment_id": file_df["attachment_id"],
                "reason": f"No users found for the provided {user_key} values"
            })
        ], ignore_index=True)

    if user_data is None:
        user_data = DataFrame()

    # Map user_ids into file_df
    user_data = user_data.rename(columns={"userId": "user_id"})
    file_df = file_df.merge(
        user_data[["user_id", user_key]],
        on=user_key,
        how="left"
    )

    # Check for unmapped users
    unmapped = file_df[file_df["user_id"].isna()]
    if not unmapped.empty:
        failed_df = pd.concat([
            failed_df,
            DataFrame({
                "file_name": unmapped["file_name"],
                "attachment_id": unmapped["attachment_id"],
                "reason": f"User not found for {user_key} value"
            })
        ], ignore_index=True)
        file_df = file_df[file_df["user_id"].notna()]

    if file_df.empty:
        if option.interactive_mode:
            print(f"⚠️ No users could be mapped to user_ids")
        return failed_df

    # Retrieve event data
    end_date = datetime.now().strftime("%d/%m/%Y")
    start_date = "01/01/1970"  # Default to all history

    if option.interactive_mode:
        print(f"ℹ Retrieving events for form '{form}' between {start_date} and {end_date} for {len(user_values)} users...")

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

    Updates user profiles by setting the avatarId field using the /api/v2/person/save endpoint.
    Uses complete user objects from /api/v2/person/get to preserve existing fields.

    Args:
        mapping_df (DataFrame): A DataFrame containing:
            - A column with a user identifier (one of 'username', 'email', 'about', or 'uuid').
            - A 'file_name' column with the names of the uploaded files (must match names in upload_results).
        upload_results (List[Dict]): The results from upload_files, containing 'file_name', 'file_id',
            and 'server_file_name' for each uploaded file.
        user_key (str): The user identifier column in mapping_df ('username', 'email', 'about', or 'uuid').
        url (str): The URL of the AMS instance (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[UserOption]): A UserOption object for customization (e.g., cache, interactive mode).
        client (Optional[AMSClient]): A pre-initialized AMSClient instance. If None, a new client is created.

    Returns:
        DataFrame: A DataFrame containing failed updates with columns ['user_id', 'file_name', 'reason'].

    Raises:
        AMSError: If the mapping_df is invalid, users cannot be retrieved, or the update operation fails.
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
            print(f"⚠️ No files matched the upload_results")
        return failed_df

    # Fetch all user data
    user_values = mapping_df[user_key].unique().tolist()
    if option.interactive_mode:
        print(f"ℹ Retrieving user data for {len(user_values)} users using {user_key}...")

    try:
        user_df = _get_all_user_data(
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
    mapping_df, failed_matches = _match_users_to_mapping(
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
        user_data = _prepare_user_payload(user_data[0], {"avatarId": file_id})
        
        # Update avatar
        error_msg = _update_user(
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

    # Report results
    # if failed_df.empty:
    #     if option.interactive_mode:
    #         print(f"\n✔ Successfully updated avatars for {len(mapping_df)} users.")
    # else:
    #     if option.interactive_mode:
    #         print(f"\n⚠️ Failed to update avatars for {len(failed_df)} users:")
    #         print(failed_df.to_string(index=False))

    
    successful_updates = total_updates - len(failed_operations)
    if option.interactive_mode:
        print(f"\n✔ Successfully updated avatars for {successful_updates} users.")
        if not failed_df.empty:
            print(f"⚠️ Failed to update avatars for {len(failed_df)} users:")
            print(failed_df.to_string(index=False))

    return failed_df