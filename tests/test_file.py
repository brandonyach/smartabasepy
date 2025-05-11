from smartabasepy import upload_files, attach_files_to_events, attach_files_to_avatars, FileUploadOption, UserOption
from smartabasepy import UpdateEventOption
from pandas import DataFrame
import pandas as pd
import numpy as np

# Step 1: Upload files


upload_results = upload_files(
    url = "https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!",
    file_path="/Users/byach/Documents/attachments",
    option = FileUploadOption(
        cache = True,
        interactive_mode=True,
        save_to_file="upload_results.csv"
    )
)


# Insert Test Events


from smartabasepy import insert_event_data, update_event_data, InsertEventOption, UpdateEventOption

data = {
    "Field 1": ["16.31", "19.45", "20.1"],
    "Field 2": ["1", "2", "3"],
    "Field 3": ["This is a test", "this is a test 2", "this is a test 3"],
    "Attachment ID": ["att_001", "att_002", "att_003"],
    "username": ["Riley.Jones", "Samantha.Fields", "Mary.Phillips"],
    "about": ["Riley Jones", "Samantha Fields", "Mary Phillips"],
    "start_date": ["01/04/2025", "01/04/2025", "01/04/2025"]
}
df = pd.DataFrame(data)

insert_event_data(
    df = df,
    form = "Test File Upload",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=InsertEventOption(
        id_col = "about",
        interactive_mode=True,
        cache=False,
        table_fields = None
    )
)


# Create a file_df mappings file to provide

file_data = {
    "username": ["Riley.Jones", "Samantha.Fields", "Mary.Phillips"],
    "file_name":["lab_values.pdf", "lattice_grow.pdf", "dummy.pdf"],
    "attachment_id": ["att_001", "att_002", "att_003"]
    # "file_name": [uf["file_name"] for uf in upload_results],
    # "file_id": [uf["file_id"] for uf in upload_results],
    # "file_url": [uf["file_url"] for uf in upload_results],
    # "server_file_name": [uf["server_file_name"] for uf in upload_results]
}

file_df = DataFrame(file_data)


# Attach files to events

attach_files_to_events(
    file_df=file_df,
    upload_results=upload_results,
    user_key="username",
    form="Test File Upload",
    file_field_name="Upload Attachment",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UpdateEventOption(interactive_mode=True)
)


# Testing get all users
from smartabasepy.user_ import get_all_user_data
from smartabasepy import get_user

get_all_user_data(
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!"
    # user_ids=[72827, 72828, 72829]
)


# Uploading Avatars ----

upload_results = upload_files(
    file_path="/Users/byach/Documents/avatars",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    processor_key="avatar-key",
    option=FileUploadOption(
        interactive_mode=True,
        save_to_file="avatar_upload_results.csv"
    )
)


upload_results = pd.read_csv("avatar_upload_results.csv")

# Attaching files to avatars

mapping_data = {
    "about": ["Riley Jones", "Samantha Fields", "Mary Phillips", "Dean Jones"],
    "file_name": ["Riley Jones.png", "Samantha Fields.png", "Mary Phillips.png", "Hockey 15.jpg"]
}
mapping_df = DataFrame(mapping_data)


attach_files_to_avatars(
    mapping_df=mapping_df,
    upload_results=upload_results,
    user_key="about",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UserOption(interactive_mode=True)
)



