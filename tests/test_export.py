from smartabasepy import get_user, get_group, get_event_data, get_profile_data, UserFilter, UserOption, GroupOption, EventOption, EventFilter, ProfileFilter, ProfileOption, get_forms, FormOption, sync_event_data, SyncEventFilter, SyncEventOption

import pandas as pd









get_event_data(
    form="ACFT",
    start_date= "01/01/2021",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    filter=EventFilter(
        user_key="username",
        user_value="riley.jones",
        data_key="Score (MDL)",
        data_value="85",
        data_condition=">=",
        events_per_user=10
    ),
    username="python.connector",
    password="Connector123!",
)


acft = get_event_data(
    form="ACFT",
    start_date= "01/01/2021",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    filter=EventFilter(
        user_key="group",
        user_value="Test Group"
    ),
    username="python.connector",
    password="Connector123!",
)

acft = get_event_data(
    form="ACFT",
    start_date= "01/01/2021",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    filter=EventFilter(
        user_key="about",
        user_value="Riley Jones"
    ),
    username="python.connector",
    password="Connector123!",
)

acft.columns


# Include Missing Users
acft_df = get_event_data(
    form="ACFT",
    start_date= "01/01/2021",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    filter = EventFilter(
        user_key="group",
        user_value="Test Group"
    ),
    option=EventOption(
        interactive_mode=True,
        cache=True,
        include_missing_users=True,
        guess_col_type=True
    )
)

acft_df.dtypes


# Table Data
get_event_data(
    form="Forcedecks Trials",
    start_date= "01/01/2022",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    filter = EventFilter(
        user_key="group",
        user_value="Test Group"
    ),
    option=EventOption(
        interactive_mode=True,
        cache=True,
        include_missing_users=False,
        guess_col_type=True,
    )
)



# Attachments
get_event_data(
    form="ACFT",
    start_date= "01/01/2021",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    filter=EventFilter(
        user_key="group",
        user_value="Test Group"
    ),
    username="python.connector",
    password="Connector123!",
    option=EventOption(
        interactive_mode=True,
        cache=True,
        download_attachment=True,
        guess_col_type=True
    )
)


# Attachments in a specific directory
get_event_data(
    form="ACFT",
    start_date= "01/01/2021",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    filter=EventFilter(
        user_key="group",
        user_value="Test Group"
    ),
    username="python.connector",
    password="Connector123!",
    option=EventOption(
        interactive_mode=True,
        cache=True,
        download_attachment=True,
        attachment_directory="/Users/byach/Documents/attachments",
        guess_col_type=True
    )
)


# No Attachments
get_event_data(
    form="ACFT",
    start_date= "01/01/2021",
    end_date= "01/01/2026",
    url="https://learn.smartabase.com/ankle",
    filter=EventFilter(
        user_key="username",
        user_value="jackson.king",
        data_key="Score (MDL)",
        data_value="85",
        data_condition=">=",
        events_per_user=10
    ),
    username="python.connector",
    password="Connector123!",
    option=EventOption(
        interactive_mode=True,
        cache=True,
        download_attachment=True,
        attachment_directory="/Users/byach/Documents/attachments",
        guess_col_type=True
    )
)




# Profile Data 
get_profile_data(
    form="ForceDecks",
    url="https://learn.smartabase.com/ankle",
    filter=ProfileFilter(
        user_key="group", 
        user_value="Test Group"
    ),
    option = ProfileOption(
        interactive_mode=True,
        cache=True,
        include_missing_users=False,
        clean_names = True,
        guess_col_type = True
    ),
    username="python.connector",
    password="Connector123!"
)




# Syncronize Events ----


# Fetch modified events
event_df, new_sync_time = sync_event_data(
    form="ACFT",
    last_synchronisation_time=0,
    url="https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!",
    filter = SyncEventFilter(
        user_key="group", 
        user_value="Test Group"
    ), 
    option = SyncEventOption(
        interactive_mode=True, 
        include_user_data=False, 
        include_uuid=True,
        cache=False
    )
)

event_df.attrs['deleted_event_ids']


# new_sync_time = 1745160059650

# Use the new_sync_time for the next call
event_df2, new_sync_time2 = sync_event_data(
    form="ACFT",
    last_synchronisation_time=new_sync_time,
    url="https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!",
    filter = SyncEventFilter(
        user_key="group", 
        user_value="Test Group"
    ), 
    option = SyncEventOption(
        interactive_mode=True, 
        include_user_data=True, 
        include_uuid=True
    )
)


event_df2.columns

event_df2.attrs['deleted_event_ids']


