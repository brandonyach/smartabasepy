import pandas as pd
import numpy as np
from smartabasepy import insert_event_data, update_event_data, upsert_event_data, InsertEventOption, UpdateEventOption, UpsertEventOption, get_user, get_group, get_event_data, get_profile_data, UserFilter, UserOption, GroupOption, EventOption, EventFilter, ProfileFilter, ProfileOption, get_forms, FormOption, AMSClient, upsert_profile_data, UpsertProfileOption


# Insert Event ----

data = {
    "Field 1": ["16.31", "19.45", "20.1"],
    "Field 2": ["Test 1", "Test 2", "Test 3"],
    "Field 3": ["This is a test", "this is a test 2", "this is a test 3"],
    "username": ["Riley.Jones", "Samantha.Fields", "Mary.Phillips"],
    "about": ["Riley Jones", "Samantha Fields", "Mary Phillips"],
    "start_date": ["01/04/2025", "01/04/2025", "01/04/2025"]
}
df = pd.DataFrame(data)


data_2 = {
    "Field 1": ["16.31", "19.45"],
    "Field 2": ["21", "23"],
    "Field 3": ["This is a test", "this is a test 2"],
    "user_id": [72827, 72828],
    "username": ["Riley.Jones", "Samantha.Fields"],
    "start_date": ["14/04/2025", "14/04/2025"]
}
df_2 = pd.DataFrame(data_2)


data_3 = {
    "Field 1": ["1.1", "19.45", "20.0"],
    "Field 2": ["1.1", "23", "24"],
    "Field 3": ["Test 1 - Update", "Test 2", "Test 3"],
    "about": ["Riley Jones", "Samantha Fields", "Mary Phillips"]
}
df_3 = pd.DataFrame(data_3)


insert_event_data(
    df = df,
    form = "Test Import Form",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=InsertEventOption(
        id_col = "about",
        interactive_mode=True,
        cache=True,
        table_fields = None
    )
)


# Update Event ----

test_df = get_event_data(
    form="Test Import Form",
    start_date= "01/01/2021",
    end_date= "01/02/2026",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    filter = EventFilter(
        user_key="group",
        user_value="Test Group"
    )
)


test_df.loc[0, "Field 1"] = 1.1
test_df.loc[0, "Field 2"] = 1.1
test_df.loc[0, "Field 3"] = "Test - Update"

test_df.loc[2, "Field 1"] = 1.0
test_df.loc[2, "Field 2"] = 1.1
test_df.loc[2, "Field 3"] = "Test - Update"

test_df.loc[0, "Field 2"] = 6.1
test_df.loc[1, "Field 2"] = 5.1
test_df.loc[2, "Field 2"] = 4.1

update_event_data(
    df = test_df,
    form = "Test Import Form",
    url="https://learn.smartabase.com/ankle",
    option=UpdateEventOption(
        id_col = "user_id",
        interactive_mode=True,
        table_fields = None
    )
)


# Upsert Event ----

test_data_2 = {
    "Field 1": ["106.31", "109.45", "108"],
    "Field 2": ["201", "230", "220"],
    "Field 3": ["This is a new test", "this is a new test 2", "this is a new test 3"],
    "user_id": [72827, 72828, 72829],
    "about": ["Riley Jones", "Samantha Fields", "Mary Phillips"],
    "start_date": ["14/04/2025", "14/04/2025", "14/02/2025"],
    "form": ["Test Import Form", "Test Import Form", "Test Import Form"]
}
test_df_2 = pd.DataFrame(test_data_2)



test_df_upsert = pd.concat([test_df, test_df_2])



upsert_event_data(
    df = test_df_upsert,
    form = "Test Import Form",
    url="https://learn.smartabase.com/ankle",
    option=UpsertEventOption(
        id_col="about",
        interactive_mode=True,
        table_fields = None
    )
)


# Table Data ----


table_data = {
    "Field 1": ["15.1", np.nan, np.nan, "16.45", np.nan, np.nan, "17.0", np.nan, np.nan],
    "Field 2": ["1.1", np.nan, np.nan, "2.2",np.nan, np.nan, "3.34", np.nan, np.nan],
    "Field 3": ["This is a test", np.nan, np.nan, "this is a test 2", np.nan, np.nan, "this is a test 3", np.nan, np.nan],
    "Row": ["1", "2", "3", "1", "2", "3", "1", "2", "3"],
    "Field 4": ["15.1", "16.45", "17.0", "15.1", "16.45", "17.0", "15.1", "16.45", "17.0"],
    "Field 5": ["Test 1", "Test 2", "Test 3", "Test 1", "Test 2", "Test 3", "Test 1", "Test 2", "Test 3"],
    "Field 6": ["This is a test 1", "this is a test 2", "this is a test 3", "This is a test 1", "this is a test 2", "this is a test 3", "This is a test", "this is a test 2", "this is a test 3"],
    
    
    "username": ["Riley.Jones", "Riley.Jones", "Riley.Jones", "Samantha.Fields",  "Samantha.Fields", "Samantha.Fields","Mary.Phillips", "Mary.Phillips", "Mary.Phillips"],
    "about": ["Riley Jones", "Riley Jones", "Riley Jones", "Samantha Fields","Samantha Fields", "Samantha Fields", "Mary Phillips", "Mary Phillips", "Mary Phillips"],
    "start_date": ["01/04/2025", "01/04/2025", "01/04/2025", "01/04/2025", "01/04/2025", "01/04/2025", "01/04/2025", "01/04/2025", "01/04/2025"]
}

table_df = pd.DataFrame(table_data)


insert_event_data(
    df = table_df,
    form = "Test Table",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=InsertEventOption(
        id_col = "username",
        interactive_mode=True,
        cache=True,
        table_fields = ["Row", "Field 4", "Field 5", "Field 6"]
    )
)


# Updating Table Data ----

table_update_data = get_event_data(
    form="Test Table",
    start_date= "01/01/2021",
    end_date= "01/02/2026",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    filter = EventFilter(
        user_key="group",
        user_value="Test Group"
    )
)



table_update_data.loc[0, "Field 1"] = 1
table_update_data.loc[0, "Field 2"] = 2.2
table_update_data.loc[0, "Field 3"] = "Test - Updates"

table_update_data.loc[0, "Field 4"] = 11
table_update_data.loc[0, "Field 5"] = "Test - Update 1"
table_update_data.loc[0, "Field 6"] = "Test - Update 1"

table_update_data.loc[1, "Field 4"] = 12
table_update_data.loc[1, "Field 5"] = "Test - Update 2"
table_update_data.loc[1, "Field 6"] = "Test - Update 2"

table_update_data.loc[2, "Field 4"] = 13
table_update_data.loc[2, "Field 5"] = "Test - Update 3"
table_update_data.loc[2, "Field 6"] = "Test - Update 3"



update_event_data(
    df = table_update_data,
    form = "Test Table",
    url="https://learn.smartabase.com/ankle",
    option=UpdateEventOption(
        id_col = "about",
        interactive_mode=True,
        cache = True,
        table_fields = ["Row", "Field 4", "Field 5", "Field 6"]
    )
)



# Upsert Table Event Data ----


table_update_data.loc[0, "Field 1"] = 1
table_update_data.loc[0, "Field 2"] = 2.2
table_update_data.loc[0, "Field 3"] = "Test - Update"

table_update_data.loc[0, "Field 4"] = 11
table_update_data.loc[0, "Field 5"] = "Test - Update 4"
table_update_data.loc[0, "Field 6"] = "Test - Update 4"

table_update_data.loc[1, "Field 4"] = 12
table_update_data.loc[1, "Field 5"] = "Test - Update 5"
table_update_data.loc[1, "Field 6"] = "Test - Update 5"

table_update_data.loc[2, "Field 4"] = 113
table_update_data.loc[2, "Field 5"] = "Test - Update 6"
table_update_data.loc[2, "Field 6"] = "Test - Update 6"

table_update_data.loc[3, "Field 1"] = 122
table_update_data.loc[3, "Field 2"] = 2.2
table_update_data.loc[3, "Field 3"] = "Test - Update"

table_update_data.loc[3, "Field 4"] = 131
table_update_data.loc[3, "Field 5"] = "Test - Update new"
table_update_data.loc[3, "Field 6"] = "Test - Update new"



new_table_data = {
    "Field 1": ["20", np.nan, np.nan, "23", np.nan, np.nan, "25", np.nan, np.nan],
    "Field 2": ["100.1", np.nan, np.nan, "200.2",np.nan, np.nan, "300.34", np.nan, np.nan],
    "Field 3": ["This is a test new", np.nan, np.nan, "this is a test new 2", np.nan, np.nan, "this is a test new 3", np.nan, np.nan],
    "Row": ["1", "2", "3", "1", "2", "3", "1", "2", "3"],
    "Field 4": ["150.1", "160.45", "170.0", "150.1", "160.45", "170.0", "150.1", "160.45", "170.0"],
    "Field 5": ["Test 1 new", "Test 2 new", "Test 3 new", "Test 1 new", "Test 2 new", "Test 3 new", "Test 1 new", "Test 2 new", "Test 3 new"],
    "Field 6": ["This is a test 1 new", "this is a test 2 new", "this is a test 3 new", "This is a test 1 new", "this is a test 2 new", "this is a test 3 new", "This is a test new", "this is a test 2 new", "this is a test 3 new"],
    
    
    "username": ["Riley.Jones", "Riley.Jones", "Riley.Jones", "Samantha.Fields",  "Samantha.Fields", "Samantha.Fields","Mary.Phillips", "Mary.Phillips", "Mary.Phillips"],
    "about": ["Riley Jones", "Riley Jones", "Riley Jones", "Samantha Fields","Samantha Fields", "Samantha Fields", "Mary Phillips", "Mary Phillips", "Mary Phillips"],
    "start_date": ["11/04/2025", "11/04/2025", "11/04/2025", "11/04/2025", "11/04/2025", "11/04/2025", "11/04/2025", "11/04/2025", "11/04/2025"]
}

new_table_df = pd.DataFrame(new_table_data)

table_df_upsert = pd.concat([table_update_data, new_table_df])



upsert_event_data(
    df = table_df_upsert,
    form = "Test Table",
    url="https://learn.smartabase.com/ankle",
    option=UpsertEventOption(
        id_col="about",
        interactive_mode=True,
        table_fields = ["Row", "Field 4", "Field 5", "Field 6"]
    )
)


# Profile Data ----


profile_data = {
    "Field 1": ["8", "9", "10"],
    "Field 2": ["Line of text 1", "Line of text 2", "Line of text 3"],
    "username": ["Riley.Jones", "Samantha.Fields", "Mary.Phillips"],
    "about": ["Riley Jones", "Samantha Fields", "Mary Phillips"]
}
profile_df = pd.DataFrame(profile_data)

new_profile_data = {
    "Field 1": ["7"],
    "Field 2": ["Line of text 4"],
    "username": ["Liam.Walker"],
    "about": ["Liam Walker"]
}
new_profile_df = pd.DataFrame(new_profile_data)

profile_df_upsert = pd.concat([profile_df, new_profile_df])



upsert_profile_data(
    df=profile_df,
    form="Test Profile Form",
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=UpsertProfileOption(
        id_col="about",
        interactive_mode=True,
        cache=True
    )
)