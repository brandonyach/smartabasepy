from smartabasepy import get_database, delete_database_entry, get_forms, FormOption



# Forms
get_forms(
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=FormOption(interactive_mode=True)
)


df, len = get_database(
    url="https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!",
    form_name="Allergies",
    option = FormOption(interactive_mode=True, raw_output=False)
)

df



delete_database_entry(
    database_entry_id= 386136,
    url="https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!"
)





# Delete all entries

from smartabasepy.delete_all_database_entries import delete_all_database_entries

delete_all_database_entries(
    url = "https://learn.smartabase.com/ankle",
    form_name = "Allergies",
    username = "python.connector",
    password = "Connector123!",
    interactive_mode = True
)



# ----------



get_forms(
    url="https://sec.smartabase.com/conference",
    username = "byach",
    password = "Au8890601!",
    option=FormOption(interactive_mode=True)
)

get_database(
    url="https://sec.smartabase.com/conference",
    username = "byach",
    password = "Au8890601!",
    form_name="OSIICS v15",
    option = FormOption(interactive_mode=True, raw_output=False)
)

delete_all_database_entries(
    url="https://sec.smartabase.com/conference",
    username = "byach",
    password = "Au8890601!",
    form_name="OSIICS v15",
    interactive_mode = True
)
