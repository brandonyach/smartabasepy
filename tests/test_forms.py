from smartabasepy import get_forms, get_form_schema, FormOption


# Forms
get_forms(
    url="https://learn.smartabase.com/ankle",
    username="python.connector",
    password="Connector123!",
    option=FormOption(interactive_mode=True)
)


# Form Schema

get_form_schema(
    form_name = "ACFT",
    url = "https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!",
    option = FormOption(
        interactive_mode=True,
        raw_output = False,
        include_instructions=True,
        field_details=True
    )
)