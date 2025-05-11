from smartabasepy import delete_event_data, delete_multiple_events, DeleteEventOption


# Delete the event
delete_event_data(
    event_id = 2286313,
    url="https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!",
    option = DeleteEventOption(
        interactive_mode=True
    )
)



delete_multiple_events(
    event_ids = [2286315, 2286316],
    url="https://learn.smartabase.com/ankle",
    username = "python.connector",
    password = "Connector123!",
    option = DeleteEventOption(
        interactive_mode=True
    ) 
)
