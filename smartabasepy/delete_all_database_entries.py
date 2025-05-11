from smartabasepy.database_ import get_database, delete_database_entry
from smartabasepy.form_option import FormOption
from pandas import DataFrame
from tqdm import tqdm

def delete_all_database_entries(
    form_name: str,
    url: str,
    username: str,
    password: str,
    interactive_mode: bool = True
) -> tuple[int, DataFrame]:
    """Delete all entries from a specified AMS database form.

    Fetches all database entries for the specified form using pagination, extracts their IDs, and deletes each entry
    using the delete_database_entry function. Stores failed deletions in a DataFrame and prints them if interactive mode is enabled.

    Args:
        form_name (str): The name of the AMS database form to delete entries from.
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (str): The username for authentication.
        password (str): The password for authentication.
        interactive_mode (bool): Whether to print status messages and progress (default: True).

    Returns:
        tuple[int, DataFrame]: A tuple containing:
            - The total number of entries successfully deleted.
            - A DataFrame containing failed deletions with columns ['id', 'reason'].
    """
    # Initialize the FormOption with interactive mode
    option = FormOption(interactive_mode=interactive_mode)

    # Step 1: Fetch all database entries with pagination
    if option.interactive_mode:
        print(f"ℹ Fetching all database entries for form '{form_name}' to delete...")

    limit = 1000  # Fetch 1000 entries at a time
    offset = 0
    all_entry_ids = []
    total_deleted = 0

    while True:
        # Unpack only two values: df and total_row_count
        df, total_row_count = get_database(
            form_name=form_name,
            url=url,
            username=username,
            password=password,
            limit=limit,
            offset=offset,
            option=option
        )

        if not isinstance(df, DataFrame):
            print(f"Error: Expected a DataFrame from get_database, got: {type(df)}")
            break

        if df.empty:
            break

        # Extract entry IDs
        entry_ids = df["id"].dropna().astype(int).tolist()
        all_entry_ids.extend(entry_ids)

        # Check if we've fetched all entries
        offset += limit
        if offset >= total_row_count:
            break

    if not all_entry_ids:
        if option.interactive_mode:
            print(f"ℹ No database entries found for form '{form_name}' to delete.")
        return 0, DataFrame(columns=["id", "reason"])

    # Step 2: Delete each entry
    if option.interactive_mode:
        print(f"ℹ Deleting {len(all_entry_ids)} database entries for form '{form_name}'...")

    failed_deletions = []
    for entry_id in tqdm(all_entry_ids, desc="Deleting entries", disable=not option.interactive_mode):
        try:
            success = delete_database_entry(
                database_entry_id=entry_id,
                url=url,
                username=username,
                password=password
            )
            if success:
                total_deleted += 1
            else:
                failed_deletions.append({"id": entry_id, "reason": "Deletion failed without error"})
        except Exception as e:
            failed_deletions.append({"id": entry_id, "reason": str(e)})
            if option.interactive_mode:
                print(f"⚠️ Failed to delete entry ID {entry_id}: {str(e)}")

    # Step 3: Create DataFrame for failed deletions
    failed_df = DataFrame(failed_deletions, columns=["id", "reason"])

    # Step 4: Report results
    if failed_df.empty:
        if option.interactive_mode:
            print(f"\n✔ Successfully deleted all {total_deleted} database entries for form '{form_name}'.")
    else:
        if option.interactive_mode:
            print(f"\n⚠️ Failed to delete {len(failed_df)} entries:")
            print(failed_df.to_string(index=False))

    return total_deleted, failed_df

# Example usage
if __name__ == "__main__":
    url = "https://learn.smartabase.com/ankle"
    form_name = "Allergies"
    username = "python.connector"
    password = "Connector123!"
    interactive_mode = True

    total_deleted, failed_df = delete_all_database_entries(
        form_name=form_name,
        url=url,
        username=username,
        password=password,
        interactive_mode=interactive_mode
    )

    print(f"Total entries deleted: {total_deleted}")
    print("Failed deletions:")
    print(failed_df)