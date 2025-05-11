from typing import Dict
from .utils import AMSClient


def _fetch_database_data(
    form_id: int,
    limit: int,
    offset: int,
    client: AMSClient,
    cache: bool = True
) -> Dict:
    """Fetch database entries from the AMS API.

    Args:
        form_id (int): The ID of the database form.
        limit (int): The maximum number of entries to return.
        offset (int): The offset of the first item to return.
        client (AMSClient): The AMSClient instance.
        cache (bool): Whether to cache the API response (default: True).

    Returns:
        Dict: The raw API response containing database entries.

    Raises:
        AMSError: If the API request fails.
    """
    payload = {
        "databaseFormId": int(form_id),
        "limit": limit,
        "offset": offset
    }
    response = client._fetch(
        "userdefineddatabase/findTableByDatabaseFormId",
        method="POST",
        payload=payload,
        cache=cache,
        api_version="v2"
    )
    return response