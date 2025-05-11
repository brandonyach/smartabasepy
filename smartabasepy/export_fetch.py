from typing import Optional, List, Tuple, Union
from pandas import DataFrame
import pandas as pd
from .export_filter import EventFilter, ProfileFilter
from .utils import AMSClient
from .user_fetch import _fetch_user_data
from .user_filter import UserFilter
from .user_process import _flatten_user_response, _filter_by_about


def _fetch_user_ids(
    client: AMSClient,
    filter: Optional[Union[UserFilter, EventFilter, ProfileFilter]] = None,
    cache: bool = True
) -> Tuple[List[int], Optional[DataFrame]]:
    """Fetch user IDs and the raw user DataFrame for event or profile data export.

    Retrieves user IDs based on the provided filter, along with the raw user DataFrame for further processing.

    Args:
        client (AMSClient): The authenticated AMSClient instance.
        filter (Optional[Union[UserFilter, EventFilter, ProfileFilter]]): A filter object to narrow user selection.
        cache (bool): Whether to cache the API response (default: True).

    Returns:
        Tuple[List[int], Optional[DataFrame]]: A tuple containing the list of user IDs and the raw user DataFrame.

    Raises:
        AMSError: If no users are found.
    """
    data = _fetch_user_data(client, filter, cache)
    user_data = _flatten_user_response(data)
    if not user_data:
        return [], None
    
    user_df = pd.DataFrame(user_data)
    if user_df.empty:
        return [], None
    
    if filter and filter.user_key == "about" and filter.user_value:
        user_df = _filter_by_about(user_df, filter.user_value)
        if user_df.empty:
            return [], None
    
    user_ids = user_df["userId"].dropna().astype(int).tolist()
    return user_ids, user_df