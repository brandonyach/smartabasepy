from typing import Optional, Union, List
from .export_validate import _validate_user_filter_key, _validate_event_filter


class EventFilter:
    """Filter for selecting events in event data export.

    Defines criteria for filtering events based on user attributes (e.g., username, group),
    data fields (e.g., specific form field values), and the number of events per user.

    Args:
        user_key (Optional[str]): The user key to filter by ('about', 'username', 'email', 'uuid', 'group').
        user_value (Optional[Union[str, List[str]]]): The value(s) to filter users by.
        data_key (Optional[str]): The form field key to filter events by.
        data_value (Optional[Union[str, List[str]]]): The value(s) to filter events by.
        data_condition (Optional[str]): The condition for data filtering ('=', '!=', '>', '<', etc.).
        events_per_user (Optional[int]): The maximum number of events to retrieve per user.

    Attributes:
        user_key (Optional[str]): The user filter key.
        user_value (Optional[Union[str, List[str]]]): The user filter value(s).
        data_key (Optional[str]): The data field key.
        data_value (Optional[Union[str, List[str]]]): The data field value(s).
        data_condition (Optional[str]): The data filter condition.
        events_per_user (Optional[int]): The maximum events per user.

    Raises:
        ValueError: If the filter parameters are invalid.
    """
    def __init__(
        self,
        user_key: Optional[str] = None,
        user_value: Optional[Union[str, List[str]]] = None,
        data_key: Optional[str] = None,
        data_value: Optional[Union[str, List[str]]] = None,
        data_condition: Optional[str] = None,
        events_per_user: Optional[int] = None
    ):
        self.user_key = user_key
        self.user_value = user_value
        self.data_key = data_key
        self.data_value = data_value
        self.data_condition = data_condition
        self.events_per_user = events_per_user
        self._validate()

    def _validate(self) -> None:
        """Validate the event filter parameters."""
        _validate_event_filter(self.user_key, self.user_value, self.data_key, self.data_value, self.data_condition, self.events_per_user)
        


class SyncEventFilter:
    """Filter for selecting users in sync event data export.

    Defines criteria for filtering users based on a key (e.g., username, email) and value for the sync_event function.

    Args:
        user_key (Optional[str]): The type of filter ('about', 'username', 'email', 'group', 'current_group').
        user_value (Optional[Union[str, List[str]]]): The value(s) to filter by (e.g., a username, group name).

    Attributes:
        user_key (Optional[str]): The filter key.
        user_value (Optional[Union[str, List[str]]]): The filter value(s).

    Raises:
        ValueError: If the user_key is invalid.
    """
    def __init__(
        self, 
        user_key: Optional[str] = None, 
        user_value: Optional[Union[str, List[str]]] = None
    ):
        self.user_key = user_key
        self.user_value = user_value
        self._validate()

    def _validate(self) -> None:
        """Validate the sync event filter parameters."""
        valid_user_keys = {"about", "username", "email", "group", "current_group"}
        if self.user_key and self.user_key not in valid_user_keys:
            raise ValueError(f"Invalid user_key: '{self.user_key}'. Must be one of {valid_user_keys}")



class ProfileFilter:
    """Filter for selecting users in profile data export.

    Defines criteria for filtering users based on a key (e.g., username, email) and value.

    Args:
        user_key (Optional[str]): The type of filter ('group', 'username', 'email', 'about').
        user_value (Optional[Union[str, List[str]]]): The value(s) to filter by (e.g., group name, username).

    Attributes:
        user_key (Optional[str]): The filter key.
        user_value (Optional[Union[str, List[str]]]): The filter value(s).

    Raises:
        ValueError: If the user_key is invalid.
    """
    def __init__(
        self, 
        user_key: Optional[str] = None, 
        user_value: Optional[Union[str, List[str]]] = None
    ):
        self.user_key = user_key
        self.user_value = user_value
        self._validate()

    def _validate(self) -> None:
        """Validate the profile filter parameters."""
        _validate_user_filter_key(self.user_key)