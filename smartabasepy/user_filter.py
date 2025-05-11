from typing import Optional, Union, List
from .export_validate import _validate_user_filter_key


class UserFilter:
    """Filter for selecting users in user data export.

    Defines criteria for filtering users based on a key (e.g., username, email) and value.

    Args:
        user_key (Optional[str]): The type of filter ('username', 'email', 'group', 'about').
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
        """Validate the user filter parameters."""
        _validate_user_filter_key(self.user_key)