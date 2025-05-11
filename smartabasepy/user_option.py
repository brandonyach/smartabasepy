from typing import Optional, List

class UserOption:
    """Options for Options for user-related operations such as get_user, create_user, and edit_user.

    Defines customization options for the `get_user` function, such as which columns to include,
    whether to cache API responses, and whether to display interactive feedback.

    Args:
        columns (Optional[List[str]]): Specific columns to include in the output DataFrame for `get_user` .  Ignored by `edit_user` and `create_user`. (default: None).
        cache (bool): Whether to cache API responses (default: True).
        interactive_mode (bool): Whether to print status messages during execution (default: True).

    Attributes:
        columns (Optional[List[str]]): The columns to include in the output.
        cache (bool): Whether caching is enabled.
        interactive_mode (bool): Whether interactive mode is enabled.
    """
    def __init__(
        self, 
        columns: Optional[List[str]] = None, 
        cache: bool = True, 
        interactive_mode: bool = True
    ):
        self.columns = columns
        self.cache = cache
        self.interactive_mode = interactive_mode



class GroupOption:
    """Options for configuring group data export.

    Defines customization options for the `get_group` function, such as guessing column types,
    caching API responses, and displaying interactive feedback.

    Args:
        guess_col_type (bool): Whether to infer column data types (default: True).
        interactive_mode (bool): Whether to print status messages during execution (default: True).
        cache (bool): Whether to cache API responses (default: True).

    Attributes:
        guess_col_type (bool): Whether to guess column types.
        interactive_mode (bool): Whether interactive mode is enabled.
        cache (bool): Whether caching is enabled.
    """
    def __init__(
        self, 
        guess_col_type: bool = True, 
        interactive_mode: bool = True, 
        cache: bool = True
    ):
        self.guess_col_type = guess_col_type
        self.interactive_mode = interactive_mode
        self.cache = cache