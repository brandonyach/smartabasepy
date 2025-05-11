from typing import Optional, List


class EventOption:  
    """Options for configuring event data export.

    Defines customization options for the `get_event_data` function, such as downloading attachments,
    cleaning column names, caching API responses, and displaying interactive feedback.

    Args:
        interactive_mode (bool): Whether to print status messages during execution (default: True).
        guess_col_type (bool): Whether to infer column data types (default: True).
        convert_dates (bool): Whether to convert date columns to datetime objects (default: False).
        clean_names (bool): Whether to clean column names (e.g., lowercase, replace spaces) (default: False).
        cache (bool): Whether to cache API responses (default: True).
        include_missing_users (bool): Whether to include events for users not found in user data (default: False).
        download_attachment (bool): Whether to download attachments associated with events (default: False).
        attachment_directory (Optional[str]): Directory to save downloaded attachments. If None, uses the current working directory (default: None).

    Attributes:
        interactive_mode (bool): Whether interactive mode is enabled.
        guess_col_type (bool): Whether to guess column types.
        convert_dates (bool): Whether to convert dates.
        clean_names (bool): Whether to clean column names.
        cache (bool): Whether caching is enabled.
        include_missing_users (bool): Whether to include missing users.
        download_attachment (bool): Whether to download attachments.
        attachment_directory (Optional[str]): The directory for saving attachments.
        attachment_count (int): The number of attachments downloaded (set during processing).
    """
    def __init__(
        self,
        interactive_mode: bool = True,
        guess_col_type: bool = True,
        convert_dates: bool = False,
        clean_names: bool = False,
        cache: bool = True,
        include_missing_users: bool = False,
        download_attachment: bool = False,
        attachment_directory: Optional[str] = None
    ):
        self.interactive_mode = interactive_mode
        self.guess_col_type = guess_col_type
        self.convert_dates = convert_dates
        self.clean_names = clean_names
        self.cache = cache
        self.include_missing_users = include_missing_users
        self.download_attachment = download_attachment
        self.attachment_directory = attachment_directory
        self.attachment_count = 0
        
        
        
class SyncEventOption:
    """Options for configuring sync event data export.

    Defines customization options for the `sync_event` function, such as including user data,
    caching API responses, and displaying interactive feedback.

    Args:
        interactive_mode (bool): Whether to print status messages during execution (default: True).
        include_user_data (bool): Whether to include user metadata (e.g., 'about') in the output (default: True).
        cache (bool): Whether to cache API responses (default: True).
        include_missing_users (bool): Whether to include users from the filter without event data (default: False).
        guess_col_type (bool): Whether to infer column data types (default: True).
        include_uuid (bool): Whether to include the user's UUID in the output (default: False).

    Attributes:
        interactive_mode (bool): Whether interactive mode is enabled.
        include_user_data (bool): Whether to include user data.
        cache (bool): Whether caching is enabled.
        include_missing_users (bool): Whether to include missing users.
        guess_col_type (bool): Whether to guess column types.
        include_uuid (bool): Whether to include UUIDs.
    """
    def __init__(
        self,
        interactive_mode: bool = True,
        include_user_data: bool = True,
        cache: bool = True,
        include_missing_users: bool = False,
        guess_col_type: bool = True,
        include_uuid: bool = False
    ):
        self.interactive_mode = interactive_mode
        self.include_user_data = include_user_data
        self.cache = cache
        self.include_missing_users = include_missing_users
        self.guess_col_type = guess_col_type
        self.include_uuid = include_uuid
        
        

class ProfileOption:
    """Options for configuring profile data export.

    Defines customization options for the `get_profile_data` function, such as cleaning column names,
    caching API responses, and displaying interactive feedback.

    Args:
        interactive_mode (bool): Whether to print status messages during execution (default: True).
        guess_col_type (bool): Whether to infer column data types (default: True).
        clean_names (bool): Whether to clean column names (e.g., lowercase, replace spaces) (default: False).
        cache (bool): Whether to cache API responses (default: True).
        include_missing_users (bool): Whether to include users from the filter without profile data (default: False).

    Attributes:
        interactive_mode (bool): Whether interactive mode is enabled.
        guess_col_type (bool): Whether to guess column types.
        clean_names (bool): Whether to clean column names.
        cache (bool): Whether caching is enabled.
        include_missing_users (bool): Whether to include missing users.
    """
    def __init__(
        self,
        interactive_mode: bool = True,
        guess_col_type: bool = True,
        clean_names: bool = False,
        cache: bool = True,
        include_missing_users: bool = False
    ):
        self.interactive_mode = interactive_mode
        self.guess_col_type = guess_col_type
        self.clean_names = clean_names
        self.cache = cache
        self.include_missing_users = include_missing_users