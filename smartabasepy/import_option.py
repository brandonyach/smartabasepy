from typing import Optional, List


class InsertEventOption:
    """Options for configuring the insert_event_data function.

    This class defines configuration options for inserting events into an AMS Event Form,
    including whether to display interactive feedback, cache API responses, specify the
    user identifier column, and define table fields.

    Args:
        interactive_mode: If True, display progress bars and interactive prompts. Defaults to False.
        cache: If True, cache API responses and reuse the client session. Defaults to True.
        id_col: The column name to use for mapping user identifiers to user IDs.
            Must be one of 'user_id', 'about', 'username', or 'email'. Defaults to 'user_id'.
        table_fields: List of field names that are table fields in the form. If None, treated as a non-table form.
            Defaults to None.

    Attributes:
        interactive_mode: Boolean indicating if interactive feedback is enabled.
        cache: Boolean indicating if caching is enabled.
        id_col: The column name used for mapping user identifiers.
        table_fields: List of table field names, or an empty list if None.

    Raises:
        ValueError: If id_col is not one of the allowed values.
    """
    
    def __init__(
            self, 
            interactive_mode: bool = False, 
            cache: bool = True, 
            id_col: str = "user_id", 
            table_fields: Optional[List[str]] = None
        ):
        
        self.interactive_mode = interactive_mode
        
        self.cache = cache
        
        if id_col not in ["user_id", "about", "username", "email"]:
            raise ValueError("id_col must be 'user_id', 'about', 'username', or 'email'.")
        
        self.id_col = id_col
        
        self.table_fields = table_fields if table_fields is not None else []



class UpdateEventOption:
    """Options for configuring the update_event_data function.

    This class defines configuration options for updating existing events in an AMS Event Form,
    including whether to display interactive feedback, cache API responses, specify the
    user identifier column, and define table fields.

    Args:
        interactive_mode: If True, display progress bars and interactive prompts. Defaults to False.
        cache: If True, cache API responses and reuse the client session. Defaults to True.
        id_col: The column name to use for mapping user identifiers to user IDs.
            Must be one of 'user_id', 'about', 'username', or 'email'. Defaults to 'user_id'.
        table_fields: List of field names that are table fields in the form. If None, treated as a non-table form.
            Defaults to None.

    Attributes:
        interactive_mode: Boolean indicating if interactive feedback is enabled.
        cache: Boolean indicating if caching is enabled.
        id_col: The column name used for mapping user identifiers.
        table_fields: List of table field names, or an empty list if None.

    Raises:
        ValueError: If id_col is not one of the allowed values.
    """
    def __init__(
            self, 
            interactive_mode: bool = False, 
            cache: bool = True, 
            id_col: str = "user_id", 
            table_fields: Optional[List[str]] = None
        ):
        
        self.interactive_mode = interactive_mode
        
        self.cache = cache
        
        if id_col not in ["user_id", "about", "username", "email"]:
            raise ValueError("id_col must be 'user_id', 'about', 'username', or 'email'.")
        
        self.id_col = id_col
        
        self.table_fields = table_fields if table_fields is not None else []



class UpsertEventOption:
    """Options for configuring the upsert_event_data function.

    This class defines configuration options for upserting events (updating existing and inserting new)
    in an AMS Event Form, including whether to display interactive feedback, cache API responses,
    specify the user identifier column, and define table fields.

    Args:
        interactive_mode: If True, display progress bars and interactive prompts. Defaults to False.
        cache: If True, cache API responses and reuse the client session. Defaults to True.
        id_col: The column name to use for mapping user identifiers to user IDs.
            Must be one of 'user_id', 'about', 'username', or 'email'. Defaults to 'user_id'.
        table_fields: List of field names that are table fields in the form. If None, treated as a non-table form.
            Defaults to None.

    Attributes:
        interactive_mode: Boolean indicating if interactive feedback is enabled.
        cache: Boolean indicating if caching is enabled.
        id_col: The column name used for mapping user identifiers.
        table_fields: List of table field names, or an empty list if None.

    Raises:
        ValueError: If id_col is not one of the allowed values.
    """
    def __init__(
            self, 
            interactive_mode: bool = False, cache: bool = True, 
            id_col: str = "user_id", 
            table_fields: Optional[List[str]] = None
        ):
        
        self.interactive_mode = interactive_mode
        
        self.cache = cache
        
        if id_col not in ["user_id", "about", "username", "email"]:
            raise ValueError("id_col must be 'user_id', 'about', 'username', or 'email'.")
        
        self.id_col = id_col
        
        self.table_fields = table_fields if table_fields is not None else []
        
        

class UpsertProfileOption:
    """Options for configuring the upsert_profile_data function.

    This class defines configuration options for upserting profile data in an AMS Profile Form,
    including whether to display interactive feedback, cache API responses, and specify the user
    identifier column. Profile forms do not support table fields.

    Args:
        interactive_mode: If True, display progress bars and interactive prompts. Defaults to False.
        cache: If True, cache API responses and reuse the client session. Defaults to True.
        id_col: The column name to use for mapping user identifiers to user IDs.
            Must be one of 'user_id', 'about', 'username', or 'email'. Defaults to 'user_id'.

    Attributes:
        interactive_mode: Boolean indicating if interactive feedback is enabled.
        cache: Boolean indicating if caching is enabled.
        id_col: The column name used for mapping user identifiers.

    Raises:
        ValueError: If id_col is not one of the allowed values.
    """
    def __init__(
            self, 
            interactive_mode: bool = False, 
            cache: bool = True, 
            id_col: str = "user_id"
        ):
        self.interactive_mode = interactive_mode
        self.cache = cache
        if id_col not in ["user_id", "about", "username", "email"]:
            raise ValueError("id_col must be 'user_id', 'about', 'username', or 'email'.")
        self.id_col = id_col