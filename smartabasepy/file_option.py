from typing import Optional

class FileUploadOption:
    """Configuration options for file upload operations.

    Encapsulates options used by file upload functions, such as interactive mode, caching,
    and saving results to a file.

    Args:
        interactive_mode (bool): If True, display progress feedback during file uploads (default: False).
        cache (bool): If True, cache the API responses (default: True).
        save_to_file (Optional[str]): If provided, save the upload results to this file path as a CSV (default: None).
    """
    def __init__(
        self,
        interactive_mode: bool = False,
        cache: bool = True,
        save_to_file: Optional[str] = None
    ):
        self.interactive_mode = interactive_mode
        self.cache = cache
        self.save_to_file = save_to_file