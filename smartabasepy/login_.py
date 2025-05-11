from typing import Optional, Dict, Any
from .utils import AMSClient, AMSError
from .login_option import LoginOption


def login(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[LoginOption] = None
) -> Dict[str, Any]:
    """Log into an AMS instance and return a login object.

    Authenticates with the AMS API using the provided credentials and returns a login object
    containing the session details. This function can be used standalone to verify credentials or
    for troubleshooting authentication issues.

    Args:
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        option (Optional[LoginOption]): A LoginOption object for customization (e.g., interactive mode).

    Returns:
        Dict[str, Any]: A dictionary containing:
            - login_data: The JSON response from the login API.
            - session_header: The session header from the response headers.
            - cookie: The cookie from the response headers.

    Raises:
        AMSError: If authentication fails or required credentials are missing.
    """
    option = option or LoginOption()
    
    if option.interactive_mode:
        username_display = username or "AMS_USERNAME"
        print(f"ℹ Logging {username_display} into {url}...")
    
    try:
        # Create an AMSClient instance to handle authentication
        # cache=False to ensure this client is independent of the persistent client
        client = AMSClient(url, username, password)
        
        login_object = {
            "login_data": client.login_data,
            "session_header": client.session_header,
            "cookie": f"JSESSIONID={client.session_header}"
        }
        
        if option.interactive_mode:
            print(f"✔ Successfully logged {username_display} into {url}.")
        
        return login_object
    
    except AMSError as e:
        if option.interactive_mode:
            print(f"✖ Failed to log {username_display} into {url}: {str(e)}")
