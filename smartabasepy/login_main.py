from typing import Optional, Dict, Any
from .utils import AMSClient, AMSError
from .login_option import LoginOption
import os


def login(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    option: Optional[LoginOption] = None
) -> Dict[str, Any]:
    """Authenticate with an AMS instance and return session details.

    Logs into the AMS API using the provided credentials, establishing a session for
    subsequent API calls. Returns a dictionary containing the login response, session
    header, and cookie. Useful for verifying credentials, troubleshooting authentication,
    or initializing a client for manual API interactions. Provides interactive feedback
    if enabled, reporting login success or failure.

    Args:
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
            Must include a valid site name.
        username (Optional[str]): The username for authentication. If None, uses the
            AMS_USERNAME environment variable. Defaults to None.
        password (Optional[str]): The password for authentication. If None, uses the
            AMS_PASSWORD environment variable. Defaults to None.
        option (Optional[LoginOption]): Configuration options for the login process,
            including interactive_mode (for status messages). If None, uses default
            LoginOption. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'login_data': The JSON response from the login API, including user details
              (e.g., user ID, application ID).
            - 'session_header': The session header from the response headers, used for
              subsequent API calls.
            - 'cookie': The formatted cookie string (e.g., 'JSESSIONID = session_header').

    Raises:
        AMSError: If the URL is invalid, credentials are missing or invalid, the login
            request fails (e.g., HTTP 401), or the session header is not provided.

    Examples:
        >>> from smartabasepy import login
        >>> from smartabasepy import LoginOption
        >>> login_result = login(
        ...     url = "https://example.smartabase.com/site",
        ...     username = "user",
        ...     password = "pass",
        ...     option = LoginOption(interactive_mode = True)
        ... )
        ℹ Logging user into https://example.smartabase.com/site...
        ✔ Successfully logged user into https://example.smartabase.com/site.
        >>> print(login_result.keys())
        dict_keys(['login_data', 'session_header', 'cookie'])
        >>> print(login_result['cookie'])
        JSESSIONID=abc123
    """
    option = option or LoginOption()
    
    if option.interactive_mode:
        username_display = username or "AMS_USERNAME"
        print(f"ℹ Logging {username_display} into {url}...")
    
    # Validate credentials
    if not username or not password:
        username = os.getenv("AMS_USERNAME")
        password = os.getenv("AMS_PASSWORD")
        if not username or not password:
            if option.interactive_mode:
                print(f"✖ Failed to log {username_display} into {url}: No valid credentials provided")
            raise AMSError("No valid credentials provided. Supply 'username' and 'password' or set AMS_USERNAME/AMS_PASSWORD env vars.")
    
    try:
        client = AMSClient(url, username, password)
        
        if not client.authenticated:
            if option.interactive_mode:
                print(f"✖ Failed to log {username} into {url}: Authentication failed")
            raise AMSError("Authentication failed")
        
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
