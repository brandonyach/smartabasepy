import requests
import os
from datetime import datetime
import hashlib
from typing import Optional, Dict, Tuple


class AMSError(Exception):
    """Base exception for AMS operations, including login, export, and import errors.

    This exception is raised for all errors encountered during interactions with the AMS API,
    such as authentication failures, invalid API responses, or data validation issues.

    Attributes:
        message (str): The error message describing the issue.
    """
    pass

class AMSClient:
    """A client for interacting with the AMS API.

    Handles authentication, API requests, and caching for AMS operations. This class is used
    internally by export and import functions to communicate with the AMS API.

    Args:
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.

    Attributes:
        url (str): The validated AMS instance URL.
        app_name (str): The site name extracted from the URL.
        headers (Dict[str, str]): HTTP headers for API requests.
        username (str): The username used for authentication.
        password (str): The password used for authentication.
        authenticated (bool): Whether the client is authenticated.
        session (requests.Session): The HTTP session for making API requests.
        login_data (Dict): The response data from the login API call.
        _cache (Dict[str, Dict]): Cache for API responses.
    """
    def __init__(
            self, 
            url: str, 
            username: Optional[str] = None, 
            password: Optional[str] = None
        ):
        self.url = self._validate_url(url)
        self.app_name = self.url.split('/')[-1].strip()
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "python-test",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "X-APP-ID": "external.example.postman"
        }
        self._cache: Dict[str, Dict] = {}
        self.username = username or os.getenv("AMS_USERNAME")
        self.password = password or os.getenv("AMS_PASSWORD")
        self.authenticated = False
        self.session = requests.Session()
        self.login_data = {}
        self.last_uploaded_files = []
        if self.username and self.password:
            self.session.auth = (self.username, self.password)
            self.login()


    def login(self) -> None:
        """Authenticate with AMS and store login data.

        Sends a login request to the AMS API using the provided username and password.
        Updates the session headers with the session token received from the server.

        Raises:
            AMSError: If login fails due to invalid credentials, URL, or missing session header.
        """
        if not self.username or not self.password:
            raise AMSError("No valid credentials provided for login. Supply 'username' and 'password' or set AMS_USERNAME/AMS_PASSWORD env vars.")
        login_url = self._AMS_url("user/loginUser", api_version="v2")
        payload = {
            "username": self.username,
            "password": self.password,
            "loginProperties": {"appName": self.app_name, "clientTime": datetime.now().isoformat()[:19]}
        }
        response = self.session.post(login_url, json=payload, headers=self.headers)
        if response.status_code != 200:
            error_text = response.text.lower()
            if response.status_code == 401:
                raise AMSError("Invalid URL or login credentials.")
            raise AMSError(f"Login failed with status {response.status_code}.")
        self.session_header = response.headers.get("session-header")
        if not self.session_header:
            raise AMSError("No session header received from server.")
        
        # Parse the response and check for login exceptions
        self.login_data = response.json()
        if self.login_data.get('__is_rpc_exception__', False):
            error_message = self.login_data.get('value', {}).get('detailMessage', 'Unknown login error')
            raise AMSError(f"Login failed: {error_message}")
        
        self.headers["session-header"] = self.session_header
        self.headers["Cookie"] = f"JSESSIONID={self.session_header}"
        self.session.headers.update(self.headers)
        self.authenticated = True

    
    
    def _AMS_url(self, endpoint: str, api_version: str = "v1") -> str:
        """Construct an AMS API URL for the given endpoint and API version.

        Args:
            endpoint (str): The API endpoint (e.g., 'usersearch').
            api_version (str): The API version to use (default: 'v1').

        Returns:
            str: The full API URL (e.g., 'https://example.smartabase.com/site/api/v1/usersearch?informat=json&format=json').
        """
        return f"{self.url}/api/{api_version}/{endpoint.lstrip('/')}?informat=json&format=json"

    
    
    def _fetch(
            self, 
            endpoint: str, 
            method: str = "POST", 
            payload: Optional[dict] = None, 
            cache: bool = True, 
            api_version: str = "v1"
        ):
        """Fetch data from the AMS API with caching.

        Sends an HTTP request to the specified endpoint and returns the JSON response. Uses caching to
        avoid redundant API calls if enabled.

        Args:
            endpoint (str): The API endpoint to fetch (e.g., 'usersearch').
            method (str): HTTP method to use ('POST' or 'GET', default: 'POST').
            payload (Optional[dict]): The JSON payload to send with the request (default: None).
            cache (bool): Whether to cache the response (default: True).
            api_version (str): The API version to use for this request (default: 'v1').

        Returns:
            Any: The JSON response from the API, or None if the response is empty.

        Raises:
            AMSError: If the API request fails or returns a non-200 status code.
        """
        if not self.authenticated:
            self.login()
        cache_key = hashlib.sha256(f"{self.url}{endpoint}{str(payload or '')}".encode()).hexdigest()
        if cache and cache_key in self._cache:
            return self._cache[cache_key]
        url = self._AMS_url(endpoint, api_version=api_version) if method == "POST" else f"{self.url}/api/v3/{endpoint.lstrip('/')}"
        kwargs = {"headers": self.headers}
        if payload and method != "GET":
            kwargs["json"] = payload
        response = self.session.request(method, url, **kwargs)
        if response.status_code != 200:
            raise AMSError(f"Failed to fetch data from {endpoint} (status {response.status_code}): {response.text}")
        # Handle empty responses (e.g., null for deleteAll endpoint)
        try:
            data = response.json()
        except ValueError:
            data = None  
        if cache:
            self._cache[cache_key] = data
        else:
            self._cache.clear()  # Clear cache if cache=False
        return data
    
    

    def _validate_url(self, url: str) -> str:
        """Validate the AMS URL.

        Args:
            url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').

        Returns:
            str: The validated URL with trailing slashes removed.

        Raises:
            AMSError: If the URL is invalid or missing a site name.
        """
        app_name = url.rstrip('/').split('/')[-1].strip()
        if not app_name:
            raise AMSError("Invalid AMS URL. Ensure it includes a valid site name (e.g., 'https://example.smartabase.com/site_name').")
        return url.rstrip('/')
    
    
    def _validate_credentials(username: Optional[str], password: Optional[str]) -> Tuple[str, str]: 
        """Validate username and password for AMSClient.

        Args:
            username (Optional[str]): The username to validate.
            password (Optional[str]): The password to validate.

        Returns:
            Tuple[str, str]: The validated username and password.

        Raises:
            AMSError: If no valid credentials are provided.
        """
        if username and password:
            return username, password
        env_username = os.getenv("AMS_USERNAME")
        env_password = os.getenv("AMS_PASSWORD")
        if env_username and env_password:
            return env_username, env_password
        _raise_ams_error(
            "No credentials provided. Set via args or environment variables (AMS_USERNAME and AMS_PASSWORD).",
            function="validate_credentials"
        )

persistent_client: Optional['AMSClient'] = None


def get_client(
        url: str, 
        username: Optional[str] = None, 
        password: Optional[str] = None, 
        cache: bool = True, 
        interactive_mode: bool = False
    ) -> AMSClient:
    """Get or create an AMSClient, managing persistence based on caching.

    Creates a new AMSClient instance or reuses an existing one if caching is enabled and the client
    is already authenticated. Prints a success message if interactive_mode is enabled.

    Args:
        url (str): The AMS instance URL (e.g., 'https://example.smartabase.com/site').
        username (Optional[str]): The username for authentication. If None, uses AMS_USERNAME env var.
        password (Optional[str]): The password for authentication. If None, uses AMS_PASSWORD env var.
        cache (bool): Whether to cache the client instance (default: True).
        interactive_mode (bool): Whether to print status messages (default: False).

    Returns:
        AMSClient: An authenticated AMSClient instance.

    Raises:
        AMSError: If no valid credentials are provided and no cached client is available.
    """
    global persistent_client
    if cache and persistent_client and persistent_client.authenticated:
        return persistent_client
    
    if not username or not password:
        raise AMSError("No valid credentials provided and no cached client available. Supply 'username' and 'password'.")
    
    client = AMSClient(url, username, password)
    if interactive_mode:
        print(f"✔ Successfully logged {username} into {url}.")
    
    if cache:
        persistent_client = client  # Persist only if cache=True
    else:
        persistent_client = None  # Clear if cache=False
    
    return client


def _raise_ams_error(
    message: str,
    function: str,
    endpoint: Optional[str] = None,
    status_code: Optional[int] = None
) -> None:
    """Raise an AMSError with a formatted message for any operation.

    Constructs a detailed error message including the provided message, function name,
    and optional endpoint and status code, then raises an AMSError.

    Args:
        message (str): The primary error message.
        function (str): The name of the function where the error occurred.
        endpoint (Optional[str]): The API endpoint involved in the error.
        status_code (Optional[int]): The HTTP status code of the error.

    Raises:
        AMSError: With the formatted error message.
    """
    error_parts = [message, f"Function: {function}"]
    if endpoint:
        error_parts.append(f"Endpoint: {endpoint}")
    if status_code:
        error_parts.append(f"Status Code: {status_code}")
    full_message = " - ".join(error_parts) + ". Please check inputs or contact your site administrator."
    raise AMSError(full_message)