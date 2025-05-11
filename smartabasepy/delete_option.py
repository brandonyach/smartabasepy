from typing import Optional

class DeleteEventOption:
    """Options for configuring event data deletion.

    Defines customization options for the `delete_event_data` function, such as displaying interactive feedback.

    Args:
        interactive_mode (bool): Whether to print status messages during execution (default: True).

    Attributes:
        interactive_mode (bool): Whether interactive mode is enabled.
    """
    def __init__(
        self, 
        interactive_mode: bool = True
    ):
        self.interactive_mode = interactive_mode