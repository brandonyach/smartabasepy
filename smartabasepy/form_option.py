class FormOption:
    """Options for configuring form schema export and summary functions.

    Defines customization options for functions like `get_forms` and `get_form_summary`, such as
    caching API responses, displaying interactive feedback, controlling output format, and
    specifying additional details to include in the summary.

    Args:
        interactive_mode (bool): Whether to print status messages during execution (default: False).
        cache (bool): Whether to cache API responses (default: True).
        raw_output (bool): Whether to return the raw API response instead of a formatted summary (default: False).
        field_details (bool): Whether to include field details (options, scores, dateSelection) in the summary (default: False).
        include_instructions (bool): Whether to include instructions for sections and fields in the summary (default: False).

    Attributes:
        interactive_mode (bool): Whether interactive mode is enabled.
        cache (bool): Whether caching is enabled.
        raw_output (bool): Whether to return the raw API response.
        field_details (bool): Whether to include field details in the summary.
        include_instructions (bool): Whether to include instructions in the summary.
    """
    def __init__(
            self, 
            interactive_mode: bool = False, 
            cache: bool = True,
            raw_output: bool = False,
            field_details: bool = False,
            include_instructions: bool = False
        ):
        self.interactive_mode = interactive_mode
        self.cache = cache
        self.raw_output = raw_output
        self.field_details = field_details
        self.include_instructions = include_instructions