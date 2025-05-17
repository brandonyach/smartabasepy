import vcr

# tests/vcr_config.py
def vcr_config():
    return {
        "filter_headers": ["session-header", "Cookie", "Authorization"],
        "filter_query_parameters": ["username", "password"],
        "filter_post_data_parameters": ["username", "password"]
    }
    
# Apply configuration to all VCR cassettes
vcr.default_vcr = vcr.VCR(**vcr_config())