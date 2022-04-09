"""
Functions for Google API
"""

import json


def get_auth_type(file):
    """ Load JSON file and detect Auth type
    """
    with open(file, 'r') as f:
        data = json.load(f)
    if 'type' in data.keys() and data['type'] == 'service_account':
        type = 'Service Account'
    elif 'installed' in data.keys():
        type = 'OAuth2'
    else:
        type = "unknown"
    return type
