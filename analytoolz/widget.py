"""
Functions for widgets (forms)
"""

from ipywidgets import Dropdown
from typing import List, Tuple


def dropdown_menu(label: str, default: str, option_list: List[Tuple[str, str]] = []):
    """Create a drop-down menu
    """
    # set label
    options = [(default, '')]
    if option_list:
        # add options
        options.extend(option_list)
    return Dropdown(description=f"{label}: ", options=options)


def create_ga4_account_property_menu(accounts):
    opt = [(d['name'], d['id']) for d in accounts]
    menu1 = dropdown_menu('Account', 'Please select', opt)
    menu2 = dropdown_menu('Property', '---')
    return menu1, menu2