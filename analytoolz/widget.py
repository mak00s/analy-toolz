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


def create_menu(dict, label=None):
    opt = [(d['name'], d['id']) for d in dict]
    return dropdown_menu(label, 'Please select', opt)


def create_ga_account_property_menu(accounts):
    # opt = [(d['name'], d['id']) for d in accounts]
    # menu1 = dropdown_menu('Account', 'Please select', opt)
    menu1 = create_menu(accounts, label='Account')
    menu2 = dropdown_menu('Property', '---')
    menu3 = dropdown_menu('View', '---')
    return menu1, menu2, menu3
