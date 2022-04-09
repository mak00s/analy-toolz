"""
Functions for widgets (forms)
"""

from ipywidgets import Dropdown


def dropdown_menu(label, default, option_list=None):
    """Create drop-down menu
    """
    # set label
    opt = [(default, '')]
    if option_list:
        # add options
        opt.extend(option_list)
    return Dropdown(description=f"{label}: ", options=opt)
