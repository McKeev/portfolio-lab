from .base import DataProvider
from .lseg import LSEG

def _copy_docstrings(parent_class):
    """
    Copies docstrings from the parent class to its child classes for methods
    that are defined in the parent class but overridden in the child class.

    Parameters
    ----------
    parent_class : class
        The parent class whose docstrings should be copied.
    """
    for child_class in parent_class.__subclasses__():
        for attr_name, attr_value in parent_class.__dict__.items():
            if callable(attr_value) and hasattr(child_class, attr_name):
                child_method = getattr(child_class, attr_name)
                if callable(child_method) and not child_method.__doc__:
                    child_method.__doc__ = attr_value.__doc__

# Automatically apply it when providers are loaded
_copy_docstrings(DataProvider)