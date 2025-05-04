from enum import Enum
import pandas as pd
import numpy as np


class Frequency(Enum):
    """
    A simple frequency class.

    Attributes
    ----------
    value : {'D', 'W', 'M', 'Y'}
        The one letter acronym.
    name : {'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY'}
        The frequency, in readable format.
    """
    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"
    YEARLY = "Y"

    def num(self):
        mapping = {
            'D': 252.0,
            'W': 52.0,
            'M': 12.0,
            'Y': 1.0,
        }
        return mapping[self.value]

    def days(self):
        mapping = {
            'D': 1.0,
            'W': 7.0,
            'M': 30.0,
            'Y': 360.0,
        }
        return mapping[self.value]

    def f_pandas(self):
        mapping = {
            'D': 'D',
            'W': 'W',
            'M': 'ME',
            'Y': 'YE',
        }
        return mapping[self.value]

    def f_lseg(self):
        mapping = {
            "D": "D",
            "W": "W",
            "M": "M",
            "Y": "Y",
        }
        return mapping[self.value]


def closest_date(index: pd.DatetimeIndex, target):
    """Vectorized operation to find closest index."""
    target = pd.Timestamp(target)
    closest_id = np.abs(index - target).argmin()
    closest_dt = index[closest_id]

    return closest_dt
