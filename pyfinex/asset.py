import pyfinex.utils as utils
import pandas as pd
import numpy as np
import datetime as dt
from scipy import stats


class Asset:
    """
    A basic object with returns data.

    Attributes
    ----------
    edate : datetime
        The end date of the asset's return data.
    freq : utils.Frequency
        The frequency at which the asset's data is evaluated.
    hpr : pd.Series
        The returns of the asset, at the specified frequency.
    name : str
        The asset's name.
    sdate : datetime
        The start date of the asset's return data.
    """

    def __init__(self, hpr: pd.Series, freq: str, name='Asset'):
        """
        Initializes an asset instance.

        Parameters
        ----------
        hpr : pd.Series
            The returns of the asset, at the specified frequency.
        freq : {'D', 'W', 'M', 'Y'}
            The frequency at which the portfolio price data is recorded.
        name : str, optional
            The asset's name (Default = 'Asset').
        """
        self._hpr = hpr
        self._hpr.name = name
        self.freq = utils.Frequency(freq)
        self.name = name
        self.sdate, self.edate = self.hpr.index[0], self.hpr.index[-1]

    @property
    def hpr(self):
        """User-facing property."""
        return self._hpr.copy()

    def apy(self, sdate: dt.date = None, edate: dt.date = None):
        """Returns the annualised return of the Asset."""
        ret = self._hpr_range(sdate, edate)
        return stats.gmean(ret + 1) ** self.freq.num() - 1

    def apr(self, sdate: dt.date = None, edate: dt.date = None):
        """Returns the Annualised Percentage Rate of the Asset."""
        ret = self._hpr_range(sdate, edate)
        return stats.gmean(ret + 1) * self.freq.num()

    def cumul(self, sdate: dt.date = None, edate: dt.date = None):
        """Returns a cumulative returns Series of the Asset."""

        ret = self._hpr_range(sdate, edate).copy()  # Mutability safeguard
        ret = ret + 1  # Get gross returns

        # Need to pre-pend 1 to keep return data intact
        start = ret.index[0]  # Get first date
        new_start = start - dt.timedelta(days=self.freq.days())
        ret.loc[pd.Timestamp(new_start)] = 1
        ret.sort_index(inplace=True)

        return ret.cumprod() - 1

    def tr(self, sdate: dt.date = None, edate: dt.date = None):
        """Returns the Total Return of the asset."""
        cumul = self.cumul(sdate, edate)
        return cumul.iloc[-1]

    def ytd(self):
        """Returns the Year-To-Date returns"""
        year = dt.date.today().year  # Get current year
        mask = self._hpr.index.year == year  # Create mask of current year
        first_date = self._hpr[mask].index.min()

        return self.tr(first_date, self.edate)

    def vol(self, sdate: dt.date = None, edate: dt.date = None):
        """Returns the annualised volatility of the Asset."""
        ret = self._hpr_range(sdate, edate)
        return ret.std(ddof=1) * np.sqrt(self.freq.num())

    def _hpr_range(self, sdate: dt.date = None, edate: dt.date = None):
        """Returns the Holding Period Returns in the specified interval."""
        if not (sdate or edate):
            return self._hpr
        elif sdate and edate:
            return self._hpr.loc[sdate:edate]
        elif sdate and (not edate):
            return self._hpr.loc[sdate:self.edate]
        elif (not sdate) and edate:
            return self._hpr.loc[self.sdate:edate]
