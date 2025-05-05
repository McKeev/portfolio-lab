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
        self._index_set = set(self._hpr.index)

    @property
    def hpr(self):
        """User-facing property."""
        return self._hpr.copy()

    def apy(self, sdate=None, edate=None):
        """Returns the annualised return of the Asset."""
        ret = self._hpr_range(sdate, edate)
        return stats.gmean(ret + 1) ** self.freq.num() - 1

    def apr(self, sdate=None, edate=None):
        """Returns the Annualised Percentage Rate of the Asset."""
        ret = self._hpr_range(sdate, edate)
        return stats.gmean(ret + 1) * self.freq.num()

    def cumul(self, sdate=None, edate=None):
        """Returns a cumulative returns Series of the Asset."""

        ret = self._hpr_range(sdate, edate).copy()  # Mutability safeguard
        ret = ret + 1  # Get gross returns

        # Need to pre-pend 1 to keep return data intact if full sample
        if not sdate:
            start = ret.index[0]  # Get first date
            new_start = start - dt.timedelta(days=self.freq.days())
            ret.loc[pd.Timestamp(new_start)] = 1
            ret.sort_index(inplace=True)
        else:
            ret.iloc[0] = 1

        return ret.cumprod() - 1

    def tr(self, sdate=None, edate=None):
        """Returns the Total Return of the asset."""
        ret = self._hpr_range(sdate, edate).copy()  # Mutability safeguard
        cumret = ret + 1  # Get gross returns

        return np.prod(cumret) - 1

    def ytd(self):
        """Returns the Year-To-Date returns"""
        year = dt.date.today().year  # Get current year
        mask = self._hpr.index.year == year  # Create mask of current year
        first_date = self._hpr[mask].index.min()

        return self.tr(first_date, self.edate)

    def vol(self, sdate=None, edate=None):
        """Returns the annualised volatility of the Asset."""
        ret = self._hpr_range(sdate, edate)
        return ret.std(ddof=1) * np.sqrt(self.freq.num())

    def _hpr_range(self, sdate=None, edate=None):
        """Returns the Holding Period Returns in the specified interval."""
        if not (sdate or edate):
            return self._hpr

        # Determine the range to use
        if sdate and edate:
            date_range = sdate, edate
        elif sdate:
            date_range = sdate, self.edate
        else:
            date_range = self.sdate, edate

        # Check if both dates are in index
        if all(date in self._index_set for date in date_range):
            return self._hpr.loc[date_range[0]:date_range[1]]
        else:
            return self._adv_hpr_range(date_range[0], date_range[1])

    def _adv_hpr_range(self, sdate, edate):
        """
        Returns the Holding Period Returns in the specified interval.
        Used when dates are no in index
        """
        if sdate not in self._index_set:
            sdate = utils.closest_date(self._hpr.index, sdate)
        elif edate not in self._index_set:
            edate = utils.closest_date(self._hpr.index, edate)

        return self._hpr.loc[sdate:edate]
