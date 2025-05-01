from functools import wraps
import time
from abc import abstractmethod
import pandas as pd


def _retry(n: int, wait: int):
    """
    Decorator generator to retry execution up to `n` times.

    Parameters
    ----------
    n : int
        Maximum number of attempts.
    wait : int, optional
        Seconds to wait between attempts.

    Returns
    -------
    function
        A decorator that retries the wrapped function call.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Attempt a max of n times
            for attempt in range(1, n + 1):
                # Try to execute function
                try:
                    return func(*args, **kwargs)
                # Handle execution errors
                except Exception:
                    if attempt < n:
                        print(
                            f'Attempt {attempt}/{n} failed. '
                            f'Retrying in {wait} seconds...'
                        )
                        time.sleep(wait)  # Wait before starting next attempt
                    else:
                        print(f'Attempt {attempt}/{n} failed.'
                              f'Max attempts reached.')
                        raise
        return wrapper
    return decorator


def _treat_historical(historical: pd.DataFrame, freq: str):
    """Handle missing data and resampling to match different market timings.

    Resamples to the specified frequency, warns about missing values,
    and fills in missing data via linear interpolation.
    """
    historical = historical.copy()  # Prevent mutability issues

    freq_pd = {
        'D': 'D',
        'W': 'W',
        'M': 'ME',
        'Y': 'YE',
    }
    freq = freq_pd[freq]

    if freq != 'D':
        historical = historical.resample(freq).last()

    nrows, ncols = historical.shape
    missing = historical.isna().mean()  # Gives % missing per column

    # Warn user of potentially problematic assets
    for col, ratio in missing.items():
        if ratio > DataProvider.THRESHOLD:
            print(f'Missing {ratio:.2%} of values for {col}')

    # Fill in blanks
    tot_missing = historical.isna().sum().sum()
    if tot_missing:
        print(f'Interpolating {tot_missing:.0f} values '
              f'({tot_missing / (nrows * ncols):.2%})')
        historical = historical.interpolate('linear').bfill().ffill()

    return historical


class DataProvider():
    """Parent class of data provider sub-classes."""

    THRESHOLD = 0.05
    _ADJ_OPTIONS = ['adjusted', 'unadjusted']

    def __init__(self, retry_limit=3, wait=3):
        """Initialise a DataProvider object.

        Parameters
        ----------
        retry_limit : int, optional
            Maximum number of attempts to fetch data (default=3).
        wait : int, optional
            Seconds to wait between attempts (default=3).
        """
        self.retry_limit = retry_limit
        self.wait = wait

    @abstractmethod
    def get_historical(self,
                       tickers: list,
                       sdate: str,
                       edate: str,
                       adj='unadjusted',
                       freq='D'):
        """Retrieve historical prices from data provider.

        Parameters
        ----------
        tickers : list of str
            List of tickers for which to retrieve prices.
        sdate : str
            Start date of fetch (format: 'YYYY-MM-DD').
        edate : str
            End date of fetch (format: 'YYYY-MM-DD').
        adj : {'unadjusted', 'adjusted'}, optional
            Adjustment type for the data (default = 'unadjusted').
            Options:
            - 'unadjusted': Returns unadjusted data (default).
            - 'adjusted': Returns adjusted data (e.g., for dividends and
            splits).
        freq: {'D', 'W', 'M', 'Y'}, optional
            Frequency interval for fetch.

        Returns
        -------
        pandas.DataFrame
            A dataframe of prices, indexed by dates (rows) and tickers
            (columns).
        """
        raise NotImplementedError('Historical data retrieval not supported '
                                  'by this provider.')
