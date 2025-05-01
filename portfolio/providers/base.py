from functools import wraps
import time
from abc import abstractmethod


def retry(n, wait):
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
                except Exception as e:
                    if attempt < n:
                        print(f'Attempt {attempt}/{n} failed. Retrying in {wait} seconds...')
                        time.sleep(wait)  # Wait before starting next attempt
                    else:
                        print(f'Attempt {attempt}/{n} failed. Max attempts reached.')
                        raise
        return wrapper
    return decorator


class DataProvider():
    """Parent class of data provider sub-classes."""

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
    def get_historical(self, tickers: list, sdate:str , edate:str , adj='unadjusted'):
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
            Options are:
            - 'unadjusted': Returns unadjusted data (default).
            - 'adjusted': Returns adjusted data (e.g., for dividends and splits).

        Returns
        -------
        pandas.DataFrame
            A dataframe of prices, indexed by dates (rows) and tickers (columns).
        """
        raise NotImplementedError('Historical data retrieval not supported by this provider.')
    

