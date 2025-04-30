from functools import wraps
import time
import refinitiv.data as rd
from abc import ABC, abstractmethod


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
        """Retrieve historical prices.
        
        Parameters
        ----------
        tickers : list
            List of tickers for which to retrieve prices.
        sdate : str
            Start date of fetch (format: 'YYYY-MM-DD').
        edate : str
            End date of fetch (format: 'YYYY-MM-DD').
        adj : str, optional
            Adjustment type for the data. Options are:
            - 'adjusted': Returns adjusted data (e.g., for dividends and splits).
            - 'unadjusted': Returns unadjusted data (default).

        Returns
        -------
        pandas.DataFrame
            A dataframe of prices, indexed by dates (rows) and tickers (columns).
        """
        raise NotImplementedError('Historical data retrieval not supported by this provider.')
    

    

class LSEG(DataProvider):

    def get_historical(self, tickers: list, sdate:str, edate:str, adj='unadjusted'):

        @retry(n=self.retry_limit, wait=self.wait)  # Add retry logic  
        def attempt():

            print('Attempting LSEG retrieval...')  # Give feedback to user

            if adj == 'unadjusted':
                output = rd.get_history(universe=tickers,
                                    fields=['TR.CLOSEPRICE(Adjusted=0)'],
                                    parameters={
                                            'SDate': sdate,
                                            'EDate': edate,
                                            'Curn': 'USD',
                                            'Frq': 'D',
                                            },
                                    )
            
            elif adj == 'adjusted':
                output = rd.get_history(universe=tickers,
                                    fields=['TR.CLOSEPRICE'],
                                    parameters={
                                            'SDate': sdate,
                                            'EDate': edate,
                                            'Curn': 'USD',
                                            'Frq': 'D',
                                            },
                                    )

            elif adj not in DataProvider._ADJ_OPTIONS:
                raise ValueError('Invalid adjustment option.\nPossible:{DataProvider._ADJ_OPTIONS}')
            
            else:
                raise NotImplementedError('The adjustment type is not supported by LSEG')
            
            if len(tickers) == 1:  # When fetching for one stock, the col name != the ticker
                output.columns = tickers  # Fix this to ensure compatibility
            
            return (output)        
        return attempt()
        
