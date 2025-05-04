import pandas as pd
from pyfinex.holdings import Holdings


class Portfolio:
    """
    A class to represent a portfolio consisting of holdings and their
    corresponding prices.

    Attributes
    ----------
    cash_holdings : pd.Series
        A Series representing the cash holdings in the portfolio.
    cashflows : pd.DataFrame
        A DataFrame containing the cashflows associated with the portfolio.
    edate : datetime
        The end date of the portfolio's return data.
    freq : {'D', 'W', 'M', 'Y'}
        The frequency at which the portfolio data is evaluated.
    holdings : pd.DataFrame
        A DataFrame representing the portfolio's holdings over time.
    hpr : pd.Series
        The returns of the portfolio, adjusted for cashflows.
    invested : float
        The total amount of money invested in the portfolio.
    nav : pd.Series
        A Series representing the total net asset value (NAV) of the portfolio
        over time.
    nav_breakdown : pd.DataFrame
        A DataFrame representing the portfolio's assets' value over time.
    prices : pd.DataFrame
        A DataFrame representing the prices of the assets in the portfolio
        over time.
    tickers : list
        A list of tickers representing the assets in the portfolio.
    weights : pd.DataFrame
        The weights in percentage terms of the portfolio's holdings
        over time.
    value : float
        The final value of the portfolio (NAV at the last date).
    sdate : datetime
        The start date of the portfolio's return data.
    """

    def __init__(self, holdings_obj: Holdings,
                 prices: pd.DataFrame,
                 freq: str):
        """
        Initializes the Portfolio class with holdings data, prices, and
        frequency.

        Parameters
        ----------
        holdings_obj : Holdings
            An instance of the Holdings class, containing the portfolio's
            holdings data.
        prices : pd.DataFrame
            A DataFrame containing the historical prices for the assets in the
            portfolio.
        freq : {'D', 'W', 'M', 'Y'}
            The frequency at which the portfolio data is recorded
        """
        # Value checks
        assert all(holdings_obj.holdings.columns == prices.columns)

        # Holdings and prices should be indexed identically
        common_dates = holdings_obj.holdings.index\
            .intersection(prices.index)
        self.holdings = holdings_obj.holdings.loc[common_dates]
        self.prices = prices.loc[common_dates]

        # Directly inherited traits from Holdings instance
        self.cashflows = holdings_obj.cashflows
        self.cash_holdings = holdings_obj.cash_holdings
        self.invested = holdings_obj.invested
        self.tickers = holdings_obj.tickers

        # Calculate NAV
        if self.cash_holdings.any():
            temp = self.holdings * self.prices
            self.nav_breakdown = temp.merge(self.cash_holdings, how='inner',
                                            left_index=True, right_index=True)
        else:
            self.nav_breakdown = self.holdings * self.prices

        self.nav = self.nav_breakdown.sum(axis=1)

        # Add a value attr for quick check
        self.value = self.nav.iloc[-1]

        # Attempt frequency check
        infer_freq = pd.infer_freq(self.nav.index)
        if infer_freq:
            if infer_freq.split('-')[0] != freq:
                print('WARNING! Check frequency. '
                      f'Detected freq: "{infer_freq}", '
                      f'Used freq: "{freq}".')

        # Asset (parent class) attributes
        self.freq = freq
        self.hpr = self._hpr()
        self.sdate, self.edate = self.hpr.index[0], self.hpr.index[1]

        # Add a weights attribute
        self.weights = self.nav_breakdown.div(
            self.nav_breakdown.sum(axis=1), axis=0)

    def _freq(self):
        """Returns the numeric frequency."""
        freq_values = {
            'D': int(252),
            'W': int(52),
            'M': int(12),
            'Y': int(1),
        }

        if freq_values.get(self.freq):
            return freq_values.get(self.freq)
        else:
            raise ValueError('INVALID FREQUENCY')

    def _hpr(self):
        """Calculates the holding period returns of the portfolio."""
        # Get break dates
        df = pd.concat([self.nav, self.cashflows], axis=1, join='outer')\
            .reset_index()  # Merge series and reset index to numerical
        df.columns = ['date', 'nav', 'cf']  # Rename cols
        df['adj_cf'] = 0.0  # Create new col with def vals = 0

        # Iterate through indexes and values of valid cashflows
        # IMPORTANT: df is numerically indexed
        for i, val in df.loc[df['cf'].notna(), 'cf'].items():
            j = i - 1  # We look at previous indexes
            while j >= 0:
                if not pd.isna(df.at[j, 'nav']):  # Check if valid NAV
                    df.at[i-1, 'adj_cf'] += val  # If yes, incr adj_cf
                    break
                j -= 1  # If invalid, increment counter downwards

        # Drop subset to get back to only valid NAV and set index
        df.dropna(subset=['nav'], inplace=True)
        df.set_index('date', drop=True, inplace=True)

        # Calculate theta
        df['theta'] = (df['nav'] + df['adj_cf']) / df['nav']

        # Reverse cumprod of thetas
        df['adj_factor'] = df['theta'].iloc[::-1].cumprod().iloc[::-1]

        adj_nav = df['nav'] * df['adj_factor']  # NAV reinvestments-adj
        hpr = adj_nav.pct_change()  # Get returns
        hpr.name = 'HPR'  # Name series for clarity

        return hpr.iloc[1:]
