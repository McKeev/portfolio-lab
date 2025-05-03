import pandas as pd
from .holdings import Holdings


class Portfolio:
    """
    A class to represent a portfolio consisting of holdings and their
    corresponding prices.

    Attributes
    ----------
    holdings : pd.DataFrame
        A DataFrame representing the portfolio's holdings over time.
    prices : pd.DataFrame
        A DataFrame representing the prices of the assets in the portfolio
        over time.
    cashflows : pd.DataFrame
        A DataFrame containing the cashflows associated with the portfolio.
    cash_holdings : pd.Series
        A Series representing the cash holdings in the portfolio.
    invested : float
        The total amount of money invested in the portfolio.
    tickers : list
        A list of tickers representing the assets in the portfolio.
    nav_breakdown : pd.DataFrame
        A DataFrame representing the portfolio's assets' value over time.
    nav : pd.Series
        A Series representing the total net asset value (NAV) of the portfolio
        over time.
    sdate : datetime
        The start date of the portfolio data.
    edate : datetime
        The end date of the portfolio data.
    value : float
        The final value of the portfolio (NAV at the last date).
    freq : {'D', 'W', 'M', 'Y'}
        The frequency at which the portfolio data is evaluated.
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

        # Calculating NAV
        if self.cash_holdings.any():
            temp = self.holdings * self.prices
            self.nav_breakdown = temp.merge(self.cash_holdings, how='inner',
                                            left_index=True, right_index=True)
        else:
            self.nav_breakdown = self.holdings * self.prices

        self.nav = self.nav_breakdown.sum(axis=1)

        # Attributes from NAV
        self.sdate, self.edate = self.nav.index[0], self.nav.index[1]
        self.value = self.nav.iloc[-1]

        # Attempt frequency check
        infer_freq = pd.infer_freq(self.nav.index)
        if infer_freq:
            if infer_freq.split('-')[0] != freq:
                print('WARNING! Check frequency. '
                      f'Detected freq: "{infer_freq}", '
                      f'Used freq: "{freq}".')
        self.freq = freq

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
