import pandas as pd
import numpy as np
import datetime as dt
import os


class MappingError(Exception):
    pass


class Portfolio():
    """
    A portfolio object containing information about holdings, market value and
    returns.

    Attributes
    ----------
    holdings: pandas.DataFrame
        A Dataframe containing historical holdings (in units) of the portfolio
    cashflows : pandas.Series
        Cashflows in/out of the portfolio, indexed by date.
    invested : float
        Capital invested (net) in the portfolio

    """
    _TICKER_MAPPER = pd.read_csv(
        os.path.join(os.path.dirname(__file__), 'ticker_mapper.csv'),
        index_col=0
    )['LSEG'].to_dict()

    def __init__(self, holdings: pd.DataFrame, cashflows: pd.Series):
        """
        Initialize a Portfolio instance by loading a historical holdings
        dataframe.

        Parameters
        ----------
        holdings : pandas.DataFrame
            A Dataframe containing historical holdings (in units) of the
            portfolio
        cashflows : pandas.Series
            Cashflows in/out of the portfolio, indexed by date.
        """
        # Bring holdings to current day
        full_range = pd.date_range(start=holdings.index.min(),
                                   end=(pd.Timestamp.today() - dt.timedelta(1))
                                   .normalize(), freq='D')

        # Initiate attributes
        self.holdings = holdings.reindex(full_range).ffill()
        _holds = self.holdings.iloc[-1]
        self._holds = _holds[_holds != 0]  # Internal use, non-0 curr holdings
        self.cashflows = cashflows
        self.invested = float(cashflows.sum())

    @classmethod
    def from_etoro(cls, acc_activity: pd.DataFrame):
        """
        Initialize a Portfolio instance by loading an etoro 'Account Activity'
        dataframe.

        Parameters
        ----------
        account_activity : pandas.DataFrame
            A Dataframe containing historical holdings (in units) of the
            portfolio.
        """

        # Convert dates to DateTime
        acc_activity["Date"] = pd.to_datetime(acc_activity["Date"],
                                              dayfirst=True)

        # Set dates as index
        acc_activity.set_index("Date", inplace=True)

        # Clean and convert a range of columns to float
        columns_to_convert = acc_activity.loc[:, 'Amount':'Balance'].columns

        # Remove commas and strip spaces, then convert to float
        acc_activity[columns_to_convert] = acc_activity[columns_to_convert]\
            .replace(
                {',': '', ' ': ''}, regex=True
            ).apply(pd.to_numeric, errors='coerce')

        # Track cashflows
        cashflows = acc_activity.loc[
            acc_activity['Type'] == 'Deposit', 'Amount'
        ].copy()
        cashflows.name = 'Cashflows'
        cashflows.index = cashflows.index.normalize()

        # Make a cash balance series (will be appended back to holdings later)
        cash_balance = acc_activity['Balance'].resample('D').last().ffill()

        # Create a trades df subset of acc_activity
        transactions = ['Open Position', 'Position closed']
        trades = acc_activity.loc[
            acc_activity['Type'].isin(transactions)].copy()

        # Get correct tickers
        trades['Asset'] = Portfolio._convert_etoro_tickers(trades['Details'])

        # Make buys positive and sells negative
        trades['Change'] = np.where(trades['Type'] == 'Open Position',
                                    trades['Units / Contracts'],
                                    - trades['Units / Contracts']
                                    )

        # Drop unnecessary columns
        trades = trades[['Asset', 'Change']]

        # Aggregate trades of same positions at same time
        trades = trades.groupby([trades.index, 'Asset'])['Change']\
            .sum().reset_index()

        # Pivot to have assets as columns
        trades = trades.pivot(index='Date', columns='Asset', values='Change')
        trades = trades.resample('D').sum()  # Resample to total EoD changes

        # Adjust for stock splits
        trades = Portfolio._adjust_splits(trades=trades,
                                          acc_activity=acc_activity)

        # Build final holdings df
        holdings = trades.cumsum()  # Cumulative sum to get holdings by date
        holdings = holdings.merge(cash_balance, how='outer',
                                  left_index=True, right_index=True)\
            .ffill().fillna(0)  # Add cash balance and frontfill

        # Rename balance col
        holdings = holdings.rename({'Balance': 'USD'}, axis=1)
        holdings[abs(holdings) < 1e-10] = 0  # Make small numbers 0

        return cls(holdings=holdings, cashflows=cashflows)

    @staticmethod
    def _convert_etoro_tickers(series: pd.Series):
        '''Convert mapped eToro tickers to LSEG, or returns a list of tickers
        to add to map'''

        series = series.copy()  # Mutability safeguard

        # Extract old tickers
        old_ticks = [value.split('/')[0] for value in series.values]

        # Create list of unmapped tickers
        unmapped = [i for i in old_ticks if i not in Portfolio._TICKER_MAPPER]

        # If no unmapped tickers, return mapped
        # else raise error with unmapped tickers
        if not unmapped:
            return pd.Series([Portfolio._TICKER_MAPPER[tick]
                              for tick in old_ticks], index=series.index)
        else:
            # Raise unique mapping errors
            raise MappingError(f'Unmapped tickers: {set(unmapped)}')

    @staticmethod
    def _adjust_splits(trades: pd.DataFrame, acc_activity: pd.DataFrame):
        """Augments the trades DataFrame for stock splits based on account
        activity.

        Parameters
        ----------
        trades : pd.DataFrame
            DataFrame of trade unit changes, indexed by Date and with assets
            as columns.
        acc_activity : pd.DataFrame
            Account activity log from eToro including corporate actions.

        Returns
        -------
        pd.DataFrame
            Updated trades DataFrame with split adjustments applied.
        """

        # Mutability safeguard
        trades = trades.copy()
        acc_activity = acc_activity.copy()

        # Locate stock splits
        splits = acc_activity.loc[
            acc_activity['Type'] == 'corp action: Split']\
            .reset_index().drop_duplicates(subset=['Date', 'Details']).copy()

        # Determine asset being split (looks like 'NVDA/USD 10:1' in 'Details')
        splits['Asset'] = splits['Details'].apply(lambda x: x.split(' ')[0])
        splits['Asset'] = Portfolio._convert_etoro_tickers(splits['Asset'])

        # Determine multiplier
        splits['Numerator'] = splits['Details'].apply(
            lambda x: int(x.split(' ')[1].split(':')[0]))

        splits['Denominator'] = splits['Details'].apply(
            lambda x: int(x.split(' ')[1].split(':')[1]))

        splits['Multiplier'] = splits['Numerator'] / splits['Denominator']

        # Drop unnecessary cols
        splits = splits[['Date', 'Asset', 'Multiplier']]

        # Set as only days (no hours)
        splits['Date'] = splits['Date'].dt.normalize()

        # Handle edge case where there are multiple splits in a day
        splits = splits.groupby(['Date', 'Asset'])['Multiplier']\
            .prod().reset_index()

        # Add trades for splits
        for _, split_r in splits.iterrows():

            # Get current holdings of the asset (t-1)
            held = trades.loc[:split_r['Date'], split_r['Asset']]\
                .cumsum().iloc[-1]

            # Compute adjustment and apply
            new = held * split_r['Multiplier']  # New balance should be
            adjust = new - held
            trades.loc[split_r['Date'], split_r['Asset']] += adjust

        return trades
