from .base import DataProvider, _retry, _treat_historical
import refinitiv.data as rd


class LSEG(DataProvider):

    def get_historical(self,
                       tickers: list,
                       sdate: str,
                       edate: str,
                       adj='unadjusted',
                       freq='D'):

        @_retry(n=self.retry_limit, wait=self.wait)  # Add retry logic
        def attempt():

            print('Attempting LSEG retrieval...')  # Give feedback to user

            if adj == 'unadjusted':
                output = rd.get_history(universe=tickers,
                                        fields=['TR.CLOSEPRICE(Adjusted=0)'],
                                        parameters={
                                            'SDate': sdate,
                                            'EDate': edate,
                                            'Curn': 'USD',
                                            'Frq': freq,
                                            },
                                        )

            elif adj == 'adjusted':
                output = rd.get_history(universe=tickers,
                                        fields=['TR.CLOSEPRICE'],
                                        parameters={
                                            'SDate': sdate,
                                            'EDate': edate,
                                            'Curn': 'USD',
                                            'Frq': freq,
                                            },
                                        )

            elif adj not in DataProvider._ADJ_OPTIONS:
                raise ValueError('Invalid adjustment option.\n'
                                 'Possible:{DataProvider._ADJ_OPTIONS}')

            else:
                raise NotImplementedError(
                    'The adjustment type is not supported by LSEG')

            print('Retrieval successful!')  # Give feedback

            # When fetching for one stock, the col name != the ticker
            if len(tickers) == 1:
                output.columns = tickers  # Fix this to ensure compatibility

            return (_treat_historical(output, freq=freq))
        return attempt()
