from pyfinex.providers.base import DataProvider, _retry, _treat_historical
import refinitiv.data as rd
import pyfinex.utils as utils


class LSEG(DataProvider):

    def get_historical(self,
                       tickers: list,
                       sdate: str,
                       edate: str,
                       adj='adjusted',
                       freq='D'):

        freq = utils.Frequency(freq)  # Convert to freq object

        @_retry(n=self.retry_limit, wait=self.wait)  # Add retry logic
        def attempt():

            print('Attempting LSEG retrieval...')  # Give feedback to user

            if adj == 'adjusted':
                output = rd.get_history(universe=tickers,
                                        fields=['TR.CLOSEPRICE'],
                                        parameters={
                                            'SDate': sdate,
                                            'EDate': edate,
                                            'Curn': 'USD',
                                            'Frq': freq.f_lseg(),
                                            },
                                        )

            elif adj == 'unadjusted':
                output = rd.get_history(universe=tickers,
                                        fields=['TR.CLOSEPRICE(Adjusted=0)'],
                                        parameters={
                                            'SDate': sdate,
                                            'EDate': edate,
                                            'Curn': 'USD',
                                            'Frq': freq.f_lseg(),
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

            # Treat NaN values
            return (_treat_historical(output, freq=freq))
        return attempt()
