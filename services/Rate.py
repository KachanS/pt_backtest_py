from time import time

import requests


class Rate:

    CLOSE_RATES_URL = 'http://platotradeinfo.silencatech.com/main/candles/ajaxfetchperiod'

    DEFAULT_PAIR = 'btc_usd'

    MINUTES_IN_DAY = 24*60

    @staticmethod
    def fetch_close(start_at: int, end_at: int, period: int = 1) -> dict:
        raw = requests.get(Rate.CLOSE_RATES_URL, params={
            'from': start_at,
            'to': end_at,
            'pair': Rate.DEFAULT_PAIR,
            'period': period
        })

        response = raw.json()
        if not response['result']:
            raise Exception('Error querying rates data')

        return response['rates']


if __name__ == '__main__':
    end = time()
    end = int(end - end % (60*Rate.MINUTES_IN_DAY))

    go_back_days = 7

    start = int(end - 60*Rate.MINUTES_IN_DAY*go_back_days)
    print(f'Start: {start}, End: {end}')
    ts = time()

    data = Rate.fetch_close(start, end)
    data_len = len(list(data))
    expected_data_len = go_back_days*Rate.MINUTES_IN_DAY
    estimated_coverage = '~%.2f%%'%((data_len/Rate.MINUTES_IN_DAY)*100/go_back_days)

    print(f'Query takes ~{time() - ts} seconds. Data length: {data_len}/{expected_data_len}, {estimated_coverage}')
