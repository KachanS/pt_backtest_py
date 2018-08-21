import argparse
from _datetime import datetime
from time import time

import pytz as pytz

from models.Advise import Advise
from services.DbHelper import DbHelper
from services.Macd import MacD
from services.Rate import Rate

def pretty_date(timestamp: int):
    return datetime.fromtimestamp(timestamp, tz=pytz.UTC).strftime('%F %T')

def params_generator():
    for period in [30, 60, 120, 240, 1440]:
        for slow in range(10, 41):
            for fast in range(2, 31):
                for signal in range(2,21):
                    if fast + 2 <= slow:
                        yield fast, slow, signal, period

def combine_rate(minute_rates, period_rates):
    result = dict(minute_rates)
    for ts, price in period_rates.items():
        result[ts] = price
    return result

if __name__ == '__main__':
    ts = time()

    end = time()

    rates = Rate().fetch_close(0, end)

    period_rates = dict()
    counter = 0
    counter_2 = 0
    for params in params_generator():
        counter += 1
        fast, slow, signal, period = params
        db = DbHelper(*params)

        p_rates = None
        if period in period_rates:
            p_rates = period_rates[period]
        else:
            p_rates = Rate().fetch_close(0, end, period)
            period_rates[period] = period_rates

        max_ts = db.get_max_ts()

        res_rates = combine_rate(rates, p_rates)
        counter_2 = 0
        ts2 = time()
        for ts, close in res_rates.items():
            counter_2 += 1
            ts = int(ts)
            close = float(close)

            if ts > max_ts:
                new = Advise(ts=ts, close=close)
                last = db.fetch_last(ts)
                if last is not None:
                    new = MacD(fast, slow, signal).calculateNext(ts, close, last)
                db.save(new)
            else:
                print(f'Skipped for {fast}, {slow}, {signal}, {period} ...')

            if counter_2%1000 == 0:
                print(f'{counter_2} rates processed')
        if counter%10 == 0:
            print(f'{counter} combinations processed')
        print(f'Spent {time() - ts2} seconds')
        del db
        del res_rates
    exit('Done');

    print(f'There are {len(list(rates))}. Spent {time() - ts} seconds');
    exit()


