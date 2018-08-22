import argparse
from _datetime import datetime
from multiprocessing.pool import Pool
from time import time

import pytz as pytz

from config import FileConfig
from models.Advise import Advise
from services.DbHelper import DbHelper
from services.Macd import MacD
from services.Rate import Rate

def pretty_date(timestamp: int):
    return datetime.fromtimestamp(timestamp, tz=pytz.UTC).strftime('%F %T')

def pool_data_gen(period, rates):
    for slow in range(10, 41):
        for fast in range(2, 31):
            for signal in range(2,21):
                if fast + 2 <= slow:
                    yield fast, slow, signal, period, dict(rates)

def pool_processor(fast, slow, signal, period, rates):
    db = DbHelper(fast, slow, signal, period)
    max_ts = db.get_max_ts()
    for cts, price in rates.items():
        cts = int(cts)
        price = float(price)

        if cts > max_ts:
            new = Advise(ts=cts, close=price)
            last = db.fetch_last(cts)
            if last is not None:
                new = MacD(fast, slow, signal).calculateNext(cts, price, last)
            db.save(new)
    del db
    del rates

def combine_rate(minute_rates, period_rates):
    result = dict(minute_rates)
    for ts, price in period_rates.items():
        result[ts] = price
    return result

if __name__ == '__main__':
    end = 1514764800#time()

    config = FileConfig()

    period = config.get('APP.PERIOD', 30, int)

    minute_prices = Rate().fetch_close(0, end)
    prices = Rate().fetch_close(0, end, period)

    combined_prices = combine_rate(minute_prices, prices)

    process_count = config.get('APP.POOL_PROCESSES', 4, int)
    max_tasks = config.get('APP.POOL_TASK_PER_CHILD', 10, int)
    start_at = time()
    pool = Pool(processes=process_count, maxtasksperchild=max_tasks)
    pool.starmap(pool_processor, pool_data_gen(period, combined_prices))
    pool.close()
    pool.join()
    print(f'Done in {time() - start_at} s. Total: {len(list(combined_prices))}')
    #print(f'Done')



