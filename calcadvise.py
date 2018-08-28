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
    for fast in range(2, 31):
        for signal in range(2, 21):
            for slow in range(10, 41):
                if fast + 2 <= slow:
                    yield fast, slow, signal, period, rates

def pool_processor(fast, slow, signal, period, raw_rates):
    rates = [(_ts, _price) for _ts, _price in raw_rates.items()]
    db = DbHelper(fast, slow, signal, period)
    max_ts = db.get_max_ts()

    min_for_init = MacD(fast, slow, signal).get_min_ts_to_init(int(rates[0][0]), period)

    last_cache = dict()

    for item in rates:
        cts, price = item
        cts = int(cts)
        pts = cts - cts%(period*60)
        if pts == cts:
            pts -= (period*60)
        price = float(price)
        initialized = False
        if cts > max_ts and cts > min_for_init:
            if not initialized or pts not in last_cache:
                last_cache[pts] = db.fetch_last(pts + 1)

            last = last_cache[pts]

            if last is not None:
                new = MacD(fast, slow, signal).calculateNext(cts, price, last)
            else:
                new = MacD(fast, slow, signal).init(cts, period, rates)
                initialized = True

            if new is not None:
                db.save(new)
    del db
    del rates

def combine_rate(minute_rates, period_rates):
    result = dict(minute_rates)
    for ts, price in period_rates.items():
        result[ts] = price
    return result

if __name__ == '__main__':
    end = 1506902400
    end = 1514764800
    end = time()

    config = FileConfig()

    period = config.get('APP.PERIOD', 30, int)

    minute_prices = Rate().fetch_close(0, end)
    prices = Rate().fetch_close(0, end, period)

    combined_prices = combine_rate(minute_prices, prices)

    process_count = config.get('APP.POOL_PROCESSES', 4, int)
    max_tasks = config.get('APP.POOL_TASK_PER_CHILD', 10, int)
    start_at = time()

    #for x in pool_data_gen(period, combined_prices):
    #    fast, slow, signal, period, rates = x
    #    pool_processor(fast, slow, signal, period, rates)
    pool = Pool(processes=process_count, maxtasksperchild=max_tasks)
    pool.starmap(pool_processor, pool_data_gen(period, combined_prices))
    pool.close()
    pool.join()
    print(f'Done in {time() - start_at} s. Total: {len(list(combined_prices))}')
    #print(f'Done')



