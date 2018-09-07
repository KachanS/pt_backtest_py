import datetime
from multiprocessing.pool import Pool
from time import time

import pytz

from config import FileConfig
from models.Ema import Ema
from services.DbHelper import DbHelper
from services.Rate import Rate

ONE_MINUTE = 60

VALUE_LIST = range(2, 41)

PERIODS = [30, 60, 120, 240, 1440]

CREATE_TABLE_SQL_TPL = "CREATE TABLE IF NOT EXISTS `{}` ( \
                                `ts` int(11) NOT NULL, \
                                `source` decimal(12,4) NOT NULL, \
                                `value` decimal(12,4) NOT NULL, \
                                PRIMARY KEY (`ts`) \
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;"


FETCH_LAST_SQL = 'SELECT source, value, ts FROM {} WHERE ts < {} AND MOD(ts + 60, {}) = 0 ORDER BY ts DESC LIMIT 1'


def processor(window: int, period, raw_rates: dict):
    db = DbHelper(0, 0, 0, 0)

    table_name = f'ema_{window}_{period}'
    # Create table
    db.execute(CREATE_TABLE_SQL_TPL.format(table_name), commit=True)
    # Get start point
    last_ts, = db.execute(f'SELECT MAX(`ts`) FROM `{table_name}`')[0]
    if last_ts is None:
        last_ts = 0
    last_ts = int(last_ts)

    rates = [(int(_ts), float(_price)) for _ts, _price in raw_rates.items() if int(_ts) > last_ts]
    rates.sort(key=lambda i: i[0])

    min_init_ts = min(map(int, raw_rates.keys())) + window * period * ONE_MINUTE

    cache = dict()

    initialized = False
    for _item in rates:
        cts, price = _item

        pts = cts - cts % (period * ONE_MINUTE)

        if cts > min_init_ts:
            ema = None
            if not initialized or pts not in cache:
                ema_data = db.execute(FETCH_LAST_SQL.format(table_name, pts+1, period*ONE_MINUTE))
                if len(ema_data):
                    ema = Ema(window, float(ema_data[0][0]), float(ema_data[0][1]))
                if initialized:
                    cache[pts] = ema

            if ema is not None:
                new = ema.calculate(price)
            else:
                sma_src = [x[1] for x in rates if x[0] < cts and x[0] % (period * ONE_MINUTE) == 0]
                new = Ema(window, price, sum(sma_src[-window:])/window)
                initialized = True

            if new is not None:
                db.execute(
                    f'INSERT INTO {table_name} VALUES(%s, %s, %s)',
                    params=(cts, new.source, new.value),
                    commit=True
                )
    del db
    del rates


def generator(rates_by_period: dict):
    for period, rates in rates_by_period.items():
        for window in VALUE_LIST:
            yield window, period, rates

def pretty_ts(ts):
    return datetime.datetime.fromtimestamp(int(ts), tz=pytz.UTC).strftime('%Y-%m-%d %T')


if __name__ == '__main__':
    config = FileConfig()

    start = config.get('APP.START_FROM', 0, int)

    end = int(time())
    end -= end % (ONE_MINUTE * 60 * 24)

    combined = dict()

    minute_prices = Rate().fetch_close(start, end)
    for c_period in PERIODS:
        combined[c_period] = minute_prices

    process_count = config.get('APP.POOL_PROCESSES', 4, int)
    max_tasks = config.get('APP.POOL_TASK_PER_CHILD', 10, int)

    use_pool = config.get('APP.USE_POOL', True, bool)

    start_at = time()

    if use_pool:
        pool = Pool(processes=process_count, maxtasksperchild=max_tasks)
        pool.starmap(processor, generator(combined))
        pool.close()
        pool.join()
    else:
        for item in generator(combined):
            processor(*item)

    print(f'Done in {time() - start_at} s. Total: {sum(map(lambda x:len(x), combined))}')
    print(f'Combinations: {len(list(generator(combined)))}')
