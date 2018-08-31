# Calculate emas
from multiprocessing.pool import Pool
from time import time

from config import FileConfig
from models.Ema import Ema
from services.DbHelper import DbHelper
from services.Rate import Rate

ONE_MINUTE = 60

VALUE_LIST = [12, 26]#range(2, 41)

CREATE_TABLE_SQL_TPL = "CREATE TABLE IF NOT EXISTS `{}` ( \
                                `ts` int(11) NOT NULL, \
                                `source` decimal(12,4) NOT NULL, \
                                `value` decimal(12,4) NOT NULL, \
                                PRIMARY KEY (`ts`) \
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;"

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

    for item in rates:
        cts, price = item

        pts = cts - cts % (period * ONE_MINUTE)
        pts_match = pts == cts
        if pts_match:
            pts -= (period * ONE_MINUTE)

        initialized = False
        if cts > min_init_ts:
            ema = None
            if not initialized or pts not in cache:
                ema_data = db.execute(f'SELECT source, value FROM {table_name} WHERE ts < {pts+1} AND MOD(ts, {period*60}) = 0 ORDER BY ts DESC LIMIT 1')
                if len(ema_data):
                    ema = Ema(window, float(ema_data[0][0]), float(ema_data[0][1]))

                if initialized:
                    cache[pts] = ema
            #print(f'CTS: {cts}; PTS: {pts}; D: {cts % (period * ONE_MINUTE)}')
            if ema is not None:
                new = ema.calculate(price)
            else:
                sma_src = [x[1] for x in rates if x[0] < cts and x[0] % (period * ONE_MINUTE) == 0]
                #print(sma_src)
                #print(f'SMA: {sum(sma_src[-window:])/window}');
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

def generator(period: int, rates: dict):
    for window in VALUE_LIST:
        yield window, period, rates

if __name__ == '__main__':
    config = FileConfig()

    start = config.get('APP.START_FROM', 0, int)
    period = config.get('APP.PERIOD', 60, int)

    end = time()

    minute_prices = Rate().fetch_close(start, end)
    prices = Rate().fetch_close(start, end, period)

    combined = Rate.combine(minute_prices, prices)

    process_count = config.get('APP.POOL_PROCESSES', 4, int)
    max_tasks = config.get('APP.POOL_TASK_PER_CHILD', 10, int)

    use_pool = config.get('APP.USE_POOL', True, bool)

    start_at = time()

    if use_pool:
        pool = Pool(processes=process_count, maxtasksperchild=max_tasks)
        pool.starmap(processor, generator(period, combined))
        pool.close()
        pool.join()
    else:
        for x in generator(period, combined):
            processor(*x)

    print(f'Done in {time() - start_at} s. Total: {len(list(combined))}')
    print(f'Combinations: {len(list(generator(period, combined)))}')
    # print(f'Done')