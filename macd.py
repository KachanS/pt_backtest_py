from multiprocessing.pool import Pool
from time import time

from pandas import DataFrame, to_numeric

from config import FileConfig
from services.DbHelper import DbHelper
from services.Rate import Rate

ONE_MINUTE = 60

FAST_LIST = [12]#range(2, 31)
SLOW_LIST = [26]#range(10, 41)
SIGNAL_LIST = [9, 10, 11, 12]#range(2, 21)

CREATE_TABLE_SQL_TPL = "CREATE TABLE IF NOT EXISTS `{}` ( \
                                `ts` int(11) NOT NULL, \
                                `close` decimal(12,4) NOT NULL, \
                                `macd` decimal(12,4) NOT NULL, \
                                `signal` decimal(12,4) NOT NULL, \
                                `hist` decimal(12,4) NOT NULL, \
                                `advise` tinyint(4) NOT NULL, \
                                `state` tinyint(4) NOT NULL, \
                                `p_slow` int(11) NOT NULL, \
                                PRIMARY KEY (`ts`, `p_slow`), INDEX(`p_slow`) \
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;"

CREATE_MEMORY_TABLE_TPL = "CREATE TABLE IF NOT EXISTS `{}` ( \
                                `ts` int(11) NOT NULL, \
                                `close` decimal(12,4) NOT NULL, \
                                `macd` decimal(12,4) NOT NULL, \
                                `signal` decimal(12,4) NOT NULL, \
                                `hist` decimal(12,4) NOT NULL, \
                                `advise` tinyint(4) NOT NULL, \
                                `state` tinyint(4) NOT NULL, \
                                `p_slow` int(11) NOT NULL \
                            ) ENGINE=Memory DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;"

def generator(period):
    for slow in SLOW_LIST:
        for fast in FAST_LIST:
            for signal in SIGNAL_LIST:
                if fast + 2 <= slow:
                    yield fast, slow, signal, period

def processor(fast, slow, signal, period):
    db = DbHelper(0, 0, 0, 0)
    table_name = f'a_{fast}_{signal}_{period}'
    db.execute(CREATE_TABLE_SQL_TPL.format(table_name), commit=True)

    m_table_name = f'm_{fast}_{slow}_{signal}_{period}'
    db.execute(CREATE_MEMORY_TABLE_TPL.format(m_table_name), commit=True)

    start, = db.execute(f'SELECT MAX(ts) FROM {table_name} WHERE p_slow = {slow}')[0]
    if start is None:
        start = 0
    start = int(start)

    fs = db.execute(f'SELECT f.ts, f.source, f.value, s.value FROM `ema_{fast}_{period}` as `f` INNER JOIN `ema_{slow}_{period}` as `s` on f.ts = s.ts WHERE f.ts > {start}')
    fs.sort(key=lambda i: i[0])

    pacc = []
    for i in fs:
        _ts, _close, _fast, _slow = i
        _ts = int(_ts)
        _close = float(_close)
        _fast = float(_fast)
        _slow = float(_slow)

        macd = _fast - _slow
        # To calculate current SIGNAL we need previous {N = signal - 1} MACD values
        if len(pacc) < (signal - 1):
            # Load initiap p_acc values or prefill it with 0's
            vals = db.execute(f'SELECT `ts`, `macd`, `hist` FROM {table_name} WHERE ts < {_ts} and MOD(ts, {ONE_MINUTE * period}) = 0 and p_slow = {slow} ORDER BY ts DESC limit {signal - 1}')
            if len(vals) < (signal - 1):
                _st = _ts - _ts % (60*period)
                _st -= (signal - 2)*(60*period)
                pacc = list([(ts, 0.0, 0.0) for ts in range(_st, _ts - len(vals)*(ONE_MINUTE * period), ONE_MINUTE*60)])
                for x in vals:
                    pacc.append((int(x[0]), float(x[1]), float(x[2])))
            else:
                pacc = list([(int(i[0]), float(i[1]), float(i[2])) for i in vals])

        sma_acc = [x[1] for x in pacc[-signal+1:]]
        sma_acc.append(macd)

        _signal = sum(sma_acc)/len(sma_acc)
        _hist = _signal - macd
        _state = 1 if _hist > 0 else 2

        prev_hist = pacc[-1][2]
        if _hist > 0 and prev_hist < 0:
            _advise = 1
        elif _hist < 0 and prev_hist > 0:
            _advise = 2
        else:
            _advise = 3

        # Fill period acc
        if _ts % (ONE_MINUTE * period) == 0:
            pacc.append((_ts, macd, _hist))

        db.execute(f'INSERT INTO {m_table_name} VALUES(%s, %s, %s, %s, %s, %s, %s, %s)', params=(_ts, _close, macd, _signal, _hist, _advise, _state, slow), commit=True)
    # After all calculations is done COPY data to real table
    db.execute(f'INSERT INTO `{table_name}` SELECT * FROM `{m_table_name}`', commit=True)
    db.execute(f'DROP TABLE `{m_table_name}`', commit=True)
    print(f'Done {fast}_{slow}_{signal}_{period}')

if __name__ == '__main__':
    config = FileConfig()

    period = config.get('APP.PERIOD', 30, int)

    process_count = config.get('APP.POOL_PROCESSES', 4, int)
    max_tasks = config.get('APP.POOL_TASK_PER_CHILD', 10, int)

    use_pool = config.get('APP.USE_POOL', True, bool)

    start_at = time()

    if use_pool:
        pool = Pool(processes=process_count, maxtasksperchild=max_tasks)
        pool.starmap(processor, generator(period))
        pool.close()
        pool.join()
    else:
        for x in generator(period):
            processor(*x)

    print(f'Done in {time() - start_at} s.')
    print(f'Combinations: {len(list(generator(period)))}')
    # print(f'Done')