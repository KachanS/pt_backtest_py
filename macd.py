from multiprocessing.pool import Pool
from time import time
from config import FileConfig
from models.Advise import Advise
from services.DbHelper import DbHelper

ONE_MINUTE = 60

FAST_LIST = range(2, 31)
SLOW_LIST = range(10, 41)
SIGNAL_LIST = range(2, 21)
PERIOD_LIST = [30, 60, 120, 240, 1440]

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

FETCH_INITIAL_VALS_SQL_TPL = "SELECT `ts`, `macd`, `hist` FROM {} \
                              WHERE ts < {} and MOD(ts, {}) = 0 and p_slow = {} \
                              ORDER BY ts DESC limit {}"

FETCH_EMA_SQL_TPL = "SELECT f.ts, f.source, f.value, s.value FROM `{}` as `f` \
                    INNER JOIN `{}` as `s` on f.ts = s.ts \
                    WHERE f.ts > {}"


def generator():
    for slow in SLOW_LIST:
        for fast in FAST_LIST:
            for signal in SIGNAL_LIST:
                for period in PERIOD_LIST:
                    if fast + 2 <= slow:
                        yield fast, slow, signal, period


def processor(fast, slow, signal, period):
    db = DbHelper(0, 0, 0, 0)
    minute_period = ONE_MINUTE * period
    table_name = f'a_{fast}_{signal}_{period}'
    db.execute(CREATE_TABLE_SQL_TPL.format(table_name), commit=True)

    m_table_name = f'm_{fast}_{signal}_{period}'
    db.execute(CREATE_MEMORY_TABLE_TPL.format(m_table_name), commit=True)

    start, = db.execute(f'SELECT MAX(ts) FROM {table_name} WHERE p_slow = {slow}')[0]
    if start is None:
        start = 0
    start = int(start)

    fs = db.execute(FETCH_EMA_SQL_TPL.format(f'ema_{fast}_{period}', f'ema_{slow}_{period}', start))
    fs.sort(key=lambda _i: _i[0])

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
            vals = db.execute(FETCH_INITIAL_VALS_SQL_TPL.format(
                table_name,
                _ts,
                minute_period,
                slow,
                signal - 1))
            if len(vals) < (signal - 1):
                _st = _ts - _ts % minute_period
                _st -= (signal - 2) * minute_period
                pacc = list([(ts, 0.0, 0.0) for ts in range(_st, _ts - len(vals)*minute_period, minute_period)])
                for _x in vals:
                    pacc.append((int(_x[0]), float(_x[1]), float(_x[2])))
            else:
                pacc = list([(int(i[0]), float(i[1]), float(i[2])) for i in vals])

        sma_acc = [_x[1] for _x in pacc[-signal+1:]]
        sma_acc.append(macd)

        _signal = sum(sma_acc)/len(sma_acc)
        _hist = macd - _signal
        _state = Advise.BUY if _hist > 0 else Advise.SELL

        prev_hist = pacc[-1][2]
        if prev_hist < 0 < _hist:
            _advise = Advise.BUY
        elif _hist < 0 < prev_hist:
            _advise = Advise.SELL
        else:
            _advise = Advise.HOLD

        # Fill period acc
        if _ts % minute_period == 0:
            pacc.append((_ts, macd, _hist))

        db.execute(
            f'INSERT INTO {m_table_name} VALUES(%s, %s, %s, %s, %s, %s, %s, %s)',
            params=(_ts, _close, macd, _signal, _hist, _advise, _state, slow),
            commit=True
        )
    # Insert system flag to be sure this slow period is fully calculated
    db.execute(
        f'INSERT INTO {m_table_name} VALUES(%s, %s, %s, %s, %s, %s, %s, %s)',
        params=(0, 0, 0, 0, 0, 0, 0, slow),
        commit=True
    )

    estimated_count = 4#max(SLOW_LIST) - max(fast + 2, min(SLOW_LIST)) + 1
    real_count,  = db.execute(f'SELECT COUNT(DISTINCT(p_slow)) FROM `{m_table_name}` WHERE `ts` = 0')[0]

    # After all calculations is done COPY data to real table
    # Check if table is fully filled and copy if it is
    if real_count == estimated_count:
        db.execute(f'DELETE FROM `{table_name}` WHERE ts = 0', commit=True)
        _s = time()
        db.execute(f'INSERT INTO `{table_name}` SELECT * FROM `{m_table_name}`', commit=True)
        print(f'Copy {m_table_name} takes {time() - _s}s')
        _s = time()
        db.execute(f'DROP TABLE `{m_table_name}`', commit=True)
        print(f'Drop {m_table_name} takes {time() - _s}s')

    del db
    del fs
    del pacc

    print(f'Done {fast}_{slow}_{signal}_{period} ({m_table_name} => {table_name})')


if __name__ == '__main__':
    config = FileConfig()

    process_count = config.get('APP.POOL_PROCESSES', 4, int)
    max_tasks = config.get('APP.POOL_TASK_PER_CHILD', 10, int)

    use_pool = config.get('APP.USE_POOL', True, bool)

    start_at = time()

    if use_pool:
        pool = Pool(processes=process_count, maxtasksperchild=max_tasks)
        pool.starmap(processor, generator())
        pool.close()
        pool.join()
    else:
        for x in generator():
            processor(*x)

    print(f'Done in {time() - start_at} s.')
    print(f'Combinations: {len(list(generator()))}')
