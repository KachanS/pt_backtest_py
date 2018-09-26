from config import FileConfig
from itertools import product
from models.Advise import Advise
from models.Backtest import Backtest as ModelBacktest
from multiprocessing.pool import Pool
from services.Backtest import Backtest
from services.DbHelper import DbHelper
from time import time

ONE_MINUTE = 60
ONE_DAY = 24*60*ONE_MINUTE

BACKTEST_LONG = 1
BACKTEST_SHORT = 2

EMERGENCY_ACC = 8 # percentage, 0 means no aggregation
NORMILIZE_ACC = 0 # percentage, 0 means no aggregation

FAST_LIST = range(2, 31)
SLOW_LIST = range(10, 41)
SIGNAL_LIST = range(2, 21)
PERIOD_LIST = [30, 60, 120, 240, 1440]

DEBUG = True

def all_is(acc, type):
    return all([a.state == type for a in acc])

def all_is_buy(acc):
    return all_is(acc, Advise.BUY)

def all_is_sell(acc):
    return all_is(acc, Advise.SELL)

def generator(start, end):
    for f_i, s_i, si_i, p_i, f_o, s_o, si_o, p_o in \
        product(FAST_LIST, SLOW_LIST, SIGNAL_LIST, PERIOD_LIST, FAST_LIST, SLOW_LIST, SIGNAL_LIST, PERIOD_LIST):
            if f_i + 2 > s_i:
                continue
            if f_o + 2 > s_o:
                continue
            if p_i > p_o:
                continue

            if True or (f_i == f_o and s_i == s_o and si_i == si_o):
                yield (
                    start,
                    end,
                    (f_i, s_i, si_i, p_i),
                    (f_o, s_o, si_o, p_o)
                )

def processor(start, end, p_in, p_out):
    statistics = Backtest(p_in, p_out, start, end).calculate()

    for _type in [ModelBacktest.TYPE_LONG, ModelBacktest.TYPE_SHORT]:
        last = ModelBacktest.find_by_params(p_in, p_out, _type)
        if last is None:
            ModelBacktest.create(p_in, p_out, _type, start, end, statistics[_type])
        else:
            # Update last data
            ModelBacktest.do_update(last.id, start, end, statistics[_type])

def generator2(start, end):
    db_cache = dict()
    if len(list(db_cache)) > 50:
        db_cache = dict()

    for fast_in, slow_in, signal_in, period_in in product(FAST_LIST, SLOW_LIST, SIGNAL_LIST, PERIOD_LIST):
        for fast_out, slow_out, signal_out, period_out in product(FAST_LIST, SLOW_LIST, SIGNAL_LIST, PERIOD_LIST):
            if fast_in + 2 > slow_in:
                continue
            if fast_out + 2 > slow_out:
                continue
            if period_in > period_out:
                continue

            already_calculated = ModelBacktest.is_calculated(
                (fast_in, slow_in, signal_in, period_in),
                (fast_out, slow_out, signal_out, period_out),
                start,
                end
            )

            print(f'{[(fast_in, slow_in, signal_in, period_in), (fast_out, slow_out, signal_out, period_out)]} already_calculated: {already_calculated}')
            if already_calculated:
                continue

            in_k = '_'.join(map(str, [fast_in, slow_in, signal_in, period_in]))
            out_k = '_'.join(map(str, [fast_out, slow_out, signal_out, period_out]))
            if in_k not in db_cache:
                try:
                    db_cache[in_k] = DbHelper(fast_in, slow_in, signal_in, period_in)\
                                        .fetch_advises(start, end)
                except:
                    db_cache[in_k] = dict()
                    print(f'{in_k} skiped because of error')
                    continue

            if db_cache[in_k] == dict():
                print(f'No IN data. {in_k}')
                continue

            if out_k not in db_cache:
                try:
                    db_cache[out_k] = DbHelper(fast_out, slow_out, signal_out, period_out)\
                                        .fetch_advises(start, end)
                except:
                    db_cache[out_k] = dict()
                    print(f'{out_k} skiped because of error')
                    continue

            if db_cache[out_k] == dict():
                print(f'No OUT data. {out_k}')
                continue
            print(f'Yielded ....')
            yield (
                start,
                end,
                (fast_in, slow_in, signal_in, period_in),
                (fast_out, slow_out, signal_out, period_out),
                dict(db_cache[in_k]),
                dict(db_cache[out_k])
            )

def processor2(start, end, p_in, p_out):
    already_calculated = ModelBacktest\
                            .is_calculated(p_in, p_out, start, end)
    if already_calculated:
        print(f'{p_in} -> {p_out} already calculated')
        return

    _s = time()

    b = Backtest(p_in, p_out, start, end)
    b.set_emergency_percentage(EMERGENCY_ACC)
    b.set_normilize_percentage(NORMILIZE_ACC)

    statistics = b.calculate(
        DbHelper(*p_in).fetch_advises(start, end),
        DbHelper(*p_out).fetch_advises(start, end)
    )

    for _type in [ModelBacktest.TYPE_LONG, ModelBacktest.TYPE_SHORT]:
        if statistics[_type] is not None:
            last = ModelBacktest.find_by_params(p_in, p_out, _type)
            if last is None:
                ModelBacktest.create(p_in, p_out, _type, start, end, statistics[_type])
            else:
                ModelBacktest.do_update(last.id, start, end, statistics[_type])
        else:
            print(f'Can not calculate statistics for {[p_in, p_out]}')
    del b
    print(f'{[p_in, p_out]}: {time() - _s}')

if __name__ == '__main__':
    config = FileConfig()

    end = 1537514011#time()
    end -= end%ONE_DAY

    start = end - 180*ONE_DAY

    process_count = config.get('APP.POOL_PROCESSES', 4, int)
    max_tasks = config.get('APP.POOL_TASK_PER_CHILD', 10, int)
    use_pool = config.get('APP.USE_POOL', True, bool)

    start_at = time()

    if use_pool:
        pool = Pool(processes=process_count, maxtasksperchild=max_tasks)
        pool.starmap(processor2, generator(start, end))
        pool.close()
        pool.join()
    else:
        for item in generator(start, end):
            processor2(*item)

    print(f'Done in {time() - start_at}s')
    exit()
