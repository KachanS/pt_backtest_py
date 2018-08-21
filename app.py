import argparse
from datetime import datetime
from time import time

import numpy as np

from services.Rate import Rate

ADVISE_BUY = 'Buy'
ADVISE_SELL = 'Sell'
ADVISE_HOLD = 'Hold';

MACD_CACHE = dict()

def calculateFrame(rates: [(int, float)], rate: (int, float), period: int):
    current_ts, close = rate
    current_period_ts = current_ts - current_ts % (60 * period)
    next_period_ts = current_period_ts + 60 * period

    frame = list(filter(lambda r: r[0] < next_period_ts, rates))

    if current_ts != current_period_ts:
        frame.append((next_period_ts, close))

    return frame

def calculateAdvise(frame: [(int, float)], fast: int, slow: int, signal: int, period: int):
    min_frame_size = max(fast, slow) + signal

    if len(frame) <= min_frame_size:
        return None
    else:
        prev_macd_ts = frame[-2][0]
        cache_id = ':'.join([fast, slow, signal, period, prev_macd_ts])
        prev_macd = MACD_CACHE[cache_id] if cache_id in MACD_CACHE else None

        if prev_macd is None:
            macd = calculateMacd(frame, fast, slow, signal)

def calculateMacd(frame: [(int, float)], fast: int, slow: int, signal: int) -> [(int, float)]:
    data = np.asarray([(x[0], x[1], 0, 0, 0, 0) for x in frame], dtype=np.dtype('int,float,float,float,float,int'))

    data['f2'] = __ema_v(data['f1'], fast)
    data['f3'] = __ema_v(data['f1'], slow)
    # Overwrite f1 with MACD
    data['f1'] = data['f2'] - data['f1']
    # Overwrite f2 with MACDS
    data['f2'] = __ema_v(data['f1'], signal)
    # Overwrite f3 with MACDH
    data['f3'] = data['f1'] - data['f2']

    return [(r[0], r[3]) for r in data.tolist()]


# Vectorized version for calculation of EMA
def __ema_v(data, window):
    alpha = 2 /(window + 1.0)
    alpha_rev = 1-alpha
    n = data.shape[0]

    pows = alpha_rev**(np.arange(n+1))

    scale_arr = 1/pows[:-1]
    offset = data[0]*pows[1:]
    pw0 = alpha*alpha_rev**(n-1)

    mult = data*pw0*scale_arr
    cum_sums = mult.cumsum()
    out = offset + cum_sums*scale_arr[::-1]
    return out

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='RealTime backtest for plato')

    parser.add_argument('--enter', help='Combinations fast, slow, signal, period. Ex: 12_26_9_30')
    parser.add_argument('--exit', help='Combinations fast, slow, signal, period. Ex 12_26_9_30')

    args = parser.parse_args()

    end = int(time())
    end = 1534204800  # 2018-08-14 00:00:00

    go_back_days = 30
    start = end - 60*60*24*go_back_days

    if args.enter is None:
        raise ValueError('Invalid enter params')
    if args.exit is None:
        raise ValueError('Invalid exit params')

    fast_in, slow_in, signal_in, period_in = map(int, args.enter.split('_'))
    fast_out, slow_out, signal_out, period_out = map(int, args.exit.split('_'))
    print('Enter', fast_in, slow_in, signal_in, period_in)
    print('Exit', fast_out, slow_out, signal_out, period_out)
    print('Start AT ', start)
    print('End AT ', end)
    print(f'Go back for {go_back_days} days')
    # Calculate minimum offset to calculate MACD
    enter_skip_tick = (max(fast_in, slow_in) + signal_in) * period_in * 60
    exit_skip_tick = (max(fast_out, slow_out) + signal_out) * period_out * 60
    # Select maximum offset to guaranty valid MACD value for start point
    rates_start_offset = max(enter_skip_tick, exit_skip_tick)

    rates = Rate.fetch_close(start - rates_start_offset, end)
    print(f'There are {len(list(rates))} rates found. Offset is: {rates_start_offset/60} minute ticks')
    rates_list = [(int(ts), float(close)) for ts, close in rates.items()]

    period_candles_in = list(filter(lambda r:  r[0] % (period_in*60) == 0, rates_list))
    period_candles_out = list(filter(lambda r:  r[0] % (period_out*60) == 0, rates_list))

    i = 0
    prev_length = 0
    ts = time()
    for rate in rates_list:
        # Try to calculate MACD advise
        # Calculate frame based on current TS and period candles
        frame_in = calculateFrame(period_candles_in, rate, period_in)
        advise_in = calculateAdvise(frame_in, fast_in, slow_in, signal_in, period_in)
        frame_out = calculateFrame(period_candles_out, rate, period_out)
        advise_out = calculateAdvise(frame_out, fast_out, slow_out, signal_out, period_out)
        #print(rate[0], advise)
    print(f'Total calc: {time() - ts}')
    exit(0)
