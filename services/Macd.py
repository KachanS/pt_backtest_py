from models.Advise import Advise


class MacD:

    def __init__(self, fast: int, slow: int, signal: int):
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def calculate(self, value: float, prev_fast: float, prev_slow: float, prev_signal: float):
        #print(type(value), type(self.fast), type(prev_fast));exit()
        cur_fast = MacD.__ema_by_prev(value, self.fast, prev_fast)
        cur_slow = MacD.__ema_by_prev(value, self.slow, prev_slow)
        cur_macd = cur_fast - cur_slow
        cur_signal = MacD.__ema_by_prev(cur_macd, self.signal, prev_signal)
        cur_hist = cur_macd - cur_signal
        cur_state = 1 if cur_hist > 0 else 2

        return cur_fast, cur_slow, cur_signal, cur_hist, cur_state

    def calculateNext(self, ts: int, close: float, prev: Advise) -> Advise:
        new = Advise(ts, close)
        n_fast, n_slow, n_signal, n_hist, n_state = self.calculate(new.close, prev.fast, prev.slow, prev.signal)
        new.fast = n_fast
        new.slow = n_slow
        new.signal = n_signal
        new.hist = n_hist
        new.state = n_state

        if prev.hist < 0 < new.hist:
            new.advise = 1
        elif prev.hist > 0 > new.hist:
            new.advise = 2
        else:
            new.advise = 3

        return new

    def init(self, cts: int, period: int, raw_rates: dict):
        # Main purpose is to fill first value with VALID values
        period_rates = [(int(ts), float(price)) for ts, price in raw_rates.items() if int(ts) < cts and (int(ts) % (period * 60) == 0)]
        if not self.__can_be_initialized(len(period_rates)):
            return None
        print(period, period_rates)
        print(f'There are {len(period_rates)} period rates')
        values = dict(fast=None, slow=None, signal=None)
        acc = dict(close=[], macd=[])
        i = 0
        for x in period_rates:
            cur_ts, cur_price = x
            acc['close'].append(cur_price)

            if len(acc['close']) == self.fast:
                values['fast'] = sum(acc['close']) / self.fast
            elif len(acc['close']) > self.fast:
                values['fast'] = MacD.__ema_by_prev(cur_price, self.fast, values['fast'])

            if len(acc['close']) == self.slow:
                values['slow'] = sum(acc['close']) / self.slow
            elif len(acc['close']) > self.slow:
                values['slow'] = MacD.__ema_by_prev(cur_price, self.slow, values['slow'])

            if values['fast'] is not None and values['slow'] is not None:
                acc['macd'].append(values['fast'] - values['slow'])

            if len(acc['macd']) == self.signal:
                values['signal'] = sum(acc['macd'])/self.signal
                return Advise(ts=cur_ts, close=cur_price, fast=values['fast'], slow=values['slow'], signal=values['signal'])
        return None

    def get_min_ts_to_init(self, start_ts, period):
        min = max(self.fast, self.slow) + self.signal - 2
        return period * min * 60 + start_ts


    def __can_be_initialized(self, period_rates_count: int):
        min = max(self.fast, self.slow) + self.signal - 2
        return period_rates_count > min;

    @staticmethod
    def __ema_by_prev(value: float, window: int, prev: float):
        a = 2/(window + 1)
        return a*(value - prev) + prev
