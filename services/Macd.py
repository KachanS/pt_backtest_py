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

    @staticmethod
    def __ema_by_prev(value: float, window: int, prev: float):
        a = 2/(window + 1)
        return a*(value - prev) + prev
