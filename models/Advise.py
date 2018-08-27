class Advise:

    BUY = 1
    SELL = 2
    HOLD = 3

    def __init__(self, ts: int, close: int, fast: float = 0.0, slow: float = 0.0, signal: float = 0.0, hist: float = 0.0, advise: int = 0, state: int = 0):
        self.ts = ts
        self.close = float(close)
        self.fast = float(fast)
        self.slow = float(slow)
        self.signal = float(signal)
        self.hist = float(hist)
        self.advise = advise
        self.state = state
        self.key = '--KEY--'

    def setKey(self, key: str):
        self.key = key
        return self

    def decompose(self):
        return self.key.split('_');

    def as_str(self):
        return f'[{self.key}] {self.close}@{self.ts} => {self.hist} => {self.advise}'

    def as_tuple(self):
        return (self.ts, self.close, self.fast, self.slow, self.signal, self.hist, self.advise, self.state)