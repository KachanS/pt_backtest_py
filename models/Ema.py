class Ema:

    def __init__(self, window: int, source: float, value: float):
        self.window = window
        self.source = source
        self.value = value

        self.__a = 2/(window + 1)

    def calculate(self, source: float):
        #print(f'{self.__a}*{source} + (1 - {self.__a})*{self.value} = {self.__a*source + (1 - self.__a)*self.value}')
        return Ema(
            self.window,
            source=source,
            value=self.__a*source + (1 - self.__a)*self.value
        )