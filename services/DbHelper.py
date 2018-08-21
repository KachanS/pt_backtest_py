import mysql.connector
from mysql.connector import ProgrammingError, connect

from models.Advise import Advise


class DbHelper:

    CREATE_TABLE_SQL_TPL = "CREATE TABLE IF NOT EXISTS `{}` ( \
                                `ts` int(11) NOT NULL, \
                                `close` decimal(12,4) NOT NULL, \
                                `fast` decimal(12,4) NOT NULL, \
                                `slow` decimal(12,4) NOT NULL, \
                                `signal` decimal(12,4) NOT NULL, \
                                `hist` decimal(12,4) NOT NULL, \
                                `advise` tinyint(4) NOT NULL, \
                                `state` tinyint(4) NOT NULL, \
                                `p_fast` int(11) NOT NULL, \
                                `p_signal` int(11) NOT NULL, \
                                PRIMARY KEY (`ts`, `p_fast`, `p_signal`), INDEX(`p_fast`), INDEX(`p_signal`) \
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;"

    FETCH_PERIOD_SQL_TPL = "SELECT * FROM `{}` WHERE `ts` >= %s AND `ts` <= %s AND MOD(`ts`, %s) = 0 ORDER BY `ts` ASC"

    MINUTE = 60

    def __init__(self, fast: int, slow: int, signal: int, period: int):
        self.db = None
        try:
            self.db = connect(
                host="localhost",
                user="root",
                passwd="",
                database="advises"
            )
        except ProgrammingError as e:
            print(f'[DB] [ERROR] {e.msg}')
            raise e

        self.fast = fast
        self.slow = slow
        self.signal = signal
        self.period = period

        self.__table = '_'.join(map(str, [self.slow, self.period]))
        self.__key = '_'.join(map(str, [self.fast, self.slow, self.signal, self.period]))

    def fetch_advises(self, start: int, end: int, period: int = 1):
        self.create_table(self.__table)
        cursor = self.db.cursor();
        cursor.execute(self.FETCH_PERIOD_SQL_TPL.format(self.__table), (self.fast, self.signal, start, end, period*self.MINUTE))
        return {x[0]:Advise(*x).setKey(self.__key) for x in cursor.fetchall()}

    def fetch_last(self, ts: int) -> Advise:
        self.create_table(self.__table)
        cursor = self.db.cursor()
        cursor.execute(f'SELECT * FROM {self.__table} WHERE p_fast = %s AND p_signal = %s AND ts < %s AND MOD(ts, {self.period*60}) = 0 ORDER BY ts DESC LIMIT 1', (self.fast, self.signal, ts))
        data = cursor.fetchone()
        if data is None:
            return None
        return Advise(*data[:-2])

    def get_max_ts(self) -> int:
        self.create_table(self.__table)
        cursor = self.db.cursor()
        cursor.execute(f'SELECT MAX(`ts`) FROM {self.__table} WHERE p_fast = %s AND p_signal = %s', (self.fast, self.signal))
        max_ts_data = cursor.fetchone()
        max_ts, = max_ts_data
        return 0 if max_ts is None else int(max_ts)

    def create_table(self, table: str):
        cursor = self.db.cursor();
        cursor.execute(self.CREATE_TABLE_SQL_TPL.format(table))
        self.db.commit()

    def save(self, advise: Advise):
        cursor = self.db.cursor()
        cursor.execute(f'INSERT INTO {self.__table} VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', advise.as_tuple() + (self.fast, self.signal))
        self.db.commit()

if __name__ == '__main__':
    res = DbHelper().fetch_advises('12_26_9_60', 1527811200, 1529020800)
    print('Res', list([f'{ts}: {x.as_str()}' for ts, x in res.items()]))