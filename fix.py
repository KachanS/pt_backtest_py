from time import time

from mysql.connector import ProgrammingError

from services.DbHelper import DbHelper

FAST_LIST = range(2, 31)
SLOW_LIST = range(10, 41)
SIGNAL_LIST = range(2, 21)
PERIOD_LIST = [30, 60, 120, 240, 1440]

if __name__ == '__main__':
    db = DbHelper(0, 0, 0, 0)

    for fast in FAST_LIST:
        for signal in SIGNAL_LIST:
            table_name = f'a_{fast}_{signal}_60'
            print(f'Start {table_name} processing')
            _s = time()
            try:
                db.execute(f'UPDATE `{table_name}` SET `hist` = `macd` - `signal`', commit=True)
                db.execute(f'UPDATE `{table_name}` SET `state` = IF(`hist` > 0, 1, 2)', commit=True)
                db.execute(f'UPDATE `{table_name}` SET `advise` = `state` where `advise` <> 3', commit=True)
            except ProgrammingError as e:
                print('[ERROR]', e)
            print(f'{table_name} finished in {time() - _s}s')
    print('All done')
