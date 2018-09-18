from mysql.connector import connect, ProgrammingError

from config import FileConfig


class Db:

    def __init__(self, section_name: str = 'DB'):
        self.db = None

        config = FileConfig()

        try:
            self.db = connect(
                host=config.get(f'{section_name}.host'),
                user=config.get(f'{section_name}.user'),
                passwd=config.get(f'{section_name}.pass'),
                database=config.get(f'{section_name}.name')
            )
        except ProgrammingError as e:
            print(f'[DB] [ERROR] {e.msg}')
            raise e

    def fetch(self, sql_string: str, params: tuple = None):
        cursor = self.db.cursor()
        cursor.execute(sql_string, params)
        return cursor.fetchall()

    def fetchone(self, sql_string: str, params: tuple = None):
        cursor = self.db.cursor()
        cursor.execute(sql_string, params)
        return cursor.fetchone()

    def execute(self, sql_string: str, params: tuple = None):
        cursor = self.db.cursor()
        cursor.execute(sql_string, params)
        self.db.commit()
