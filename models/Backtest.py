from config import FileConfig
from json import dumps
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, Text, String, create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

config = FileConfig()

Base = declarative_base()

class Backtest(Base):

    __tablename__ = 'main_backtest'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)

    active = Column(Integer)

    buy_fast = Column(Integer)
    buy_slow = Column(Integer)
    buy_signal = Column(Integer)
    buy_period = Column(Integer)

    sell_fast = Column(Integer)
    sell_slow = Column(Integer)
    sell_signal = Column(Integer)
    sell_period = Column(Integer)

    ts_start = Column(Integer)
    ts_end = Column(Integer)

    status = Column(Integer)
    data = Column(Text)
    extend = Column(Text)
    name = Column(String)

    total_month6 = Column(Float)
    total_month3 = Column(Float)
    total_month1 = Column(Float)
    total_week = Column(Float)

    type = Column(Integer)
    is_rt = Column(Integer)

    TYPE_LONG = 1;
    TYPE_SHORT = 2;

    CON_STRING = ''

    def __repr__(self):
        return f'Total. #{self.id} W: {self.total_week}, M1: {self.total_month1}, M2: {self.total_month3}, M3: {self.total_month6}'

    @staticmethod
    def __get_session():
        return sessionmaker(bind=create_engine(Backtest.CON_STRING, pool_recycle=60))()

    @staticmethod
    def find_by_params(in_params: dict, out_params: dict, type: int, is_rt: int = 1):
        session = Backtest.__get_session()
        filters = [
            Backtest.buy_fast == in_params[0],
            Backtest.buy_slow == in_params[1],
            Backtest.buy_signal == in_params[2],
            Backtest.buy_period == in_params[3],
            Backtest.sell_fast == out_params[0],
            Backtest.sell_slow == out_params[1],
            Backtest.sell_signal == out_params[2],
            Backtest.sell_period == out_params[3],
            Backtest.is_rt == is_rt,
            Backtest.type == type
        ]

        bt = session.query(Backtest).filter(*filters).first()
        session.close()
        return bt

    @staticmethod
    def create(in_params: tuple, out_params: tuple, _type: int, start: int, end: int, statistics: dict):
        session = Backtest.__get_session()

        in_name = f'{in_params[0]}_{in_params[1]}_{in_params[2]}_{in_params[3]}'
        out_name = f'{out_params[0]}_{out_params[1]}_{out_params[2]}_{out_params[3]}'
        type_name = "Short" if _type == Backtest.TYPE_SHORT else "Long"

        _backtest = Backtest(buy_fast=in_params[0],
                             buy_slow=in_params[1],
                             buy_signal=in_params[2],
                             buy_period=in_params[3],
                             sell_fast=out_params[0],
                             sell_slow=out_params[1],
                             sell_signal=out_params[2],
                             sell_period=out_params[3],
                             status=3,
                             data=f'{dumps(dict(statistics=statistics))}',
                             extend='|main.backtest|',
                             name=f'{type_name}: In {in_name} -> Out {out_name}',
                             total_month6=float(statistics['4']['total'] - statistics['4']['fees']),
                             total_month3=float(statistics['3']['total'] - statistics['3']['fees']),
                             total_month1=float(statistics['2']['total'] - statistics['2']['fees']),
                             total_week=float(statistics['1']['total'] - statistics['1']['fees']),
                             ts_start=start,
                             ts_end=end,
                             is_rt=1,
                             type=_type,
                             active=1
                             )
        session.add(_backtest)
        session.commit()
        session.close()

    @staticmethod
    def do_update(bid: int, start: int, end: int, statistics: dict):
        session = Backtest.__get_session()
        session.query(Backtest).filter(Backtest.id == bid).update({
            'ts_start': start,
            'ts_end': end,
            'data': f'{dumps(dict(statistics=statistics))}',
            'total_month6': float(statistics['4']['total'] - statistics['4']['fees']),
            'total_month3': float(statistics['3']['total'] - statistics['3']['fees']),
            'total_month1': float(statistics['2']['total'] - statistics['2']['fees']),
            'total_week': float(statistics['1']['total'] - statistics['1']['fees'])
        })

        session.commit()
        session.close()

    @staticmethod
    def is_calculated(p_in, p_out, start, end):
        SQL = f'SELECT COUNT(`id`) FROM `main_backtest` ' \
              f'WHERE `buy_fast` = {p_in[0]} AND `buy_slow` = {p_in[1]} ' \
              f'AND `buy_signal` = {p_in[2]} AND `buy_period` = {p_in[3]} ' \
              f'AND `sell_fast` = {p_out[0]} AND `sell_slow` = {p_out[1]} ' \
              f'AND `sell_signal` = {p_out[2]} AND `sell_period` = {p_out[3]} ' \
              f'AND `ts_end` = {end} ' \
              f'AND `active` > 0'

        session = Backtest.__get_session()
        c,  = session.execute(SQL).fetchone()
        session.close()
        return c >= 2


Backtest.CON_STRING = config.get('APP.BT_CONNECTION_STRING', '')