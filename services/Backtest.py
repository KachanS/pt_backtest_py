import json
from datetime import datetime
from math import ceil
from time import time

import pytz
from dateutil.relativedelta import relativedelta
from pandas import DataFrame, to_datetime

from models.Advise import Advise
from services.DbHelper import DbHelper

ONE_MINUTE = 60

class Backtest:

    TYPE_LONG = 1
    TYPE_SHORT = 2

    MODE_NORMAL = 'n'
    MODE_EMERGENCY = 'e'

    DIR_IN = 'i'
    DIR_OUT = 'o'

    FEE = 0.002

    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    def __init__(self, p_in, p_out, start, end, amount: float = 1.0):
        self.__p_in = p_in
        self.__p_out = p_out
        self.__start = start
        self.__end = end

        self.__normalize_percentage = 50
        self.__emergency_percentage = 50

        self.__amount = amount

    def set_normilize_percentage(self, percentage: int):
        self.__normalize_percentage = percentage

    def set_emergency_percentage(self, percentage: int):
        self.__emergency_percentage = percentage

    def __calculate_length(self, percentage, period):
        l = ceil((percentage / 100) * period)
        return 1 if l < 1 else l

    def __get_norm_length(self, period):
        return self.__calculate_length(self.__normalize_percentage, period)

    def __get_emer_length(self, period):
        return self.__calculate_length(self.__emergency_percentage, period)

    def __all_is(self, acc, type):
        return all([a.state == type for a in acc])

    def __all_is_buy(self, acc):
        return self.__all_is(acc, Advise.BUY)

    def __all_is_sell(self, acc):
        return self.__all_is(acc, Advise.SELL)

    def __start_deal(self, flags, ts: int, price: float):
        _, recently_closed, open_appeared, _, _, close_disappeared, interval_has_deal = flags
        is_emer = recently_closed and close_disappeared
        if open_appeared or is_emer:
            if not interval_has_deal:
                return dict(
                    ts_enter=ts,
                    price_enter=price,
                    emergency_enter=is_emer,
                    emergency_exit=False
                )

        return None

    def __close_deal(self, deal, flags, ts: int, price: float) -> dict:
        recently_opened, _, _, close_appeared, open_disappeared, _, _ = flags
        is_emer = recently_opened and open_disappeared
        if close_appeared or is_emer:
            deal['ts_exit'] = ts
            deal['price_exit'] = price
            deal['emergency_exit'] = is_emer
            return deal

        return None

    def __calculate_statistics(self, deals_dict: dict, type: int):
        deals = DataFrame.from_dict(data=deals_dict, orient='index')
        deals['ts'] = deals['ts_exit']

        deals['fees'] = (deals['price_enter'] + deals['price_exit']) * self.FEE
        deals['delta'] = deals['price_exit'].values - deals['price_enter'].values
        deals['ts_enter'] = to_datetime(deals['ts_enter'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
        deals['ts_exit'] = to_datetime(deals['ts_exit'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

        factor = 1
        if type == Backtest.TYPE_SHORT:
            factor = -1;

        deals['delta'] = deals['delta'].values * factor

        dateTo = datetime.fromtimestamp(self.__end, tz=pytz.UTC)

        statistics = {}

        for idx, period in self.STATISTICS_PERIODS.items():
            dateLimit = (dateTo + relativedelta(**period)).timestamp()
            stGroup = deals[deals.ts >= dateLimit]
            if stGroup.empty:
                statistics[idx] = dict(
                    fees=0, profit=0, total=0, totalWins=0,
                    totalLosses=0, wins='0', losses='0', trades='0', calcs=[]
                )
                continue

            stGroup.insert(0, 'interval', str(idx))
            stGroup.insert(0, 'amount', self.__amount)

            winsGroup = stGroup[stGroup.delta > 0]
            lossesGroup = stGroup[stGroup.delta <= 0]

            fees = round(stGroup['fees'].sum(), 2)
            total = round(stGroup['delta'].sum(), 2)
            wins = round(winsGroup['delta'].sum(), 2)

            statistics[idx] = dict(
                fees=fees,
                profit=total - fees,
                total=total,
                totalWins=wins,
                totalLosses=total - wins,
                wins='%d' % winsGroup['delta'].count(),
                losses='%d' % lossesGroup['delta'].count(),
                trades='%d' % stGroup['delta'].count(),
                calcs=stGroup.to_dict('index')
            )

        del deals

        return statistics

    def calculate(self, adv_in: dict = None, adv_out: dict = None):
        fast_in, slow_in, signal_in, period_in = self.__p_in
        fast_out, slow_out, signal_out, period_out = self.__p_out

        #_t = time()
        db = None
        if adv_in is not None:
            advises_in = adv_in
        else:
            db = DbHelper(fast_in, slow_in, signal_in, period_in)
            advises_in = db.fetch_advises(self.__start, self.__end)

        if adv_out is not None:
            advises_out = adv_out
        else:
            db = DbHelper(fast_out, slow_out, signal_out, period_out)
            advises_out = db.fetch_advises(self.__start, self.__end)

        deals = dict()
        deals[Backtest.TYPE_LONG] = dict()
        deals[Backtest.TYPE_SHORT] = dict()

        deal = dict()
        deal[Backtest.TYPE_LONG] = None
        deal[Backtest.TYPE_SHORT] = None

        last_deal = dict()
        last_deal[Backtest.TYPE_LONG] = None
        last_deal[Backtest.TYPE_SHORT] = None

        cts = int(self.__start)

        accumulators = dict()
        for _m in [Backtest.MODE_NORMAL, Backtest.MODE_EMERGENCY]:
            accumulators[_m] = dict()
            for _d in [Backtest.DIR_IN, Backtest.DIR_OUT]:
                accumulators[_m][_d] = dict()
                for _t in [Backtest.TYPE_LONG, Backtest.TYPE_SHORT]:
                    accumulators[_m][_d][_t] = []

        in_acc_length = self.__get_norm_length(period_in)
        out_acc_length = self.__get_norm_length(period_out)
        in_em_acc_length = self.__get_emer_length(period_in)
        out_em_acc_length = self.__get_emer_length(period_out)

        while cts <= self.__end:
            one_in_perion = ONE_MINUTE * period_in
            one_out_period = ONE_MINUTE * period_out

            cin_advise = advises_in[cts] if cts in advises_in else None
            cout_advise = advises_out[cts] if cts in advises_out else None

            if cin_advise is None and cout_advise is None:
                cts += ONE_MINUTE
                continue

            cur_interval_in = (cts - cts % one_in_perion) + one_in_perion
            cur_interval_out = (cts - cts % one_out_period) + one_out_period

            for _type in [Backtest.TYPE_LONG, Backtest.TYPE_SHORT]:
                recently_opened = deal[_type] is not None \
                                  and (cur_interval_in - deal[_type]['ts_enter']) <= 2 * one_in_perion

                recently_closed = deal[_type] is None \
                                  and last_deal[_type] is not None \
                                  and not last_deal[_type]['emergency_exit'] \
                                  and (cur_interval_out - last_deal[_type]['ts_exit']) <= 2 * one_out_period

                if cin_advise is not None:
                    accumulators[Backtest.MODE_NORMAL][Backtest.DIR_IN][_type].append(cin_advise)
                    accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_IN][_type].append(cin_advise)

                if len(accumulators[Backtest.MODE_NORMAL][Backtest.DIR_IN][_type]) > in_acc_length:
                    accumulators[Backtest.MODE_NORMAL][Backtest.DIR_IN][_type] = \
                        accumulators[Backtest.MODE_NORMAL][Backtest.DIR_IN][_type][-in_acc_length:]
                if len(accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_IN][_type]) > in_em_acc_length:
                    accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_IN][_type] = \
                        accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_IN][_type][-in_em_acc_length:]

                if cout_advise is not None:
                    accumulators[Backtest.MODE_NORMAL][Backtest.DIR_OUT][_type].append(cout_advise)
                    accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_OUT][_type].append(cout_advise)

                if len(accumulators[Backtest.MODE_NORMAL][Backtest.DIR_OUT][_type]) > out_acc_length:
                    accumulators[Backtest.MODE_NORMAL][Backtest.DIR_OUT][_type] = \
                        accumulators[Backtest.MODE_NORMAL][Backtest.DIR_OUT][_type][-out_acc_length:]
                if len(accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_OUT][_type]) > out_em_acc_length:
                    accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_OUT][_type] = \
                        accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_OUT][_type][-out_em_acc_length:]

                if _type == Backtest.TYPE_LONG:
                    open_appeared = cin_advise is not None and self.__all_is_buy(accumulators[Backtest.MODE_NORMAL][Backtest.DIR_IN][_type])
                    close_appeared = cout_advise is not None and self.__all_is_sell(accumulators[Backtest.MODE_NORMAL][Backtest.DIR_OUT][_type])
                    open_disappeared = self.__all_is_sell(accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_IN][_type])
                    close_disappeared = self.__all_is_buy(accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_OUT][_type])
                elif _type == Backtest.TYPE_SHORT:
                    open_appeared = cin_advise is not None and self.__all_is_sell(accumulators[Backtest.MODE_NORMAL][Backtest.DIR_IN][_type])
                    close_appeared = cout_advise is not None and self.__all_is_buy(accumulators[Backtest.MODE_NORMAL][Backtest.DIR_OUT][_type])
                    open_disappeared = self.__all_is_buy(accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_IN][_type])
                    close_disappeared = self.__all_is_sell(accumulators[Backtest.MODE_EMERGENCY][Backtest.DIR_OUT][_type])
                else:
                    raise ValueError(f'Invalid backtest type "{type}" given')

                flags = (
                    recently_opened,
                    recently_closed,
                    open_appeared,
                    close_appeared,
                    open_disappeared,
                    close_disappeared,
                    cur_interval_in in deals[_type]
                )

                if deal[_type] is None and cin_advise is not None:
                    deal[_type] = self.__start_deal(flags, cts, cin_advise.close)

                elif cout_advise is not None:
                    closed_deal = self.__close_deal(deal[_type], flags, cts, cout_advise.close)
                    if closed_deal is not None:
                        pts = deal[_type]['ts_enter'] - deal[_type]['ts_enter'] % one_in_perion + one_in_perion
                        deals[_type][pts] = deal[_type]
                        last_deal[_type] = deal[_type]
                        deal[_type] = None

            cts += ONE_MINUTE

        del db
        del adv_in
        del adv_out
        db = DbHelper(fast_in, slow_in, signal_in, period_in)

        last_advise = db.find_by_ts(self.__end)
        if last_advise is not None:
            for _type in [Backtest.TYPE_LONG, Backtest.TYPE_SHORT]:
                if deal[_type] is not None:
                    if deal[_type]['ts_enter'] != last_advise.ts:
                        deal[_type]['price_exit'] = last_advise.close
                        deal[_type]['ts_exit'] = last_advise.ts

                        deals[_type][cts] = deal[_type]

        statistics = dict()
        statistics[Backtest.TYPE_LONG] = self.__calculate_statistics(deals[Backtest.TYPE_LONG], Backtest.TYPE_LONG)
        statistics[Backtest.TYPE_SHORT] = self.__calculate_statistics(deals[Backtest.TYPE_SHORT], Backtest.TYPE_SHORT)

        return statistics
