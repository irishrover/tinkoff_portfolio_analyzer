import sys
sys.path.append('gen')

import datetime
import math
from bisect import bisect_left
from collections import OrderedDict, defaultdict

from models import constants as cnst
from models.operations import Operation


DEFAULT_SECTOR = 'Other'
DAY_RANGES = OrderedDict(reversed({
    1: 2,
    7: 2,
    14: 3,
    1 * 30: 5,
    2 * 30: 7,
    3 * 30: 7,
    6 * 30: 14,
    9 * 30: 14,
    1.0 * 365: 21,
    1.5 * 365: 21,
    2.0 * 365: 30,
    3.0 * 365: 60,
    4.0 * 365: 90,
    5.0 * 365: 180,
    6.0 * 365: 180,
    7.0 * 365: 180,
    8.0 * 365: 180,
    9.0 * 365: 180,
    10.0 * 365: 180,
}.items()))


class DayRangeHelper:

    @staticmethod
    def get_days(days):

        def get_day_index(days, day, low, high):
            pos = bisect_left(days, day, lo=low, hi=high)
            if pos >= len(days):
                return -1
            if abs(days[pos] - day) >= abs(day - days[pos - 1]):
                return pos - 1
            return pos

        if len(days) <= 1:
            return []

        ref_date = days[-1]
        result = []
        low = 0
        high = len(
            days) - 1 if days[-1] < cnst.NOW.date() else len(days) - 2
        for d, delta in DAY_RANGES.items():
            d_delta = datetime.timedelta(days=delta)
            day_to_search_for = ref_date - datetime.timedelta(days=d)
            day_index = get_day_index(days, day_to_search_for, low, high)
            low = day_index
            if days[day_index] >= day_to_search_for - d_delta and\
               days[day_index] <= day_to_search_for + d_delta:
                result.insert(0, days[day_index])

        return result


class PortfolioComparer:

    def __init__(self, currencyHelper, operationsHelper, instrumentsHelper):
        self.__currency_helper = currencyHelper
        self.__operations_helper = operationsHelper
        self.__instruments_helper = instrumentsHelper
        self.__prepared_operations = {}

    @staticmethod
    def __get_row(v1, v2):
        if v1 == v2:
            if v1 == 0.0:
                return (None, None, None, None)
            return (v1, v2, None, None)
        if v1 != 0.0:
            return (v1, v2, v2 - v1, (v2 - v1) / abs(v1) * 100.0)
        return (v1, v2, v2 - v1, math.inf)

    @staticmethod
    def __get_total_row(title, d1, d2):
        return (title, "", "", *(PortfolioComparer.__get_row(d1, d2) +
                             PortfolioComparer.__get_row(0, 0) * 3))

    def __get_total_stat_info(self, account, d1, p1, d2, p2, result):
        total_v1 = 0
        total_v2 = 0
        total_y1 = 0
        total_y2 = 0
        for item in p1:
            total_v1 += cnst.get_item_value(item, d1, self.__currency_helper)
            total_y1 += cnst.get_item_yield(item, d1, self.__currency_helper)
        for item in p2:
            total_v2 += cnst.get_item_value(item, d2, self.__currency_helper)
            total_y2 += cnst.get_item_yield(item, d2, self.__currency_helper)

        xirr_1 = self.__operations_helper.get_total_xirr(
            account, {d1: total_v1})[d1]
        xirr_2 = self.__operations_helper.get_total_xirr(
            account, {d2: total_v2})[d2]
        result.append(
            ("[Total]", "", "", *
             (PortfolioComparer.__get_row(total_v1, total_v2) +
              PortfolioComparer.__get_row(0, 0) * 2 +
              PortfolioComparer.__get_row(xirr_1, xirr_2))))

        assert account in self.__prepared_operations
        all_operations = self.__prepared_operations[account]
        for op in (o for o in Operation if o.visible()):
            assert op in self.__prepared_operations[account]
            if all_operations[op][d1] != all_operations[op][d2]:
                result.append(
                    PortfolioComparer.__get_total_row(
                        f"[{op.name.title()}]", all_operations[op][d1],
                        all_operations[op][d2]))

        payins = all_operations[Operation.INPUT]
        payouts = all_operations[Operation.OUTPUT]
        pay_in_out_1 = payins[d1] + payouts[d1]
        pay_in_out_2 = payins[d2] + payouts[d2]
        result.append(("[Total Yield]", "", "", *
                       (PortfolioComparer.__get_row(
                           total_v1 - pay_in_out_1,
                           total_v2 - pay_in_out_2) +
                        PortfolioComparer.__get_row(0, 0) * 3)))
        result.append(
            ("[Total Yield, %]", "", "", *
             (PortfolioComparer.__get_row(
                 100.0 * (total_v1 - pay_in_out_1) / pay_in_out_1,
                 100.0 * (total_v2 - pay_in_out_2) / pay_in_out_2) +
              PortfolioComparer.__get_row(0, 0) * 3)))

        result.append(
            ("[Yield]", "", "", *
             (PortfolioComparer.__get_row(total_y1, total_y2) +
              PortfolioComparer.__get_row(0, 0) * 3)))

    def prepare_operations(self, account, dates):
        self.__prepared_operations[account] = \
            self.__operations_helper.get_all_operations_by_dates(
            account, dates)

    def compare(self, account, d1, p1, d2, p2):
        names1 = set(self.__instruments_helper.get_by_figi(
            item.figi).name for item in p1)
        names2 = set(self.__instruments_helper.get_by_figi(
            item.figi).name for item in p2)

        items1 = {
            self.__instruments_helper.get_by_figi(item.figi).name: item
            for item in p1}
        items2 = {
            self.__instruments_helper.get_by_figi(item.figi).name: item
            for item in p2}

        result = []

        self.__get_total_stat_info(account, d1, p1, d2, p2, result)

        common_names = sorted(names1 | names2)
        for name in common_names:
            in_items1 = name in names1
            in_items2 = name in names2
            ticker = None
            v1 = 0
            v2 = 0
            y1 = 0
            y2 = 0
            b1 = 0
            b2 = 0
            xirr1 = defaultdict(float)
            xirr2 = defaultdict(float)
            ticker = None
            sector = None
            if in_items1:
                item1 = items1[name]
                instrument = self.__instruments_helper.get_by_figi(item1.figi)
                ticker = instrument.ticker
                sector = (instrument.sector
                          if instrument.sector else DEFAULT_SECTOR).capitalize()
                v1 = cnst.get_item_value(item1, d1, self.__currency_helper)
                y1 = cnst.get_item_yield(item1, d1, self.__currency_helper)
                b1 = item1.quantity
                xirr1 = self.__operations_helper.get_item_xirrs(
                    account, item1.figi,
                    {d1: cnst.get_item_orig_value(item1)})
            if in_items2:
                item2 = items2[name]
                instrument = self.__instruments_helper.get_by_figi(item2.figi)
                ticker = instrument.ticker
                sector = (instrument.sector
                          if instrument.sector else DEFAULT_SECTOR).capitalize()
                v2 = cnst.get_item_value(item2, d2, self.__currency_helper)
                y2 = cnst.get_item_yield(item2, d2, self.__currency_helper)
                b2 = item2.quantity
                xirr2 = self.__operations_helper.get_item_xirrs(
                    account, item2.figi,
                    {d2: cnst.get_item_orig_value(item2)})
            if v1 != v2:
                result.append((name, ticker, sector, *
                               (PortfolioComparer.__get_row(v1, v2) +
                                PortfolioComparer.__get_row(y1, y2) +
                                PortfolioComparer.__get_row(b1, b2) +
                                PortfolioComparer.__get_row(
                                    xirr1[d1],
                                    xirr2[d2]))))
        return result
