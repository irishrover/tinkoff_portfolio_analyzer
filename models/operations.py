import sys
sys.path.append('gen')

import datetime
from collections import defaultdict
from enum import Enum, auto

from pyxirr import xirr  # pylint: disable=no-name-in-module

from models import constants
from models.base_classes import Currency


class Operation(Enum):
    BrokerCommission = (auto(), True)  # pylint: disable=invalid-name
    Buy = (auto(), False)  # pylint: disable=invalid-name
    BuyCard = (auto(), False)  # pylint: disable=invalid-name
    Coupon = (auto(), True)  # pylint: disable=invalid-name
    Dividend = (auto(), True)  # pylint: disable=invalid-name
    PartRepayment = (auto(), False)  # pylint: disable=invalid-name
    PayIn = (auto(), True)  # pylint: disable=invalid-name
    PayOut = (auto(), True)  # pylint: disable=invalid-name
    Sell = (auto(), False)  # pylint: disable=invalid-name
    ServiceCommission = (auto(), True)  # pylint: disable=invalid-name
    Tax = (auto(), True)  # pylint: disable=invalid-name
    TaxCoupon = (auto(), True)  # pylint: disable=invalid-name
    TaxDividend = (auto(), True)  # pylint: disable=invalid-name

    def visible(self):
        return self.value[1]


class OperationsHelper:

    MIN_DATE = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=constants.TIMEZONE)
    OPERATION_NAMES_SET = frozenset(
        [Operation.Buy.name, Operation.BuyCard.name, Operation.Sell.name,
         Operation.Coupon.name, Operation.Dividend.name])
    PAY_IN_OUT_NAMES_SET = frozenset(
        [Operation.PayIn.name, Operation.PayOut.name])

    def __init__(self, client, currency_helper, operations):
        self.__client = client
        self.__operations = operations
        self.__operations_dict = constants.db2dict(self.__operations)
        self.__currency_helper = currency_helper

    def commit(self):
        constants.dict2db(self.__operations_dict, self.__operations)

    def update(self, account_id):
        account_operations = self.__operations_dict[account_id] \
            if account_id in self.__operations_dict else {}

        if account_operations and any(account_operations):
            min_date = max(k[0] for k in account_operations.keys())
        else:
            min_date = self.MIN_DATE
        max_date = constants.NOW

        ops = self.__client.operations.operations_get(
            broker_account_id=account_id, _from=min_date.isoformat(),
            to=max_date.isoformat())
        account_operations.update({(o.date, o.operation_type, o.id): o
                                   for o in ops.payload.operations})
        self.__operations_dict[account_id] = account_operations

    def get_all_operations_by_dates(self, account, dates):
        operations = sorted(
            ((k[0], v) for k, v in self.__operations_dict[account].items()
             if v.status == 'Done'),
            key=lambda k: k[0])

        assert isinstance(dates, list)
        dates = sorted(dates)
        result = {op: defaultdict(float) for op in Operation}
        partial_sum = {op: 0.0 for op in Operation}
        d_i = 0
        p_i = 0
        while d_i < len(dates):
            target_date = datetime.datetime.combine(
                dates[d_i], datetime.time.max).astimezone()
            while p_i < len(operations) and operations[p_i][0] <= target_date:
                o = operations[p_i][1]
                partial_sum[Operation[o.operation_type]] += o.payment * \
                    self.__currency_helper.get_rate_for_date(o.date, Currency(o.currency))
                p_i += 1
            for o in Operation:
                result[o][dates[d_i]] += partial_sum[o]
            d_i += 1
        return result

    def get_operations_by_dates(self, account, dates, operation):
        assert isinstance(operation, Operation)
        operations = sorted(
            ((k[0],
              v) for k, v in self.__operations_dict[account].items()
             if k[1] == operation.name and v.status == 'Done'),
            key=lambda k: k[0])

        dates = list(dates)
        result = {}
        d_i = 0
        p_i = 0
        partial_sum = 0
        while d_i < len(dates):
            while p_i < len(operations) and operations[p_i][0] <= \
                datetime.datetime.combine(dates[d_i],
                                          datetime.time.max).astimezone():
                o = operations[p_i][1]
                partial_sum += o.payment * \
                    self.__currency_helper.get_rate_for_date(o.date, Currency(o.currency))
                p_i += 1
            result[dates[d_i]] = partial_sum
            d_i += 1
        return result

    def get_total_xirr(self, account, dates_totals):
        last_date = datetime.datetime.combine(
            max(dates_totals.keys()),
            datetime.time.max).astimezone()

        operations = sorted(
            ((k[0],
              v) for k, v in self.__operations_dict[account].items()
             if v.date <= last_date and v.status == 'Done' and
             k[1] in self.PAY_IN_OUT_NAMES_SET),
            key=lambda k: k[0])

        result = defaultdict(float)
        if any(operations):
            for d in dates_totals:
                dates_amounts = [
                    (o[1].date, o[1].payment * self.__currency_helper.get_rate_for_date(
                        o[1].date, Currency(o[1].currency))) for o in operations
                    if o[1].date.date() <= constants.prepare_date(d)]
                dates_amounts.append((d, -dates_totals[d]))
                res = xirr(dates_amounts)
                result[d] = res * 100.0 if res else 0.0
        return result

    def get_item_xirrs(self, account, figi, dates_totals):
        # Workaround for TCSG
        if figi == 'BBG00QPYJ5H0':
            figi = 'BBG005DXJS36'
        last_date = datetime.datetime.combine(
            max(dates_totals.keys()),
            datetime.time.max).astimezone()
        operations = sorted(
            ((k[0],
              v) for k, v in self.__operations_dict[account].items()
             if v.figi == figi and v.date <= last_date and v.status == 'Done' and
             k[1] in self.OPERATION_NAMES_SET),
            key=lambda k: k[0])

        result = defaultdict(float)
        if any(operations):
            for d in dates_totals:
                dates_amounts = [
                    (o[1].date, o[1].payment) for o in operations
                    if o[1].date.date() <= constants.prepare_date(d)]
                if dates_totals[d] == 0:
                    result[d] = 0
                else:
                    dates_amounts.append((d, dates_totals[d]))
                    res = xirr(dates_amounts)
                    result[d] = res * 100.0 if res else 0.0

        return result
