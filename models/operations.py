import datetime
import logging
import sys
import google.protobuf.timestamp_pb2 as ggl
sys.path.append('gen')
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from gen import operations_pb2
from pyxirr import xirr  # pylint: disable=no-name-in-module
from models import constants
from models.base_classes import Currency, Money


def timestamp_from_datetime(dt):
    ts = ggl.Timestamp()
    ts.FromDatetime(dt)
    return ts


def value_to_money(v):
    return Money(currency=Currency(v.currency.upper()),
                 amount=constants.sum_units_nano(v))


class Operation(Enum):
    UNSPECIFIED = 0
    INPUT = 1
    BOND_TAX = 2
    OUTPUT_SECURITIES = 3
    OVERNIGHT = 4
    TAX = 5
    BOND_REPAYMENT_FULL = 6
    SELL_CARD = 7
    DIVIDEND_TAX = 8
    OUTPUT = 9
    BOND_REPAYMENT = 10
    TAX_CORRECTION = 11
    SERVICE_FEE = 12
    BENEFIT_TAX = 13
    MARGIN_FEE = 14
    BUY = 15
    BUY_CARD = 16
    INPUT_SECURITIES = 17
    SELL_MARGIN = 18
    BROKER_FEE = 19
    BUY_MARGIN = 20
    DIVIDEND = 21
    SELL = 22
    COUPON = 23
    SUCCESS_FEE = 24
    DIVIDEND_TRANSFER = 25
    ACCRUING_VARMARGIN = 26
    WRITING_OFF_VARMARGIN = 27
    DELIVERY_BUY = 28
    DELIVERY_SELL = 29
    TRACK_MFEE = 30
    TRACK_PFEE = 31
    TAX_PROGRESSIVE = 32
    BOND_TAX_PROGRESSIVE = 33
    DIVIDEND_TAX_PROGRESSIVE = 34
    BENEFIT_TAX_PROGRESSIVE = 35
    TAX_CORRECTION_PROGRESSIVE = 36
    TAX_REPO_PROGRESSIVE = 37
    TAX_REPO = 38
    TAX_REPO_HOLD = 39
    TAX_REPO_REFUND = 40
    TAX_REPO_HOLD_PROGRESSIVE = 41
    TAX_REPO_REFUND_PROGRESSIVE = 42
    DIV_EXT = 43
    TAX_CORRECTION_COUPON = 44
    OUT_STAMP_DUTY = 47

    @staticmethod
    def visible():
        return True


@dataclass
class OperationItem:
    id : str
    date: datetime.date
    figi: str
    operation_type: Operation
    payment: float


class OperationsHelper:

    MIN_DATE = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=constants.TIMEZONE)
    OPERATION_NAMES_SET = frozenset(
        [Operation.BUY, Operation.BUY_CARD, Operation.SELL, Operation.COUPON,
         Operation.DIVIDEND])
    PAY_IN_OUT_NAMES_SET = frozenset([Operation.INPUT, Operation.OUTPUT])


    def __init__(self, api_context, currency_helper, operations):
        self.__operations = operations
        self.__api_context = api_context
        self.__operations_dict = constants.db2dict(self.__operations)
        self.__currency_helper = currency_helper


    def commit(self):
        constants.dict2db(self.__operations_dict, self.__operations)

    def __get_operations(self, account_id, min_date, max_date):
        operations = []
        cursor = ""
        min_date_str = timestamp_from_datetime(min_date)
        max_date_str = timestamp_from_datetime(max_date)
        while True:
            request = operations_pb2.GetOperationsByCursorRequest(
                **
                {"account_id": account_id,
                 "limit": 1000,
                 "state": operations_pb2.OperationState.OPERATION_STATE_EXECUTED,
                 "from": min_date_str,
                 "to": max_date_str,
                 "without_trades": True,
                 "cursor": cursor,
                 })
            curr_operations = self.__api_context.operations().GetOperationsByCursor(
                request=request, metadata=self.__api_context.metadata())
            operations.extend(list(curr_operations.items))
            logging.info(
                "update_operations: [%s] %s..%s, size: %d", account_id,
                min_date.date(),
                max_date.date(),
                len(operations))
            if not curr_operations.has_next:
                break
            cursor = curr_operations.next_cursor
        return operations


    def update(self, account_id):
        account_operations = self.__operations_dict[account_id] \
            if account_id in self.__operations_dict else {}

        if account_operations and any(account_operations):
            min_date = max(k[0] for k in account_operations.keys())
        else:
            min_date = self.MIN_DATE
        max_date = constants.NOW

        logging.info(
            "update_operations: [%s] %s..%s", account_id, min_date.date(),
            max_date.date())
        operations = self.__get_operations(account_id, min_date, max_date)
        operation_items = []
        for o in operations:
            operation_items.append(
                OperationItem(
                    id=o.id, date=constants.seconds_to_time(o.date),
                    figi=o.figi, operation_type=Operation(int(o.type)),
                    payment=value_to_money(o.payment)))

        account_operations.update({(o.date, o.operation_type, o.id): o for o in operation_items})
        self.__operations_dict[account_id] = account_operations

    def get_all_operations_by_dates(self, account, dates):
        operations = sorted(
            ((k[0],
              v) for k, v in self.__operations_dict[account].items()),
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
                partial_sum[o.operation_type] += o.payment.amount * \
                    self.__currency_helper.get_rate_for_date(o.date, o.payment.currency)
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
             if k[1] == operation),
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
                partial_sum += o.payment.amount * \
                    self.__currency_helper.get_rate_for_date(o.date, o.payment.currency)
                p_i += 1
            result[dates[d_i]] = partial_sum
            d_i += 1
        return result


    def get_total_xirr(self, account, dates_totals):
        last_date = datetime.datetime.combine(
            max(dates_totals.keys()),
            datetime.time.max).astimezone()

        operations = sorted(
            ((k[0], v) for k, v in self.__operations_dict[account].items()
             if v.date <= last_date and
             k[1] in self.PAY_IN_OUT_NAMES_SET),
            key=lambda k: k[0])

        result = defaultdict(float)
        if any(operations):
            for d in dates_totals:
                dates_amounts = [
                    (o[1].date, o[1].payment.amount * self.__currency_helper.get_rate_for_date(
                        o[1].date, o[1].payment.currency)) for o in operations
                    if o[1].date.date() <= constants.prepare_date(d)]
                dates_amounts.append((d, -dates_totals[d]))
                res = xirr(dates_amounts)
                result[d] = res * 100.0 if res else 0.0
        return result


    def get_item_xirrs(self, account, figi, dates_totals):
        last_date = datetime.datetime.combine(
            max(dates_totals.keys()),
            datetime.time.max).astimezone()
        operations = sorted(
            ((k[0],
              v) for k, v in self.__operations_dict[account].items()
             if v.figi == figi and v.date <= last_date and
             k[1] in self.OPERATION_NAMES_SET),
            key=lambda k: k[0])

        result = defaultdict(float)
        if any(operations):
            for d in dates_totals:
                prepared_d = constants.prepare_date(d)
                dates_amounts = [
                    (o[1].date, o[1].payment.amount * self.__currency_helper.
                     get_rate_for_date(o[1].date, o[1].payment.currency))
                    for o in operations
                    if o[1].date.date() <= prepared_d]
                if dates_totals[d] == 0:
                    result[d] = 0
                else:
                    dates_amounts.append((d, dates_totals[d]))
                    try:
                        res = xirr(dates_amounts)
                    except:
                        res = None
                    result[d] = res * 100.0 if res else 0.0

        return result
