from enum import Enum
from dataclasses import dataclass
from collections import defaultdict
import datetime
import logging
import sys
import google.protobuf.timestamp_pb2 as ggl

sys.path.append('gen')

from models.base_classes import Currency, Money, InstrumentType
from models import constants
from pyxirr import xirr  # pylint: disable=no-name-in-module
from gen import operations_pb2



def timestamp_from_datetime(dt):
    ts = ggl.Timestamp()
    ts.FromDatetime(dt)
    return ts


def value_to_money(v):
    return Money(currency=Currency(v.currency.upper()),
                 amount=constants.sum_units_nano(v))


class Operation(Enum):
    UNSPECIFIED = 0  # Тип операции не определён.
    INPUT = 1  # Пополнение брокерского счёта.
    BOND_TAX = 2  # Удержание НДФЛ по купонам.
    OUTPUT_SECURITIES = 3  # Вывод ЦБ.
    OVERNIGHT = 4  # Доход по сделке РЕПО овернайт.
    TAX = 5  # Удержание налога.
    BOND_REPAYMENT_FULL = 6  # Полное погашение облигаций.
    SELL_CARD = 7  # Продажа ЦБ с карты.
    DIVIDEND_TAX = 8  # Удержание налога по дивидендам.
    OUTPUT = 9  # Вывод денежных средств.
    BOND_REPAYMENT = 10  # Частичное погашение облигаций.
    TAX_CORRECTION = 11  # Корректировка налога.
    SERVICE_FEE = 12  # Удержание комиссии за обслуж.брок.счёта.
    BENEFIT_TAX = 13  # Удержание налога за материальную выгоду.
    MARGIN_FEE = 14  # Удержание комиссии за непокрытую позицию.
    BUY = 15  # Покупка ЦБ.
    BUY_CARD = 16  # Покупка ЦБ с карты.
    INPUT_SECURITIES = 17  # Перевод ценных бумаг из другого депо.
    SELL_MARGIN = 18  # Продажа в результате Margin-call.
    BROKER_FEE = 19  # Удержание комиссии за операцию.
    BUY_MARGIN = 20  # Покупка в результате Margin-call.
    DIVIDEND = 21  # Выплата дивидендов.
    SELL = 22  # Продажа ЦБ.
    COUPON = 23  # Выплата купонов.
    SUCCESS_FEE = 24  # Удержание комиссии SuccessFee.
    DIVIDEND_TRANSFER = 25  # Передача дивидендного дохода.
    ACCRUING_VARMARGIN = 26  # Зачисление вариационной маржи.
    WRITING_OFF_VARMARGIN = 27  # Списание вариационной маржи.
    DELIVERY_BUY = 28  # Покупка в рамках экспир.фьюч.контракта.
    DELIVERY_SELL = 29  # Продажа в рамках экспир. фьюч контракта.
    TRACK_MFEE = 30  # Комиссия за управление по счёту автослед.
    TRACK_PFEE = 31  # Комиссия за результат по счёту автослед.
    TAX_PROGRESSIVE = 32  # Удержание налога по ставке 15%.
    BOND_TAX_PROGRESSIVE = 33  # Удержание налога по купонам 15%.
    DIVIDEND_TAX_PROGRESSIVE = 34  # Удержание налога по див.15%.
    BENEFIT_TAX_PROGRESSIVE = 35  # Удержание налога за м/в 15%.
    TAX_CORRECTION_PROGRESSIVE = 36  # Корректировка налога 15%.
    TAX_REPO_PROGRESSIVE = 37  # Удержание налога РЕПО 15%.
    TAX_REPO = 38  # Удержание налога за возмещ. по сделкам РЕПО.
    TAX_REPO_HOLD = 39  # Удержание налога по сделкам РЕПО.
    TAX_REPO_REFUND = 40  # Возврат налога по сделкам РЕПО.
    TAX_REPO_HOLD_PROGRESSIVE = 41  # Удержание налога по РЕПО 15%
    TAX_REPO_REFUND_PROGRESSIVE = 42  # Возврат налога РЕПО 15%.
    DIV_EXT = 43  # Выплата дивидендов на карту.
    TAX_CORRECTION_COUPON = 44  # Корректировка налога по купонам.
    CASH_FEE = 45  # Комиссия за валютный остаток.
    OUT_FEE = 46  # Комиссия за вывод валюты с брокерского счета.
    OUT_STAMP_DUTY = 47  # Гербовый сбор.
    OUTPUT_SWIFT = 50  # SWIFT-перевод
    INPUT_SWIFT = 51  # SWIFT-перевод
    OUTPUT_ACQUIRING = 53  # Перевод на карту
    INPUT_ACQUIRING = 54  # Перевод с карты
    OUTPUT_PENALTY = 55  # Комиссия за вывод средств
    ADVICE_FEE = 56  # Списание оплаты за сервис Советов
    TRANS_IIS_BS = 57  # Перевод ценных бумаг с ИИС на бр. счет
    TRANS_BS_BS = 58  # Перевод ц/б с одного бр.счета на другой
    OUT_MULTI = 59  # Вывод денежных средств со счета
    INP_MULTI = 60  # Пополнение денежных средств со счета
    OVER_PLACEMENT = 61  # Размещение биржевого овернайта
    OVER_COM = 62  # Списание комиссии
    OVER_INCOME = 63  # Доход от оверанайта
    OPTION_EXPIRATION = 64  # Экспирация

    def always_visible(self):
        return self.value in [
            Operation.INPUT.value, Operation.OUTPUT.value,
            Operation.DIVIDEND.value, Operation.COUPON.value
        ]


@dataclass
class OperationItem:
    id: str
    instrument_uid: str
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
        # Upgrade figi
        for _, value in self.__operations_dict.items():
            for __, op in value.items():
                op.figi = constants.upgrade_figi(op.figi)


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
                    id=o.id, instrument_uid=o.instrument_uid,
                    date=constants.seconds_to_time(o.date),
                    figi=constants.upgrade_figi(o.figi),
                    operation_type=Operation(int(o.type)),
                    payment=value_to_money(o.payment)))

        account_operations.update(
            {(o.date, o.operation_type, o.id): o for o in operation_items})
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

    def get_item_xirrs(self, account, instrument, dates_totals):
        result = defaultdict(float)
        if instrument.instrument_type == InstrumentType.CURRENCY:
            return result
        last_date = datetime.datetime.combine(
            max(dates_totals.keys()),
            datetime.time.max).astimezone()
        upgraded_instr_figi = constants.upgrade_figi(instrument.figi)
        operations = sorted(
            ((k[0],
              v) for k, v in self.__operations_dict[account].items()
             if(
                 v.instrument_uid == instrument.uid or v.figi ==
                 upgraded_instr_figi) and v.date <= last_date and
             k[1] in self.OPERATION_NAMES_SET),
            key=lambda k: k[0])

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
        else:
            logging.info("xirr: no ops %s15s\t%20s\t%20s/%s", account, instrument.uid, instrument.figi, last_date)

        return result
