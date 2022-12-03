
import sys
sys.path.append('gen')


from dataclasses import dataclass, field
from enum import Enum
from gen import operations_pb2
from gen import users_pb2
from models import constants
from models.base_classes import InstrumentType, Money, Currency
from typing import List, DefaultDict
import collections
import datetime
import logging


class AccountType(Enum):
    BROKER = "Broker"
    IIS = "IIS"
    INVEST_BOX = "InvestBox"


@dataclass
class Position:
    instrument_type: InstrumentType
    figi: str
    quantity: int
    average_price: Money
    expected_yield: Money
    nkd: Money


@dataclass
class Account:
    id: int
    name: str
    type: AccountType
    positions: DefaultDict[datetime.date, List[Position]] = field(
        default_factory=collections.defaultdict)


def api_to_portfolio(positions) -> List[Position]:

    def to_money(v, currency=None):
        return Money(
            currency=Currency(
                currency.upper() if currency else v.currency.upper()),
            amount=constants.sum_units_nano(v))

    def prepare_type(t):
        t = t.title()
        if t == 'Share':
            t = 'Stock'
        return t

    result = {}
    for p in positions:
        if p.figi in result:
            quantity = constants.sum_units_nano(p.quantity)
            result[p.figi].quantity += quantity

            quantity = result[p.figi].quantity
            curr_price = to_money(p.current_price)
            avg_price = result[p.figi].average_price
            yield_price = Money(avg_price.currency,
                                (curr_price.amount - avg_price.amount) * quantity)
            result[p.figi].expected_yield = yield_price

            #assert p.blocked == True or avg_price.amount == 0, result[p.figi]
            logging.info('api_to_portfolio: %d blocked shares handled %s', quantity, p.figi)
        else:
            quantity = constants.sum_units_nano(p.quantity)
            curr_price = to_money(p.current_price)
            # TODO: revert to average_position_price_fifo after https://github.com/Tinkoff/investAPI/issues/312
            #assert p.average_position_price.currency == p.average_position_price_fifo.currency
            avg_price = to_money(p.average_position_price)
            yield_price = Money(avg_price.currency,
                                (curr_price.amount - avg_price.amount) * quantity)
            pos = Position(
                InstrumentType(prepare_type(p.instrument_type)),
                figi=p.figi, quantity=quantity, average_price=avg_price,
                expected_yield=yield_price, nkd=to_money(p.current_nkd)
                if p.current_nkd.currency else Money())
            result[p.figi] = pos

    # Handle RUB positions
    if 'RUB000UTSTOM' in result:
        rub_value = result['RUB000UTSTOM']
        rub_value.figi = constants.FAKE_RUB_FIGI
        result[rub_value.figi] = rub_value
        del result['RUB000UTSTOM']

    return list(result.values())

class V2:

    @staticmethod
    def get_accounts(api_context):
        return (acc for acc in api_context.users().GetAccounts(
            users_pb2.GetAccountsRequest(), metadata=api_context.metadata()).accounts
            if (acc.status == users_pb2.ACCOUNT_STATUS_OPEN) and
            (acc.type in [users_pb2.ACCOUNT_TYPE_TINKOFF,
                          users_pb2.ACCOUNT_TYPE_TINKOFF_IIS]))

    @staticmethod
    def get_positions(api_context, account_id):
        return api_context.operations().GetPortfolio(
            operations_pb2.PortfolioRequest(account_id=account_id),
            metadata=api_context.metadata())
