
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

    def money_v2_v2(v, currency=None):
        return Money(
            currency=Currency(
                currency.upper() if currency else v.currency.upper()),
            amount=constants.sum_units_nano(v))

    def prepare_type(t):
        t = t.title()
        if t == 'Share':
            t = 'Stock'
        return t

    result = []
    for p in positions:
        quantity = constants.sum_units_nano(p.quantity)
        curr_price = money_v2_v2(p.current_price)
        avg_price = money_v2_v2(p.average_position_price)
        yield_price = Money(avg_price.currency,
                            (curr_price.amount - avg_price.amount) * quantity)
        pos = Position(
            InstrumentType(prepare_type(p.instrument_type)),
            figi=p.figi, quantity=quantity, average_price=avg_price,
            expected_yield=yield_price, nkd=money_v2_v2(p.current_nkd)
            if p.current_nkd.currency else Money())
        result.append(pos)

    return result

class V2:

    @staticmethod
    def get_accounts(api_context):
        return (acc for acc in api_context.users().GetAccounts(
            users_pb2.GetAccountsRequest(), metadata=api_context.metadata()).accounts
            if (acc.status == users_pb2.ACCOUNT_STATUS_OPEN) and
            (acc.type in [users_pb2.ACCOUNT_TYPE_TINKOFF,
                          users_pb2.ACCOUNT_TYPE_TINKOFF_IIS]))

    @staticmethod
    def get_rub_position(api_context, account_id):
        positions = api_context.operations().GetPositions(
            operations_pb2.PositionsRequest(account_id=account_id),
            metadata=api_context.metadata())
        for m in positions.money:
            if m.currency.upper() == Currency.RUB.name:
                return Position(
                    InstrumentType.CURRENCY, figi=constants.FAKE_RUB_FIGI,
                    quantity=constants.sum_units_nano(m),
                    average_price=Money(Currency.RUB, 1.0),
                    expected_yield=Money(Currency.RUB, 0.0),
                    nkd=Money(Currency.RUB, 0.0))
        assert False

    @staticmethod
    def get_positions(api_context, account_id):
        return api_context.operations().GetPortfolio(
            operations_pb2.PortfolioRequest(account_id=account_id),
            metadata=api_context.metadata())
