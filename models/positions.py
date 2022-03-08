from locale import currency
import sys
sys.path.append('gen')

import logging
from gen import users_pb2
from gen import users_pb2_grpc
from gen import operations_pb2_grpc
from gen import operations_pb2
from models.base_classes import InstrumentType, Money, Currency
import collections
from enum import Enum
import datetime
from typing import List, DefaultDict
from dataclasses import dataclass, field



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


def V2ToV2SinglePortfolio(positions) -> List[Position]:

    def MoneyV2ToV2(v, currency=None):
        return Money(
            currency=Currency(
                currency.upper() if currency else v.currency.upper()),
            amount=v.units + v.nano / 1000000000)

    def prepare_type(t):
        t = t.title()
        if t == 'Share':
            t = 'Stock'
        return t

    result = []
    for p in positions:
        quantity = p.quantity.units + p.quantity.nano / 1000000000
        curr_price = MoneyV2ToV2(p.current_price)
        avg_price = MoneyV2ToV2(p.average_position_price)
        yield_price = Money(avg_price.currency,
                            (curr_price.amount - avg_price.amount) * quantity)
        pos = Position(
            InstrumentType(prepare_type(p.instrument_type)),
            figi=p.figi, quantity=quantity, average_price=avg_price,
            expected_yield=yield_price, nkd=MoneyV2ToV2(p.current_nkd)
            if p.current_nkd.currency else Money())
        result.append(pos)

    return result

class V2:

    @staticmethod
    def GetAccounts(channel, metadata):
        users_stub = users_pb2_grpc.UsersServiceStub(channel)
        return (acc for acc in users_stub.GetAccounts(
            users_pb2.GetAccountsRequest(), metadata=metadata).accounts
            if (acc.status == users_pb2.ACCOUNT_STATUS_OPEN) and
            (acc.type in [users_pb2.ACCOUNT_TYPE_TINKOFF,
                          users_pb2.ACCOUNT_TYPE_TINKOFF_IIS]))


    def GetPositions(channel, metadata, account_id):
        operations_stub = operations_pb2_grpc.OperationsServiceStub(channel)
        portfolio = operations_stub.GetPortfolio(
            request=operations_pb2.PortfolioRequest(account_id=account_id), metadata=metadata)
        return portfolio
