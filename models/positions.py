from dataclasses import dataclass, field
from typing import List, DefaultDict
import datetime
from enum import Enum
import collections
from models.base_classes import InstrumentType, Money, Currency


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


def V1ToV2SinglePortfolio(positions) -> List[Position]:

    def MoneyV1ToV2(v):
        return Money(currency=Currency(v.currency), amount=v.value)

    result = []
    for p in positions:
        pos = Position(
            InstrumentType(str(p.instrument_type)),
            figi=p.figi,
            quantity=p.balance,
            average_price=MoneyV1ToV2(p.average_position_price),
            expected_yield=MoneyV1ToV2(p.expected_yield),
            nkd=Money())
        result.append(pos)

    return result


def V1ToV2Portofolio(accounts):

    def MoneyV1ToV2(v):
        return Money(currency=Currency(v.currency), amount=v.value)

    result = {}
    for a in accounts:
        account = Account(
            id=a[0],
            name=a[1]['name'],
            type=AccountType.IIS
            if a[1]['name'].endswith('Iis') else AccountType.BROKER,
            positions=collections.defaultdict())
        result[account.id] = account
        for k, v in a[1]['positions'].items():
            account.positions[k] = []
            for p in v:
                pos = Position(
                    InstrumentType(str(p.instrument_type)),
                    figi=p.figi,
                    quantity=p.balance,
                    average_price=MoneyV1ToV2(p.average_position_price),
                    expected_yield=MoneyV1ToV2(p.expected_yield),
                    nkd=Money())
                account.positions[k].append(pos)
    return result

class PositionsHelper:

    def __init__(self, positions):
        pass
