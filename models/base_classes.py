from enum import Enum
from dataclasses import dataclass

class InstrumentType(Enum):
    BOND = "Bond"
    CURRENCY = "Currency"
    ETF = "Etf"
    SHARE = "Stock"


class Currency(Enum):
    CAD = "CAD"
    CHF = "CHF"
    CNY = "CNY"
    EUR = "EUR"
    GBP = "GBP"
    HKD = "HKD"
    ILS = "ILS"
    JPY = "JPY"
    RUB = "RUB"
    TRY = "TRY"
    USD = "USD"

@dataclass
class Money:
    currency: Currency = Currency.RUB
    amount: float = 0.0


