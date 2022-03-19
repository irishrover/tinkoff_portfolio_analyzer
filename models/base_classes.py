import sys
sys.path.append('gen')

from enum import Enum
from dataclasses import dataclass
import instruments_pb2_grpc
import marketdata_pb2_grpc
import operations_pb2_grpc
import users_pb2_grpc


@dataclass
class ApiContext:
    def __init__(self, channel, metadata):
        self.__channel = channel
        self.__metadata = metadata
        self.__instruments_stub = instruments_pb2_grpc.InstrumentsServiceStub(self.__channel)
        self.__market_stub = marketdata_pb2_grpc.MarketDataServiceStub(self.__channel)
        self.__operations_stub = operations_pb2_grpc.OperationsServiceStub(self.__channel)
        self.__users_stub = users_pb2_grpc.UsersServiceStub(self.__channel)

    def metadata(self):
        return self.__metadata

    def instruments(self):
        return self.__instruments_stub

    def market(self):
        return self.__market_stub

    def operations(self):
        return self.__operations_stub

    def users(self):
        return self.__users_stub


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


