import sys
sys.path.append('gen')

from dataclasses import dataclass
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
from models import constants
from models.base_classes import InstrumentType, Currency, Money
from datetime import datetime
import instruments_pb2
import instruments_pb2_grpc
import logging
import pytz


@dataclass
class Instrument:
    instrument_type: InstrumentType
    currency: Currency
    figi: str
    ticker: str
    name: str
    nominal: Money
    first_trade_date: datetime.date = datetime.min,
    last_trade_date: datetime.date = datetime.max,
    country: str = ""
    sector: str = ""

    def nominal_rate(self):
        if self.instrument_type == InstrumentType.BOND:
            return 0.01 * self.nominal.amount
        else:
            return 1.0

class InstrumentsHelper:

    USER_TIMEZONE = pytz.timezone('Europe/Moscow')

    def __init__(self, channel, metadata, instruments):
        self.__instruments = instruments
        self.__instruments_dict = constants.db2dict(self.__instruments)
        self.__stub = instruments_pb2_grpc.InstrumentsServiceStub(channel)
        self.__metadata = metadata


    def commit(self):
        constants.dict2db(self.__instruments_dict, self.__instruments)

    def __update(self):

        def parse_date(d):
            return datetime.fromtimestamp(
                max(0, d.seconds + d.nanos / 1000000000),
                self.USER_TIMEZONE)

        def ToCurrency(v):
            return Currency(v.upper())

        def parse_bond(v) -> Instrument:
            d1 = parse_date(v.placement_date)
            d2 = parse_date(v.maturity_date)
            return Instrument(instrument_type=InstrumentType.BOND,
                              currency=v.currency, figi=v.figi, ticker=v.ticker,
                              name=v.name,
                              nominal=Money(
                                  currency=ToCurrency(v.nominal.currency),
                                  amount=v.nominal.units + v.nominal.nano /
                                  1000000000),
                              first_trade_date=d1,
                              last_trade_date=d2,
                              country=v.country_of_risk, sector=v.sector)

        def parse_currency(v) -> Instrument:
            return Instrument(instrument_type=InstrumentType.CURRENCY,
                              currency=v.currency, figi=v.figi, ticker=v.ticker,
                              name=v.name,
                              nominal=Money(
                                  currency=ToCurrency(v.nominal.currency),
                                  amount=v.nominal.units + v.nominal.nano /
                                  1000000000),
                              first_trade_date=datetime.min,
                              last_trade_date=datetime.max,
                              country=v.country_of_risk)

        def parse_etf(v) -> Instrument:
            d1 = parse_date(v.released_date)
            d2 = datetime.max
            return Instrument(instrument_type=InstrumentType.ETF,
                              currency=v.currency, figi=v.figi, ticker=v.ticker,
                              name=v.name,
                              nominal=None,
                              first_trade_date=d1,
                              last_trade_date=d2,
                              country=v.country_of_risk, sector=v.sector)

        def parse_share(v) -> Instrument:
            d1 = parse_date(v.ipo_date)
            d2 = datetime.max
            return Instrument(instrument_type=InstrumentType.SHARE,
                              currency=v.currency, figi=v.figi, ticker=v.ticker,
                              name=v.name,
                              nominal=None,
                              first_trade_date=d1,
                              last_trade_date=d2,
                              country=v.country_of_risk, sector=v.sector)

        logging.info("InstrumentsHelper.update")

        request = instruments_pb2.InstrumentsRequest(
            instrument_status='INSTRUMENT_STATUS_ALL')

        for b in self.__stub.Bonds(
                request, metadata=self.__metadata).instruments:
            self.__instruments_dict[b.figi] = parse_bond(b)

        for e in self.__stub.Etfs(
                request, metadata=self.__metadata).instruments:
            self.__instruments_dict[e.figi] = parse_etf(e)

        for s in self.__stub.Shares(
                request, metadata=self.__metadata).instruments:
            self.__instruments_dict[s.figi] = parse_share(s)

        for c in self.__stub.Currencies(
                request, metadata=self.__metadata).instruments:
            self.__instruments_dict[c.figi] = parse_currency(c)

        self.__instruments_dict[constants.FAKE_RUB_FIGI] = Instrument(
            instrument_type=InstrumentType.CURRENCY, currency=Currency.RUB,
            figi=constants.FAKE_RUB_FIGI, ticker='RUB', name='Российский рубль',
            nominal=Money(),
            first_trade_date=datetime.min,
            last_trade_date=datetime.max)


    def get_by_figi(self, figi: str) -> Instrument:
        # Dadya Donner workaround
        if figi == 'BBG00L31GQQ4':
            return Instrument(
                instrument_type=InstrumentType.BOND,
                currency=Currency.RUB,
                nominal=Money(Currency.RUB, 50000.0),
                figi=figi, name="Дядя Дёнер", ticker='DYDNRO V0 07/08/21 BOP1')
        result = self.__instruments_dict.get(figi, None)
        if result:
            return result
        logging.info("InstrumentsHelper.update because of figi=%s", figi)
        self.__update()
        assert figi in self.__instruments_dict, figi
        return self.__instruments_dict[figi]


