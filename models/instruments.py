import sys
sys.path.append('gen')

from dataclasses import dataclass
from datetime import datetime
from models import constants
from models.base_classes import InstrumentType, Currency, Money
import instruments_pb2 as instrs
import common_pb2 as cmn
import logging


@dataclass
class Instrument:
    instrument_type: InstrumentType
    uid: str
    currency: Currency
    figi: str
    ticker: str
    name: str
    nominal: Money
    first_trade_date: datetime.date = datetime.min
    last_trade_date: datetime.date = datetime.max
    country: str = ""
    sector: str = ""
    exchange: str = ""

    def nominal_rate(self):
        if self.instrument_type == InstrumentType.BOND:
            return 0.01 * self.nominal.amount
        return 1.0

class InstrumentsHelper:

    def __init__(self, api_context, instruments):
        self.__instruments = instruments
        self.__instruments_dict = constants.db2dict(self.__instruments)
        self.__api_context = api_context
        self.__update()


    def commit(self):
        constants.dict2db(self.__instruments_dict, self.__instruments)

    @staticmethod
    def to_currency(v):
        return Currency(v.upper())

    @staticmethod
    def __parse_bond(v) -> Instrument:
        d1 = constants.seconds_to_time(v.placement_date)
        d2 = constants.seconds_to_time(v.maturity_date)
        return Instrument(
            instrument_type=InstrumentType.BOND,
            uid=v.uid,
            currency=InstrumentsHelper.to_currency(v.currency),
            figi=v.figi, ticker=v.ticker, name=v.name,
            exchange=v.exchange,
            nominal=Money(
                currency=InstrumentsHelper.to_currency(v.nominal.currency),
                amount=constants.sum_units_nano(v.nominal)),
            first_trade_date=d1, last_trade_date=d2,
            country=v.country_of_risk, sector=v.sector
            if len(v.sector) else constants.DEFAULT_SECTOR)

    @staticmethod
    def __parse_currency(v) -> Instrument:
        return Instrument(
            instrument_type=InstrumentType.CURRENCY,
            uid=v.uid,
            currency=InstrumentsHelper.to_currency(v.nominal.currency),
            figi=v.figi, ticker=v.ticker,
            name=v.name, exchange=v.exchange, sector='Currency',
            nominal=Money(
                currency=InstrumentsHelper.to_currency(v.nominal.currency),
                amount=constants.sum_units_nano(v.nominal)),
            first_trade_date=datetime.min, last_trade_date=datetime.max,
            country=v.country_of_risk)

    @staticmethod
    def __parse_etf(v) -> Instrument:
        d1 = constants.seconds_to_time(v.released_date)
        d2 = datetime.max
        return Instrument(
            instrument_type=InstrumentType.ETF,
            uid=v.uid,
            currency=InstrumentsHelper.to_currency(v.currency),
            figi=v.figi, ticker=v.ticker, name=v.name,
            exchange=v.exchange, nominal=None,
            first_trade_date=d1, last_trade_date=d2,
            country=v.country_of_risk, sector=v.sector
            if len(v.sector) else constants.DEFAULT_SECTOR)

    @staticmethod
    def __parse_share(v) -> Instrument:
        d1 = constants.seconds_to_time(v.ipo_date)
        d2 = datetime.max
        return Instrument(
            instrument_type=InstrumentType.SHARE,
            uid=v.uid,
            currency=InstrumentsHelper.to_currency(v.currency),
            figi=v.figi, ticker=v.ticker, name=v.name,
            exchange=v.exchange, nominal=None,
            first_trade_date=d1, last_trade_date=d2,
            country=v.country_of_risk, sector=v.sector
            if len(v.sector) else constants.DEFAULT_SECTOR)

    @staticmethod
    def __parse_futures(v) -> Instrument:
        d1 = constants.seconds_to_time(v.first_trade_date)
        d2 = constants.seconds_to_time(v.last_trade_date)
        return Instrument(
            instrument_type=InstrumentType.FUTURES,
            uid=v.uid,
            currency=InstrumentsHelper.to_currency(v.currency),
            figi=v.figi,
            ticker=v.ticker,
            name=v.name,
            exchange=v.exchange, nominal=None,
            first_trade_date=d1, last_trade_date=d2,
            country=v.country_of_risk, sector=v.sector
            if len(v.sector) else constants.DEFAULT_SECTOR)


    def __update(self):

        logging.info("InstrumentsHelper.update")

        request = instrs.InstrumentsRequest(
            instrument_status='INSTRUMENT_STATUS_ALL')

        for b in self.__api_context.instruments().Bonds(
                request, metadata=self.__api_context.metadata()).instruments:
            self.__instruments_dict[b.figi] = InstrumentsHelper.__parse_bond(b)

        for e in self.__api_context.instruments().Etfs(
                request, metadata=self.__api_context.metadata()).instruments:
            self.__instruments_dict[e.figi] = InstrumentsHelper.__parse_etf(e)

        for s in self.__api_context.instruments().Shares(
                request, metadata=self.__api_context.metadata()).instruments:
            self.__instruments_dict[s.figi] = InstrumentsHelper.__parse_share(s)

        for c in self.__api_context.instruments().Currencies(
                request, metadata=self.__api_context.metadata()).instruments:
            self.__instruments_dict[c.figi] = InstrumentsHelper.__parse_currency(c)

        self.__instruments_dict[constants.FAKE_RUB_FIGI] = Instrument(
            instrument_type=InstrumentType.CURRENCY, currency=Currency.RUB,
            uid="a92e2e25-a698-45cc-a781-167cf465257c",
            figi=constants.FAKE_RUB_FIGI, ticker='RUB', name='Российский рубль',
            exchange='mos',
            nominal=Money(), sector='Currency',
            first_trade_date=datetime.min,
            last_trade_date=datetime.max)

    def __try_get_by_figi(self, figi: str) -> Instrument:
        request = instrs.FindInstrumentRequest(query=figi)
        v = self.__api_context.instruments().FindInstrument(
            request, metadata=self.__api_context.metadata())
        if not any(v.instruments):
            return None
        uid = v.instruments[0].uid
        kind = v.instruments[0].instrument_kind
        result = None
        request = instrs.InstrumentRequest(
            id_type=instrs.InstrumentIdType.INSTRUMENT_ID_TYPE_UID, id=uid)
        if kind == cmn.InstrumentType.INSTRUMENT_TYPE_BOND:
            response = self.__api_context.instruments().BondBy(
                request, metadata=self.__api_context.metadata()).instrument
            result = InstrumentsHelper.__parse_bond(response)
        elif kind == cmn.InstrumentType.INSTRUMENT_TYPE_CURRENCY:
            response = self.__api_context.instruments().CurrencyBy(
                request, metadata=self.__api_context.metadata()).instrument
            result = InstrumentsHelper.__parse_currency(response)
        elif kind == cmn.InstrumentType.INSTRUMENT_TYPE_ETF:
            response = self.__api_context.instruments().EtfBy(
                request, metadata=self.__api_context.metadata()).instrument
            result = InstrumentsHelper.__parse_etf(response)
        elif kind == cmn.InstrumentType.INSTRUMENT_TYPE_SHARE:
            response = self.__api_context.instruments().ShareBy(
                request, metadata=self.__api_context.metadata()).instrument
            result = InstrumentsHelper.__parse_share(response)
        elif kind == cmn.InstrumentType.INSTRUMENT_TYPE_FUTURES:
            response = self.__api_context.instruments().FutureBy(
                request, metadata=self.__api_context.metadata()).instrument
            result = InstrumentsHelper.__parse_futures(response)
        else:
            assert False, v
        self.__instruments_dict[figi] = result
        return result

    def get_by_figi(self, figi: str) -> Instrument:
        if figi == 'BBG00QPYJ5H0':
            figi = 'TCS00A107UL4'
        result = self.__instruments_dict.get(figi, None)
        if result:
            return result
        logging.info("InstrumentsHelper.try_update because of figi=%s", figi)
        result = self.__try_get_by_figi(figi)
        if result:
            return result
        logging.info("InstrumentsHelper.update because of figi=%s", figi)
        self.__update()
        assert figi in self.__instruments_dict, figi
        return self.__instruments_dict[figi]
