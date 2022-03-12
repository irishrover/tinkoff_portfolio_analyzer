import sys
sys.path.append('gen')

from models import constants
import datetime
from google.protobuf.timestamp_pb2 import Timestamp
import pytz
import marketdata_pb2_grpc
import marketdata_pb2


class PriceHelper:

    DAYS_TO_FETCH = 180
    USER_TIMEZONE = pytz.timezone('Europe/Moscow')

    def __init__(
        self, instruments_helper, prices, first_trade_dates,
            channel, metadata):
        self.__prices = prices
        self.__prices_dict = constants.db2dict(self.__prices)
        self.__market_stub = marketdata_pb2_grpc.MarketDataServiceStub(channel)
        self.__metadata = metadata

        self.__instruments_helper = instruments_helper
        self.__first_trade_dates = first_trade_dates
        self.__first_trade_dates_dict = constants.db2dict(
            self.__first_trade_dates)
        # Don't cache the last 2 dates as their close prices could have
        # change since the last fetch.
        for figi, v in self.__prices_dict.items():
            data = v
            for _ in range(2):
                if any(data.keys()):
                    data.pop(max(data.keys()), None)
                else:
                    break
            self.__prices_dict[figi] = data

    @staticmethod
    def timestamp_from_datetime(dt):
        ts = Timestamp(seconds=int(dt.replace(tzinfo=pytz.utc).timestamp()))
        return ts

    @staticmethod
    def combine_dates(date, time):
        return datetime.datetime.combine(
            date, time).astimezone(
            PriceHelper.USER_TIMEZONE)

    def commit(self):
        constants.dict2db(self.__prices_dict, self.__prices)
        constants.dict2db(self.__first_trade_dates_dict,
                          self.__first_trade_dates)

    def get_candles(self, figi, min_date, max_date):
        request = marketdata_pb2.GetCandlesRequest(**{
            "figi": figi,
            "from": PriceHelper.timestamp_from_datetime(min_date),
            "to": PriceHelper.timestamp_from_datetime(max_date),
            "interval": "CANDLE_INTERVAL_DAY",
        })
        candles = self.__market_stub.GetCandles(
            request=request, metadata=self.__metadata)
        result = []
        rate = self.__instruments_helper.get_by_figi(figi).nominal_rate()
        for c in candles.candles:
            d = datetime.datetime.fromtimestamp(
                c.time.seconds + c.time.nanos/1000000000, PriceHelper.USER_TIMEZONE).date()
            result.append(
                (d, rate * (c.close.units + c.close.nano / 1000000000.0)))

        return result

    def __ensure_price_loaded(self, figi, d):
        d = constants.prepare_date(d)
        if not figi in self.__prices_dict:
            prices = {}
        else:
            if (val := self.__prices_dict[figi].get(d, None)) is not None:
                return val
            prices = self.__prices_dict[figi]

        time_delta = datetime.timedelta(days=self.DAYS_TO_FETCH)
        min_date = d - time_delta
        max_date = min(
            d + time_delta, constants.prepare_date(constants.NOW.date()))

        keys = sorted(prices.keys())
        actual_min_date = constants.find_lt(keys, d)
        actual_max_date = constants.find_gt(keys, d)

        if not actual_min_date is None:
            min_date = max(min_date, actual_min_date)
        min_date = PriceHelper.combine_dates(min_date, datetime.time.min)

        if not actual_max_date is None:
            max_date = min(max_date, actual_max_date)
        max_date = PriceHelper.combine_dates(max_date, datetime.time.max)

        for c in self.get_candles(figi, min_date, max_date):
            prices[constants.prepare_date(c[0])] = c[1]

        # Propagate the missing values from prev values.
        last_value = None
        for day in constants.daterange(
                constants.prepare_date(min_date),
                constants.prepare_date(max_date)):
            prepared_dd = constants.prepare_date(day)
            if not prepared_dd in prices:
                if not last_value is None:
                    prices[prepared_dd] = last_value
            else:
                last_value = prices[prepared_dd]
        self.__prices_dict[figi] = prices
        return self.__prices_dict[figi].get(d, 0.0)

    def get_price(self, figi, d):
        if figi == constants.FAKE_RUB_FIGI:
            return 1.0
        d = constants.prepare_date(d)
        assert d <= constants.NOW.date()
        value = self.__ensure_price_loaded(figi, d)
        assert figi in self.__prices_dict
        return value

    def get_first_trade_date(self, figi):
        if figi == constants.FAKE_RUB_FIGI:
            return constants.prepare_date(datetime.date.min)
        if figi in self.__first_trade_dates_dict:
            return self.__first_trade_dates_dict[figi]
        today = constants.NOW.date()
        time_delta = datetime.timedelta(days=360)
        min_date = PriceHelper.combine_dates(
            constants.prepare_date(today - time_delta),
            datetime.time.min)
        max_date = PriceHelper.combine_dates(
            constants.prepare_date(today),
            datetime.time.max)
        candles = self.get_candles(figi, min_date, max_date)
        assert any(candles)
        self.__first_trade_dates_dict[figi] = min(
            constants.prepare_date(c[0]) for c in candles)
        return self.__first_trade_dates_dict[figi]
