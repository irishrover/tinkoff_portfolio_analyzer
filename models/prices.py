import sys
sys.path.append('gen')

from dataclasses import dataclass
from models import constants
import datetime
import marketdata_pb2


class PriceHelper:

    @dataclass
    class PriceItem:
        price_date: datetime.date
        price: float
        is_closed: bool

    DAYS_TO_FETCH = 180

    def __init__(
        self, api_context, instruments_helper, prices, first_trade_dates):
        self.__api_context = api_context
        self.__prices = prices
        self.__prices_dict = constants.db2dict(self.__prices)

        self.__instruments_helper = instruments_helper
        self.__first_trade_dates = first_trade_dates
        self.__first_trade_dates_dict = constants.db2dict(
            self.__first_trade_dates)
        # Remove unclosed prices to force thier updates.
        for figi, v in self.__prices_dict.items():
            data = v
            unclosed_prices = list(
                k for(k, v) in data.items() if not v.is_closed)
            for p in unclosed_prices:
                del data[p]
            self.__prices_dict[figi] = data

    @staticmethod
    def combine_dates(date, time):
        return datetime.datetime.combine(
            date, time).astimezone(
            constants.TIMEZONE)

    def commit(self):
        constants.dict2db(self.__prices_dict, self.__prices)
        constants.dict2db(self.__first_trade_dates_dict,
                          self.__first_trade_dates)

    def __get_candles(self, figi, min_date, max_date):
        request = marketdata_pb2.GetCandlesRequest(**{
            "figi": figi,
            "from": constants.timestamp_from_datetime(min_date),
            "to": constants.timestamp_from_datetime(max_date),
            "interval": "CANDLE_INTERVAL_DAY",
        })
        candles = self.__api_context.market().GetCandles(
            request=request, metadata=self.__api_context.metadata())
        result = []
        rate = self.__instruments_helper.get_by_figi(figi).nominal_rate()
        for c in candles.candles:
            d = constants.seconds_to_time(c.time).date()
            result.append(
                PriceHelper.PriceItem(
                    d, rate * constants.sum_units_nano(c.close),
                    c.is_complete))

        return result

    def __ensure_price_loaded(self, figi, d):
        d = constants.prepare_date(d)
        if not figi in self.__prices_dict:
            prices = {}
        else:
            if (val := self.__prices_dict[figi].get(d, None)) is not None:
                return val.price
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

        for c in self.__get_candles(figi, min_date, max_date):
            prices[constants.prepare_date(c.price_date)] = c

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
        value = self.__prices_dict[figi].get(d, None)
        return value.price if value else 0.0

    def get_price(self, figi, d):
        if figi == constants.FAKE_RUB_FIGI:
            return 1.0
        d = constants.prepare_date(d)
        assert d <= constants.NOW.date()
        value = self.__ensure_price_loaded(figi, d)
        assert figi in self.__prices_dict
        return value

    def get_first_trade_date(self, figi):
        if figi in self.__first_trade_dates_dict:
            return self.__first_trade_dates_dict[figi]
        if figi == constants.FAKE_RUB_FIGI:
            return constants.prepare_date(datetime.date.min)
        today = constants.NOW.date()
        time_delta = datetime.timedelta(days=360)
        min_date = PriceHelper.combine_dates(
            constants.prepare_date(today - time_delta),
            datetime.time.min)
        max_date = PriceHelper.combine_dates(
            constants.prepare_date(today),
            datetime.time.max)
        candles = self.__get_candles(figi, min_date, max_date)
        assert any(candles)
        self.__first_trade_dates_dict[figi] = min(
            constants.prepare_date(c.price_date) for c in candles)
        return self.__first_trade_dates_dict[figi]
