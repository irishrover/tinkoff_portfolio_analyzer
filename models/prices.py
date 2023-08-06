import sys
sys.path.append('gen')

from dataclasses import dataclass
from models import constants
import datetime
import logging
import marketdata_pb2

class PriceHelper:

    MISSING_FIGIS = frozenset(
        ['BBG00L31GQQ4', 'BBG00QKPJVK3', 'BBG0073DLHS1', 'BBG005VKB7D7',
         'BBG005HLSZ23', 'BBG00P5M77Y0', 'BBG00NNQMD85', 'BBG005HLTYH9',
         'BBG00Y6D0N45', 'BBG005H7MXN2', 'BBG00R980XY3', 'BBG00M8C8Y03',
         'TCS00A103VJ5', 'TCS00A103VK3', 'BBG00QDTJQD2', 'BBG00PNDGP98',
         'BBG00GVWHJJ9', 'BBG00Q38CTS4', 'BBG00Q9K64Q5', 'BBG013N16YX7',
         'BBGHUYNYA0X3', 'IE0000CHPRB9', 'IE00BK224M36', 'TCS0BD3QFB18',
         'IE00BD3QJ757', 'IE00BG0C3K84', 'BBG00HBPXX50', 'BBG00NTZWLM4',
         'BBG00PYMSNH9', 'BBG00Q214T90', 'BBG00P3B1V33', 'BBG00PNM0N81',
         'BBG00PVVTDW6', 'BBG00Q41F4Y3', ])

    @dataclass
    class PriceItem:
        price_date: datetime.date
        price: float
        is_closed: bool

    DAYS_TO_FETCH = 180

    def __init__(
            self, api_context, instruments_helper, prices, first_trade_dates):
        self.price_fetched_count = 0
        self.__api_context = api_context
        self.__prices = prices
        self.__prices_dict = constants.db2dict(self.__prices)

        self.__instruments_helper = instruments_helper
        self.__first_trade_dates = first_trade_dates
        self.__first_trade_dates_dict = constants.db2dict(
            self.__first_trade_dates)
        # Remove unclosed prices to force their updates.
        unclosed_count = 0
        for figi, v in self.__prices_dict.items():
            data = v
            unclosed_prices = list(
                k for(k, v) in data.items() if not v.is_closed)
            if unclosed_prices:
                unclosed_count += len(unclosed_prices)
                for p in unclosed_prices:
                    del data[p]
            self.__prices_dict[figi] = data
        if unclosed_count > 0:
            logging.info("clean %d unclosed prices", unclosed_count)


    @staticmethod
    def combine_dates(date, time):
        return datetime.datetime.combine(
            date, time).astimezone(
            constants.TIMEZONE)

    @staticmethod
    def fix_blocked_figi(figi):
        # NuBank blocked shares
        if figi == 'KYG6683N1034':
            return 'BBG0136WM1M4'
        # Realty Income REIT blocked shares
        elif figi == 'US7561091049':
            return 'BBG000DHPN63'
        # Wells Fargo & Company blockes shares
        elif figi == 'US9497461015':
            return 'BBG000BWQFY7'
        return figi

    def commit(self):
        constants.dict2db(self.__prices_dict, self.__prices)
        constants.dict2db(self.__first_trade_dates_dict,
                          self.__first_trade_dates)

    def __commit_if_needed(self):
        if self.price_fetched_count > 100:
            self.commit()
            self.price_fetched_count = 0
            logging.info("commit prices")
        else:
            self.price_fetched_count += 1


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

        self.__commit_if_needed()
        return value.price if value else 0.0

    def get_price(self, figi, d):
        if figi == constants.FAKE_RUB_FIGI:
            return 1.0
        figi = PriceHelper.fix_blocked_figi(figi)
        d = constants.prepare_date(d)
        assert d <= constants.NOW.date()
        value = self.__ensure_price_loaded(figi, d)
        assert figi in self.__prices_dict
        return value

    def get_first_trade_date(self, figi):
        if figi in self.MISSING_FIGIS:
            return constants.NOW.date()
        figi = PriceHelper.fix_blocked_figi(figi)
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
        assert any(candles), figi
        self.__first_trade_dates_dict[figi] = min(
            constants.prepare_date(c.price_date) for c in candles)

        self.__commit_if_needed()
        return self.__first_trade_dates_dict[figi]
