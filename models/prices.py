import datetime

from models import constants


class PriceHelper:

    DAYS_TO_FETCH = 180

    def __init__(self, client, prices, first_trade_dates):
        self.__client = client
        self.__prices = prices
        self.__prices_dict = constants.db2dict(self.__prices)

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
    def combine_dates(date, time):
        return datetime.datetime.combine(date, time).astimezone()

    def commit(self):
        constants.dict2db(self.__prices_dict, self.__prices)
        constants.dict2db(self.__first_trade_dates_dict,
                          self.__first_trade_dates)

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

        candles = self.__client.market.market_candles_get(
            figi, min_date, max_date, 'day')
        for candle in candles.payload.candles:
            prices[constants.prepare_date(candle.time)] = candle.c

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
        if figi == 'FAKE_RUB_FIGI':
            return 1.0
        d = constants.prepare_date(d)
        assert d <= constants.NOW.date()
        value = self.__ensure_price_loaded(figi, d)
        assert figi in self.__prices_dict
        return value

    def get_first_trade_date(self, figi):
        if figi == 'FAKE_RUB_FIGI':
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
        candles = self.__client.market.market_candles_get(
            figi, min_date, max_date, 'day')

        assert any(candles.payload.candles)
        self.__first_trade_dates_dict[figi] = min(
            constants.prepare_date(c.time) for c in candles.payload.candles)
        return self.__first_trade_dates_dict[figi]
