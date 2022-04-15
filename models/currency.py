import sys

sys.path.append('gen')

from models import constants
from models.base_classes import Currency


class CurrencyHelper:

    def __init__(self, price_helper):
        self.__price_helper = price_helper

    def get_rate_for_date(self, d, currency: Currency):
        if currency == Currency.USD:
            return self.__price_helper.get_price(constants.USD_FIGI, d)

        assert currency == Currency.RUB, currency
        return 1.0
