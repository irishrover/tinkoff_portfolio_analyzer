class CurrencyHelper:

    USD_FIGI = 'BBG0013HGFT4'

    def __init__(self, price_helper):
        self.__price_helper = price_helper

    def get_rate_for_date(self, d, currency):
        if currency == 'USD':
            return self.__price_helper.get_price(self.USD_FIGI, d)

        assert currency == 'RUB'
        return 1.0
