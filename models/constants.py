import bisect
import datetime

from pytz import timezone

DATE_FORMAT = "%d.%m.%y %a"
TIMEZONE = timezone('Europe/Moscow')
NOW = datetime.datetime.now(tz=TIMEZONE)
DATE_COLS = 11
TITLE_FOR_SUMMARY = '[Total]'
SUMMARY_COLUMNS = [TITLE_FOR_SUMMARY, '', '']
SUMMARY_COLUMNS_SIZE = len(SUMMARY_COLUMNS)
MOVING_AVERAGE_DAYS = 7 * 4


def prepare_date(d):
    if isinstance(d, datetime.datetime):
        d = d.date()
    return d


def daterange(start_date, end_date):
    for n in range(1 + int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def find_lt(arr, x):
    i = bisect.bisect_left(arr, x)
    if i:
        return arr[i - 1]
    return None


def find_gt(arr, x):
    i = bisect.bisect_right(arr, x)
    if i < len(arr):
        return arr[i]
    return None


def mean(arr):
    total = 0
    cnt = 0
    for x in arr:
        if not x is None:
            total += x
            cnt += 1
    if cnt > 0:
        return total / cnt
    return None


def db2dict(db):
    return dict(db.items())


def dict2db(dct, db):
    db.clear()
    db.update(dct)
    db.commit()


def get_item_price(item, date, prices_helper):
    if date < prices_helper.get_first_trade_date(item.figi):
        return None
    return prices_helper.get_price(item.figi, date)


def get_item_yield(item, date, currency_helper):
    return currency_helper.get_rate_for_date(
        date, item.average_position_price.currency) * item.expected_yield.value


def get_item_yield_percent(item):
    if item.average_position_price.value != 0.0:
        return 100.0 * item.expected_yield.value / (
            item.balance * item.average_position_price.value)
    return 0.0


def get_item_value(item, date, currency_helper):
    rate = currency_helper.get_rate_for_date(
        date, item.average_position_price.currency)
    return rate * (item.expected_yield.value +
                   item.balance * item.average_position_price.value)


def get_item_orig_value(item):
    return item.expected_yield.value + \
        item.balance * item.average_position_price.value


def get_xirr_value(account, item, d, operations_helper):
    d = datetime.datetime.combine(d, datetime.time.max).astimezone()
    return operations_helper.get_item_xirrs(
        account, item.figi, {d: get_item_orig_value(item)})[d]
