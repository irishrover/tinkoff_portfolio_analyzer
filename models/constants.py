import bisect
import datetime
from pytz import timezone, utc
from google.protobuf.timestamp_pb2 import Timestamp
from models.base_classes import InstrumentType

#DATE_FORMAT = "%d.%m.%y %a"
DATE_FORMAT = "%d.%m.%y"
TIMEZONE = timezone('Europe/Moscow')
NOW = datetime.datetime.now(tz=TIMEZONE)
DATE_COLS = 20
TITLE_FOR_SUMMARY = '[Total]'
SUMMARY_COLUMNS = [TITLE_FOR_SUMMARY, '', '', '']
SUMMARY_COLUMNS_SIZE = len(SUMMARY_COLUMNS)
MOVING_AVERAGE_TIMEDELTA = datetime.timedelta(days=90)
FAKE_RUB_FIGI = 'FAKE_RUB_FIGI'
USD_FIGI = 'BBG0013HGFT4'
EURO_FIGI = 'BBG0013HJJ31'
HKD_FIGI = 'BBG0013HSW87'
DEFAULT_SECTOR = 'Other'

UPGRADE_FIGI = {
    'BBG00VSYBL16': 'TCS00A101UD4',
    'BBG00YNJ37B7': 'TCS00A102KR3',
    'BBG01475RD30': 'TCSS0A1049P5',
    'BBG111111111': 'TCS10A101X68',
    'KYG6683N1034': 'BBG0136WM1M4',
    'TCS00A102EK1': 'TCS10A102EK1',
    'TCS00A102EM7': 'TCS10A102EM7',
    'TCS00A102EQ8': 'TCS10A102EQ8',
    'TCS00A1039P6': 'TCS10A1039P6',
    'TCS00A103VF3': 'TCS20A103VF3',
    'TCS00A103VG1': 'TCS20A103VG1',
    'TCS00A103VH9': 'TCS10A103VH9',
    'TCS00A1049P5': 'TCSS0A1049P5',
    'TCS00A105A95': 'TCSS0A105A95',
    'TCS0207L1061': 'BBG00KHGQ0H4',
    'TCS20A101UD4': 'TCS00A101UD4',
    'TCS20A101X68': 'TCS10A101X68',
    'TCS20A102EK1': 'TCS10A102EK1',
    'TCS20A102EM7': 'TCS10A102EM7',
    'TCS20A102EQ8': 'TCS10A102EQ8',
    'TCS20A102KR3': 'TCS00A102KR3',
    'TCS20A1039P6': 'TCS10A1039P6',
    'TCS5207L1061': 'TCS2207L1061',
    'TCS6683N1034': 'BBG0136WM1M4',
    'TCSS0A102YC6': 'TCS00A102YC6',
    'TCSS0A1052T1': 'TCS00A1052T1',
    'TCSSSA1049P5': 'TCSS0A1049P5',
    'IE00BD3QJ757': 'BBG005HLTYH9',
}


def prepare_date(d):
    if isinstance(d, datetime.datetime):
        d = d.date()
    return d

def sum_units_nano(v):
    return v.units + v.nano / 1000000000

def seconds_to_time(d):
    return datetime.datetime.fromtimestamp(
        max(0, d.seconds + d.nanos / 1000000000), TIMEZONE)

def timestamp_from_datetime(dt):
    return Timestamp(seconds=int(dt.replace(tzinfo=utc).timestamp()))

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


def get_item_nkd(item, date, currency_helper):
    return currency_helper.get_rate_for_date(
        date, item.nkd.currency) * item.nkd.amount * item.quantity


def get_item_yield(item, date, currency_helper):
    rate = currency_helper.get_rate_for_date(date, item.average_price.currency)
    return rate * item.expected_yield.amount + get_item_nkd(
        item, date, currency_helper)


def get_item_yield_percent(item):
    if item.average_price.amount != 0.0:
        return 100.0 * item.expected_yield.amount / (
            item.quantity * item.average_price.amount)
    return 0.0


def get_item_value(item, date, currency_helper):
    rate = currency_helper.get_rate_for_date(
        date, item.average_price.currency)
    return rate * (item.expected_yield.amount +
                   item.quantity * (item.average_price.amount + item.nkd.amount))

def get_item_blocked_value(item, date, currency_helper, instruments_helper):
    # Currencies aren't blocked yet.
    if item.instrument_type == InstrumentType.CURRENCY:
        return 0.0
    instrument = instruments_helper.get_by_figi(item.figi)
    exchange = instrument.exchange
    if exchange != 'unknown' and not exchange.endswith('_close'):
        return 0.0
    rate = currency_helper.get_rate_for_date(
        date, item.average_price.currency)
    return rate * (item.expected_yield.amount +
                   item.quantity * (item.average_price.amount + item.nkd.amount))


def get_item_orig_value(item):
    return item.expected_yield.amount + \
        item.quantity * item.average_price.amount


def upgrade_figi(figi):
    return UPGRADE_FIGI.get(figi, figi)
