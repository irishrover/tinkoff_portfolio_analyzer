import sys
sys.path.append('gen')

from collections import defaultdict
from pathlib import Path
import argparse
import datetime
import locale
import logging

from dash import Dash, dcc, html
from sqlitedict import SqliteDict
import grpc
import pandas as pd

from gen import users_pb2
from models import constants as cnst
from models import currency, instruments, operations
from models import positions as pstns
from models import prices, stats
from models.base_classes import ApiContext, Currency
from models.operations import Operation
from views.plots import Plot
from views.tables import Table

DB_NAME = 'my_db.sqlite'
TOKEN = Path('.token').read_text()

locale.setlocale(locale.LC_ALL, ('RU', 'UTF8'))
pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

OPERATIONS = SqliteDict(DB_NAME, tablename='operations', autocommit=True)
OPERATIONS_HELPER = None

FIRST_DATE_TRADES = SqliteDict(
    DB_NAME, tablename='first_date_trades', autocommit=True)

PRICES = SqliteDict(DB_NAME, tablename='prices', autocommit=True)
PRICES_HELPER = None
CURRENCY_HELPER = None

INSTRUMENTS = SqliteDict(DB_NAME, tablename='instruments', autocommit=True)
INSTRUMENTS_HELPER = None


def update_portfolios(all_accounts, api_context):
    accounts = list(pstns.V2.get_accounts(api_context))
    for account in accounts:
        logging.info(
            "update_portfolios '%s' [%s]", account.name, account.id)
        if account.id not in all_accounts:
            logging.info(
                "create a new portfolio '%s' [%s]", account.name, account.id)
            all_accounts[account.id] = pstns.Account(id=account.id,
                                                     name=account.name, type=pstns.AccountType.BROKER
                                                     if account.type == users_pb2.ACCOUNT_TYPE_TINKOFF else
                                                     users_pb2.ACCOUNT_TYPE_TINKOFF_IIS)

        account_positions = all_accounts[account.id]
        fetch_date = cnst.NOW.date()
        positions = pstns.api_to_portfolio(
            pstns.V2.get_positions(api_context, account.id).positions)

        account_positions.positions[fetch_date] = positions
        all_accounts[account.id] = account_positions


def pretty_print_date_diff(day, diff):
    days = diff.days
    result = [f"{day:%d %b %Y} ("]
    if days >= 365:
        result.append(f"{days // 365} year{'s' if days // 365 > 1 else ''}")
        days = days % 365
    if days >= 30:
        result.append(f"{days // 30} month{'s' if days // 30 > 1 else ''}")
        days = days % 30
    if days >= 7:
        result.append(f"{days // 7} week{'s' if days // 7 > 1 else ''}")
        days = days % 7
    if days > 0:
        result.append(f"{days} day{'s' if days > 1 else ''}")
    result.append(')')
    return ' '.join(result)


def get_full_name(item: pstns.Position):
    # https://www.tinkoff.ru/invest/stocks/{item.ticker}
    instrument_data = INSTRUMENTS_HELPER.get_by_figi(item.figi)
    if not item.average_price:
        return (f'{instrument_data.name} ${instrument_data.ticker}',
                item.instrument_type,
                'RUB')
    return (f'{instrument_data.name} ${instrument_data.ticker}',
            item.instrument_type.name.title(),
            instrument_data.currency.name.title(),
            instrument_data.sector.capitalize())


def get_usd_df(key_dates):
    first_usd_value = CURRENCY_HELPER.get_rate_for_date(key_dates[0], Currency.USD)
    df_usd = pd.DataFrame(
        (d, 100.0 *
         (CURRENCY_HELPER.get_rate_for_date(d, Currency.USD) / first_usd_value - 1.0))
        for d in key_dates)
    df_usd.convert_dtypes()
    return df_usd


def get_stats_df(account, portfolio, key_dates):
    result = []
    if len(key_dates) < 1:
        return result

    ref_date = key_dates[-1]
    dates_range = stats.DayRangeHelper.get_days(key_dates)

    comparer = stats.PortfolioComparer(
        CURRENCY_HELPER, OPERATIONS_HELPER, INSTRUMENTS_HELPER)
    comparer.prepare_operations(account, dates_range + [ref_date])
    for arange in dates_range:
        items = comparer.compare(account, arange,
                                 portfolio[arange],
                                 ref_date, portfolio[ref_date])
        df = pd.DataFrame(items)
        df.attrs['allowed_items'] = []
        df.attrs['disallowed_columns'] = []
        df.columns = [
            'Name', 'Ticker', "Currency", "Sector",
            'Old', 'New', 'Diff', 'Diff, %',
            'Old@', 'New@', 'Diff@', 'Diff@, %',
            'Old@@', 'New@@', 'Diff@@', 'Diff@@, %',
            'Old@@@', 'New@@@', 'Diff@@@', 'Diff@@@, %',
        ]
        df.convert_dtypes()
        result.append(
            (pretty_print_date_diff(arange, ref_date - arange), df))
    return result


def tune_df(df, key_dates, allowed_items, disallowed_dates):
    df.convert_dtypes()
    df['Name'] = df['Name'].astype('string')
    df['Type'] = df['Type'].astype('string')
    df['Currency'] = df['Currency'].astype('string')
    df['Sector'] = df['Sector'].astype('string')
    df.attrs['disallowed_columns'] = list(
        x.strftime(cnst.DATE_FORMAT) for x in disallowed_dates)
    df.attrs['allowed_items'] = allowed_items
    df.attrs['date_columns'] = key_dates


def get_data_frame_by_portfolio(account_id, portfolio):

    def insert_row(df, data):
        df.loc[-1] = data
        df.index = df.index + 1
        df.sort_index(inplace=True)

    logging.info('get_data_frame_by_portfolio [%s]', account_id)

    key_dates = sorted(portfolio.keys())
    if not any(key_dates):
        return (pd.DataFrame(),) * 7

    date_yields = {}
    date_totals = {}
    date_percents = {}
    date_xirrs = {}
    date_prices = {}
    date_xirrs_tmp = defaultdict(lambda: defaultdict(dict))

    for d in key_dates:
        date_yields[d] = defaultdict(float)
        date_totals[d] = defaultdict(float)
        date_percents[d] = defaultdict(float)
        date_xirrs[d] = defaultdict(lambda: None)
        date_prices[d] = defaultdict(float)

    all_items = {item.figi: item for d in key_dates for item in portfolio[d]}
    for d in key_dates:
        for item in portfolio[d]:
            full_name = get_full_name(item)[0]
            date_yields[d][full_name] = cnst.get_item_yield(
                item, d, CURRENCY_HELPER)
            date_totals[d][full_name] = cnst.get_item_value(
                item, d, CURRENCY_HELPER)
            date_xirrs_tmp[(item.figi, full_name)][d] = cnst.get_item_value(
                item, d, CURRENCY_HELPER)
            date_percents[d][full_name] = cnst.get_item_yield_percent(item)

        d_time_delta = d - datetime.timedelta(days=7)
        for item in all_items.values():
            full_name = get_full_name(item)[0]
            p_curr = cnst.mean(
                cnst.get_item_price(item, d - datetime.timedelta(days=delta), PRICES_HELPER)
                for delta in range(7))
            p_prev = cnst.mean(
                cnst.get_item_price(
                    item, d_time_delta - datetime.timedelta(days=delta), PRICES_HELPER)
                for delta in range(30))
            if p_curr is not None and p_prev is not None and p_prev != 0.0:
                date_prices[d][full_name] = 100.0 * (p_curr - p_prev) / p_prev
            else:
                date_prices[d][full_name] = None

    # Fill XIRRs separately.
    for k, v in date_xirrs_tmp.items():
        if k[0] != cnst.USD_FIGI:
            xirrs = OPERATIONS_HELPER.get_item_xirrs(account_id, k[0], v)
            for d in key_dates:
                date_xirrs[d][k[1]] = xirrs[d]
        else:
            for d in key_dates:
                date_xirrs[d][k[1]] = 0

    allowed_items = [
        cnst.TITLE_FOR_SUMMARY] + list(date_yields[max(key_dates)].keys())
    items_yields = []
    items_totals = []
    items_percents = []
    items_xirrs = []
    items_prices = []
    for name in set(get_full_name(y) for x in portfolio.values() for y in x):
        item_yield = list(name)
        item_total = list(name)
        item_percent = list(name)
        item_xirr = list(name)
        item_price = list(name)
        for d in key_dates:
            item_yield.append(date_yields[d][name[0]])
            item_total.append(date_totals[d][name[0]])
            item_percent.append(date_percents[d][name[0]])
            item_xirr.append(date_xirrs[d][name[0]])
            item_price.append(date_prices[d][name[0]])
        items_yields.append(item_yield)
        items_totals.append(item_total)
        items_percents.append(item_percent)
        items_xirrs.append(item_xirr)
        items_prices.append(item_price)

    max_date = max(key_dates)
    min_date = min(key_dates)
    days_diff = max(1, int((max_date - min_date).days / cnst.DATE_COLS))
    allowed_dates = [key_dates[0]]
    disallowed_dates = []
    for d in key_dates[1:-1]:
        if ((d - allowed_dates[-1]).days >= days_diff) or \
                ((max_date - d).days <= 5):
            allowed_dates.append(d)
        else:
            disallowed_dates.append(d)

    columns = ['Name', 'Type', 'Currency',
               'Sector'] + list(x.strftime(cnst.DATE_FORMAT) for x in key_dates)

    df_yields = pd.DataFrame(items_yields, columns=columns)
    df_totals = pd.DataFrame(items_totals, columns=columns)
    df_percents = pd.DataFrame(items_percents, columns=columns)
    df_xirrs = pd.DataFrame(items_xirrs, columns=columns)
    df_prices = pd.DataFrame(items_prices, columns=columns)

    df_stats = get_stats_df(account_id, portfolio, key_dates)

    payins_operations = OPERATIONS_HELPER.get_operations_by_dates(
        account_id, key_dates, Operation.INPUT)
    payouts_operations = OPERATIONS_HELPER.get_operations_by_dates(
        account_id, key_dates, Operation.OUTPUT)
    payins = {k.strftime(cnst.DATE_FORMAT): v for k,
              v in payins_operations.items()}
    payouts = {k.strftime(cnst.DATE_FORMAT): v for k,
               v in payouts_operations.items()}
    insert_row(
        df_percents, cnst.SUMMARY_COLUMNS +
        list(
            100 * (df_totals[x].sum() / payins[x] - 1.0)
            for x in df_percents.columns[cnst.SUMMARY_COLUMNS_SIZE:]))
    insert_row(
        df_yields, cnst.SUMMARY_COLUMNS +
        list(
            df_yields[x].sum()
            for x in df_yields.columns[cnst.SUMMARY_COLUMNS_SIZE:]))
    dates_totals = {
        x: df_totals.iloc[:, i + cnst.SUMMARY_COLUMNS_SIZE].sum() for i,
        x in enumerate(key_dates)}
    insert_row(df_xirrs, cnst.SUMMARY_COLUMNS +
               list(
                   OPERATIONS_HELPER.get_total_xirr(
                       account_id, dates_totals).values()))

    insert_row(
        df_totals, cnst.SUMMARY_COLUMNS +
        list(
            df_totals[x].sum() - (payins[x] + payouts[x])
            for x in df_totals.columns[cnst.SUMMARY_COLUMNS_SIZE:]))

    df_usd = get_usd_df(key_dates)

    #
    # df tuning
    #
    for df in [df_yields, df_totals, df_percents, df_xirrs, df_prices]:
        tune_df(df, key_dates, allowed_items, disallowed_dates)

    return (df_yields, df_totals, df_percents, df_xirrs, df_prices, df_stats, df_usd)

#
# Main
#


def main():
    global CURRENCY_HELPER
    global OPERATIONS_HELPER
    global PRICES_HELPER
    global INSTRUMENTS_HELPER

    start_server = True

    def parse_cmd_line():
        nonlocal start_server
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-log", "--log", default='info',
            help="Provide logging level. Example --log debug'")
        parser.add_argument(
            "--no-server", dest="no_server", action='store_true',
            required=False, default=False,
            help="Don't start a web-server with charts and tables.'")
        args = parser.parse_args()
        log_level = args.log.upper()
        logging.basicConfig(
            level=log_level,
            format='%(relativeCreated)10d - [%(levelname)s]' +
            ' - %(filename)15s:%(lineno)3d:%(funcName)30s - %(message)s')
        start_server = not args.no_server

    parse_cmd_line()
    logging.info("main is starting")

    channel = grpc.secure_channel(
        'invest-public-api.tinkoff.ru:443', grpc.ssl_channel_credentials())
    metadata = (('authorization', 'Bearer ' + TOKEN),)

    api_context = ApiContext(channel, metadata)
    INSTRUMENTS_HELPER = instruments.InstrumentsHelper(api_context, INSTRUMENTS)
    PRICES_HELPER = prices.PriceHelper(api_context, INSTRUMENTS_HELPER, PRICES, FIRST_DATE_TRADES)
    CURRENCY_HELPER = currency.CurrencyHelper(PRICES_HELPER)
    OPERATIONS_HELPER = operations.OperationsHelper(api_context, CURRENCY_HELPER, OPERATIONS)


    with SqliteDict(DB_NAME,
                    tablename='accounts',
                    autocommit=True) as accounts:
        update_portfolios(accounts, api_context)
        accounts.commit()

        tabs = []
        for account in accounts.values():
            OPERATIONS_HELPER.update(account.id)
            tables = []
            logging.info("get_data_frame_by_portfolio is starting")
            df_yields, df_totals, df_percents, df_xirrs, df_prices, df_stats, df_usd \
                = get_data_frame_by_portfolio(account.id, account.positions)
            logging.info("get_data_frame_by_portfolio done")
            tables.append(Plot.getTotalWithMAPlot(
                df_yields, df_totals, df_percents, df_usd))

            df_xirrs_clipped = df_xirrs.copy()
            numeric_columns = df_xirrs_clipped.select_dtypes('number').columns
            df_xirrs_clipped[numeric_columns] = df_xirrs_clipped[numeric_columns].clip(
                -100, 300)

            if start_server:
                tables.append(
                    html.Div(
                        dcc.Tabs(
                            [dcc.Tab(
                                children=[html.Div(
                                    [
                                        Table.get_stats_table(df[1], df[0]),
                                        Plot.getTreeMapPlotWithNeg(df[1], 'Diff')
                                    ])
                                    for df in df_stats],
                                label="Stats"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(
                                     df_totals, 'total'),
                                     Plot.getSunburstPlot(df_totals),
                                     Plot.getTreeMapPlotWithNeg(df_totals, df_totals.columns[-1], False),
                                     Table.get_table(df_totals), ],
                                 label="Totals"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(
                                     df_yields, 'yield'),
                                     Plot.getTreeMapPlotWithNeg(df_yields, df_yields.columns[-1]),
                                     Table.get_table(df_yields)],
                                 label="Yields"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(df_percents),
                                           Plot.getItemsPlot(df_percents),
                                           Table.get_table(df_percents)],
                                 label="Percents"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(df_prices),
                                           html.H1("MA7-MA30"),
                                           Plot.getItemsPlot(
                                               df_prices, inverse=True),
                                           Table.get_table(
                                               df_prices,
                                               highlight_neg_pos=True,
                                               highlight_max_row=False,
                                               use_allowed_items=False), ],
                                 label="Prices"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(
                                     df_xirrs_clipped),
                                     Plot.getCandlesPlot(df_xirrs_clipped),
                                     Plot.getItemsPlot(
                                     df_xirrs, [-100, 100],
                                     compare_to_total=True),
                                     Table.get_table(df_xirrs)],
                                 label="XIRR")])))
                tabs.append(
                    dcc.Tab(
                        label=account.name,
                        children=tables))


    OPERATIONS_HELPER.commit()
    OPERATIONS.commit()
    OPERATIONS.close()

    PRICES_HELPER.commit()
    PRICES.commit()
    PRICES.close()

    FIRST_DATE_TRADES.commit()
    FIRST_DATE_TRADES.close()

    INSTRUMENTS_HELPER.commit()
    INSTRUMENTS.commit()
    INSTRUMENTS.close()

    if start_server:
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        app = Dash("Yields")
        app.layout = html.Div(dcc.Tabs(tabs))
        logging.info("Server is starting")
        app.run_server(debug=False)
        logging.info("Server is stopped")
    logging.info("main is done")


# Main entry
main()
