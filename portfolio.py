import argparse
import datetime
import locale
import logging
from collections import defaultdict

import pandas as pd
from dash import Dash, dcc, html
from openapi_client import openapi
from openapi_genclient.exceptions import ApiValueError
from sqlitedict import SqliteDict

from models import constants as cnst, currency, operations, prices, stats
from views.plots import Plot
from views.tables import Table

DB_NAME = 'my_db.sqlite'

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


def get_client():

    def get_token(replace):
        with SqliteDict(DB_NAME,
                        tablename='tokens',
                        autocommit=True) as tokens:
            if replace or ('token' not in tokens):
                tokens['token'] = input('Enter token: ')
            return tokens['token']

    # Try to authorize
    replace_token = False
    while True:
        try:
            client = openapi.api_client(get_token(replace_token))
            # Just to test that the token is valid
            client.user.user_accounts_get()
            return client
        except ApiValueError:
            replace_token = True


def get_portoflio_parsed(client, account_id):
    portfolio = client.portfolio.portfolio_get(broker_account_id=account_id)
    return portfolio.payload.positions


def update_portfolios(client, all_positions):
    accounts = client.user.user_accounts_get()
    assert accounts.status == "Ok"
    for account in accounts.payload.accounts:
        logging.info(
            "update_portfolios '%s' [%s]", account.broker_account_type, account.broker_account_id)
        if account.broker_account_id not in all_positions:
            all_positions[account.broker_account_id] = {
                "name": account.broker_account_type, "positions": {}}

        account_positions = all_positions[account.broker_account_id]
        account_positions["positions"][
            cnst.NOW.date()] = get_portoflio_parsed(
            client, account.broker_account_id)
        all_positions[account.broker_account_id] = account_positions


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


def get_full_name(item):
    # https://www.tinkoff.ru/invest/stocks/{item.ticker}
    return (f'{item.name} ${item.ticker}',
            item.instrument_type,
            item.average_position_price.currency)


def get_usd_df(key_dates):
    first_usd_value = CURRENCY_HELPER.get_rate_for_date(key_dates[0], 'USD')
    df_usd = pd.DataFrame(
        (d, 100.0 *
         (CURRENCY_HELPER.get_rate_for_date(d, 'USD') / first_usd_value - 1.0))
        for d in key_dates)
    df_usd.convert_dtypes()
    return df_usd


def get_stats_df(account, portfolio, key_dates):
    result = []
    if len(key_dates) < 1:
        return result

    ref_date = key_dates[-1]
    dates_range = stats.DayRangeHelper.get_days(key_dates)

    comparer = stats.PortfolioComparer(CURRENCY_HELPER, OPERATIONS_HELPER)
    comparer.prepare_operations(account, dates_range + [ref_date])
    for arange in dates_range:
        items = comparer.compare(account, arange,
                                 portfolio[arange],
                                 ref_date, portfolio[ref_date])
        df = pd.DataFrame(items)
        df.attrs['allowed_items'] = []
        df.attrs['disallowed_columns'] = []
        df.columns = [
            'Name', 'Ticker',
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
    df.attrs['disallowed_columns'] = list(
        x.strftime(cnst.DATE_FORMAT) for x in disallowed_dates)
    df.attrs['allowed_items'] = allowed_items
    df.attrs['date_columns'] = key_dates


def get_data_frame_by_portfolio(account, portfolio):
    logging.info('get_data_frame_by_portfolio [%s]', account)

    portfolio = portfolio['positions']
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

    all_items = {item.ticker: item for d in key_dates for item in portfolio[d]}
    for d in key_dates:
        for item in portfolio[d]:
            full_name = get_full_name(item)[0]
            date_yields[d][full_name] = cnst.get_item_yield(
                item, d, CURRENCY_HELPER)
            date_totals[d][full_name] = cnst.get_item_value(
                item, d, CURRENCY_HELPER)
            date_xirrs_tmp[(item.figi, full_name)
                           ][d] = cnst.get_item_orig_value(item)
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
            if p_curr is not None and p_prev is not None:
                date_prices[d][full_name] = 100.0 * (p_curr - p_prev) / p_prev
            else:
                date_prices[d][full_name] = None

    # Fill XIRRs separately.
    for k, v in date_xirrs_tmp.items():
        xirrs = OPERATIONS_HELPER.get_item_xirrs(account, k[0], v)
        for d in key_dates:
            date_xirrs[d][k[1]] = xirrs[d]

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

    columns = ['Name', 'Type',
               'Currency'] + list(x.strftime(cnst.DATE_FORMAT) for x in key_dates)

    df_yields = pd.DataFrame(items_yields, columns=columns)
    df_totals = pd.DataFrame(items_totals, columns=columns)
    df_percents = pd.DataFrame(items_percents, columns=columns)
    df_xirrs = pd.DataFrame(items_xirrs, columns=columns)
    df_prices = pd.DataFrame(items_prices, columns=columns)

    df_stats = get_stats_df(account, portfolio, key_dates)

    payins_operations = OPERATIONS_HELPER.get_operations_by_dates(
        account, key_dates, operations.Operation.PayIn)
    payouts_operations = OPERATIONS_HELPER.get_operations_by_dates(
        account, key_dates, operations.Operation.PayOut)
    payins = {k.strftime(cnst.DATE_FORMAT): v for k,
              v in payins_operations.items()}
    payouts = {k.strftime(cnst.DATE_FORMAT): v for k,
               v in payouts_operations.items()}
    df_percents.loc[0] = cnst.SUMMARY_COLUMNS + list(
        100 * (df_totals[x].sum() / payins[x] - 1.0)
        for x in df_percents.columns[cnst.SUMMARY_COLUMNS_SIZE:])
    df_yields.loc[0] = cnst.SUMMARY_COLUMNS + \
        list(df_yields[x].sum()
             for x in df_yields.columns[cnst.SUMMARY_COLUMNS_SIZE:])

    dates_totals = {
        x: df_totals.iloc[:, i + cnst.SUMMARY_COLUMNS_SIZE].sum() for i,
        x in enumerate(key_dates)}
    df_xirrs.loc[0] = cnst.SUMMARY_COLUMNS + list(
        OPERATIONS_HELPER.get_total_xirr(account, dates_totals).values())

    df_totals.loc[0] = cnst.SUMMARY_COLUMNS + list(
        df_totals[x].sum() - (payins[x] + payouts[x])
        for x in df_totals.columns[cnst.SUMMARY_COLUMNS_SIZE:])

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
            format='%(asctime)s - [%(levelname)s]' +
            ' - %(filename)s:%(lineno)d:%(funcName)s - %(message)s')
        start_server = not args.no_server

    parse_cmd_line()
    logging.info("main is starting")

    client = get_client()
    PRICES_HELPER = prices.PriceHelper(client, PRICES, FIRST_DATE_TRADES)
    CURRENCY_HELPER = currency.CurrencyHelper(PRICES_HELPER)
    OPERATIONS_HELPER = operations.OperationsHelper(
        client, CURRENCY_HELPER, OPERATIONS)

    with SqliteDict(DB_NAME,
                    tablename='portfolios',
                    autocommit=True) as all_positions:
        update_portfolios(client, all_positions)
        all_positions.commit()

        tabs = []
        for account, positions in all_positions.items():
            OPERATIONS_HELPER.update(account)
            tables = []
            logging.info("get_data_frame_by_portfolio is starting")
            df_yields, df_totals, df_percents, df_xirrs, df_prices, df_stats, df_usd \
                = get_data_frame_by_portfolio(account, positions)
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
                                    [Table.get_stats_table(
                                        df[1],
                                        df[0])])
                                    for df in df_stats],
                                label="Stats"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(
                                     df_totals, 'total'),
                                     Plot.getItemsPlot(df_totals),
                                     Table.get_table(df_totals)],
                                 label="Totals"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(
                                     df_yields, 'yield'),
                                     Plot.getItemsPlot(df_yields),
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
                                           Plot.getItemsPlot(df_prices,
                                                             inverse=True),
                                           Table.get_table(
                                               df_prices,
                                               highlight_neg_pos=True,
                                               highlight_max_row=False,
                                               use_allowed_items=False), ],
                                 label="Prices"),
                             dcc.Tab(
                                 children=[Plot.getAllItemsPlot(df_xirrs_clipped),
                                           Plot.getCandlesPlot(df_xirrs_clipped),
                                           Plot.getItemsPlot(
                                               df_xirrs, [-100, 100],
                                               compare_to_total=True),
                                           Table.get_table(df_xirrs)],
                                 label="XIRR")])))
                tabs.append(
                    dcc.Tab(
                        label=positions['name'],
                        children=tables))

    OPERATIONS_HELPER.commit()
    OPERATIONS.commit()
    OPERATIONS.close()

    PRICES_HELPER.commit()
    PRICES.commit()
    PRICES.close()

    FIRST_DATE_TRADES.commit()
    FIRST_DATE_TRADES.close()

    if start_server:
        app = Dash("Yields")
        app.logger.setLevel = lambda x: None
        app.layout = html.Div(dcc.Tabs(tabs))
        logging.info("Server is starting")
        app.run_server(debug=True)
        logging.info("Server is stopped")
    logging.info("main is done")


# Main entry
main()
