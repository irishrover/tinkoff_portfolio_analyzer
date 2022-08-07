from dash import dash_table
from dash.dash_table.Format import Format, Group, Scheme, Symbol


class Table:

    @staticmethod
    def __interlace_rows():
        return [{'if': {'row_index': 'odd'},
                 'backgroundColor': 'rgb(248, 248, 248)'},
                {'if': {'filter_query': '{Name} contains  "["'},
                 'backgroundColor': 'rgba(0, 0, 248, 0.3)'}, ]

    @staticmethod
    def __highlight_neg_pos(df):
        result = []
        df_numeric_columns = df.select_dtypes('number').columns
        result.extend(
            {'if':
             {'column_type': 'numeric',
              'filter_query': f"{{{col}}} >= 3",
              'column_id': col},
             'backgroundColor': 'rgba(248, 0, 0, 0.3)'
             }
            for(i, col) in enumerate(df_numeric_columns))
        result.extend(
            {'if':
             {'column_type': 'numeric',
              'filter_query': f"{{{col}}} <= -3",
              'column_id': col},
             'backgroundColor': 'rgba(0, 248, 0, 0.3)'
             }
            for(i, col) in enumerate(df_numeric_columns))
        return result

    @staticmethod
    def __highlight_max_row(df):
        result = []
        df_numeric_columns = df.drop(
            ['id', 'Name', 'Type', 'Currency', 'Sector'] +
            df.attrs['disallowed_columns'],
            axis=1)

        for minmax in [
            ('#993D70', df_numeric_columns[df_numeric_columns != 0.0].idxmin(
                axis=1)),
            ('#3D9970', df_numeric_columns[df_numeric_columns != 0.0].idxmax(
                axis=1)), ]:
            result.extend(
                {'if':
                 {'filter_query': f"{{id}} = '{i}'",
                  'column_id': col},
                    'backgroundColor': minmax[0], 'color': 'white'}
                for(i, col) in enumerate(minmax[1]))

        return result

    @staticmethod
    def get_table(
            df, use_allowed_items=True, highlight_max_row=True,
            highlight_neg_pos=False):
        df = df.set_index('Name', inplace=False)
        if use_allowed_items:
            df.drop(
                df.index.difference(df.attrs['allowed_items']),
                axis=0, inplace=True)
        df.reset_index(inplace=True)
        df['id'] = df.index

        conditions = conditions = Table.__interlace_rows()
        if highlight_max_row:
            conditions.extend(Table.__highlight_max_row(df))
        if highlight_neg_pos:
            conditions.extend(Table.__highlight_neg_pos(df))

        return dash_table.DataTable(
            columns=[{'id': str(c), 'name': str(c),
                      "type": ("text" if c in ["Name", "Type", "Currency", "Sector"]
                               else "numeric"),
                      "format": Format(group=Group.yes, precision=0,
                                       scheme=Scheme.fixed, symbol=Symbol.no)}
                     for c in df.columns],
            data=df.to_dict('records'),
            hidden_columns=['id'] + list(df.attrs['disallowed_columns']),
            filter_action="native",
            sort_action="native",
            sort_mode="single",
            sort_by=[{'column_id': df.columns[-2], 'direction': 'desc'}],
            fill_width=False,
            style_table={'minWidth': '100%'},
            style_cell={'padding': '5px',
                        'minWidth': '80px', 'width': '80px',
                        'whiteSpace': 'normal'
                        },
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'textAlign': 'center', 'fontWeight': 'bold'
            },
            style_cell_conditional=[
                {'if': {'column_id': "Name"},
                 'textAlign': 'left', 'minWidth': '275px'},
                {'if': {'column_id': "Type"},
                 'textAlign': 'left', 'minWidth': '35px', 'width': '35px'},
                {'if': {'column_id': "Currency"},
                 'textAlign': 'left', 'minWidth': '65px', 'width': '65px'},
            ],
            style_data_conditional=conditions,
        )

    @staticmethod
    def get_stats_table(df, date_str):
        df = df.set_index('Name', inplace=False)
        df.reset_index(inplace=True)
        df['id'] = df.index

        def __index2str(index, col):
            if index < 4:
                return [date_str, "", col]
            if index < 8:
                return [date_str, 'Value', col.replace('@', '')]
            if index < 12:
                return [date_str, 'Yield', col.replace('@', '')]
            if index < 16:
                return [date_str, 'Balance', col.replace('@', '')]

            return [date_str, 'XIRR', col.replace('@', '')]

        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'id': str(c),
                      'name': __index2str(index, str(c)),
                      "type":
                      ("text" if c in ["Name", "Ticker", "Sector"] else "numeric"),
                      "format":
                      Format(
                          group=Group.yes, precision=0, scheme=Scheme.fixed,
                          symbol=Symbol.no)} for index, c
                     in enumerate(df.columns)],
            hidden_columns=['id'] + list(df.attrs['disallowed_columns']),
            filter_action="native", filter_query="{Name} contains '['",
            sort_action="native", sort_mode="single",
            sort_by=[{'column_id': df.columns[15],
                      'direction': 'desc'}],
            fill_width=False, style_table={'minWidth': '100%'},
            style_cell={'padding': '5px', 'minWidth': '80px', 'width': '80px',
                        'whiteSpace': 'normal'},
            style_header={'backgroundColor': 'rgb(230, 230, 230)',
                          'textAlign': 'center', 'fontWeight': 'bold'},
            style_cell_conditional=[{'if': {'column_id': "Name"},
                                     'textAlign': 'left', 'minWidth': '275px'},
                                    {'if': {'column_id': "Ticker"},
                                     'textAlign': 'left', 'minWidth': '35px',
                                     'width': '35px'}, ],
            style_data_conditional=Table.__interlace_rows() +
            [{'if': {'column_id': 'Diff, %', },
              'backgroundColor': 'dodgerblue', 'color': 'white'},
             {'if': {'column_id': 'Diff@, %', },
              'backgroundColor': 'dodgerblue', 'color': 'white'},
             {'if': {'column_id': 'Diff@@, %', },
              'backgroundColor': 'dodgerblue', 'color': 'white'},
             {'if': {'column_id': 'Diff@@@, %', },
              'backgroundColor': 'dodgerblue', 'color': 'white'}, ],
            merge_duplicate_headers=True,)
