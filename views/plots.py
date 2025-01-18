from dash import dcc
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import textwrap
import datetime
import logging
import math

from models import constants

def _values_sign_func(x):
    if x >= 0.0:
        return 'Gain'
    return 'Loss'


def rolling_mean_df(df_x, df_y):
    df = pd.DataFrame({'Y': df_y})
    df.index = pd.to_datetime(df_x).values
    return df.rolling(constants.MOVING_AVERAGE_TIMEDELTA).mean()['Y']


class Plot:

    @staticmethod
    def getTotalWithMAPlot(df_yield, df_total, df_percents, df_usd, df_xirrs):
        figure = make_subplots(specs=[[{"secondary_y": True}]])
        total_x = list(df_total.attrs['date_columns'])
        if len(df_total) == 0:
            return dcc.Graph(figure=figure)
        # Add vertical lines for for Jan 1st
        start_year = total_x[0].year
        end_year = total_x[-1].year
        for year in range(start_year + 1, end_year + 1):
            jan_first = datetime.datetime(year, 1, 1)
            if total_x[0] <= pd.Timestamp(jan_first) <= total_x[-1]:
                figure.add_shape(
                    type="line",
                    x0=jan_first,
                    x1=jan_first,
                    y0=0,
                    y1=1,
                    yref="paper",
                    line=dict(color='rgba(255, 255, 255, 128)',
                              width=7, dash="solid"),
                    layer="below",
                )
            jul_first = datetime.datetime(year, 7, 1)
            if total_x[0] <= pd.Timestamp(jul_first) <= total_x[-1]:
                figure.add_shape(
                    type="line",
                    x0=jul_first,
                    x1=jul_first,
                    y0=0,
                    y1=1,
                    yref="paper",
                    line=dict(color='rgba(255, 255, 255, 128)',
                              width=3, dash="solid"),
                    layer="below",
                )


        fig = go.Scatter(
            name='Total Yield', mode='lines+markers',
            line=dict(color='red', width=6, dash='solid', shape="spline"),
            x=total_x, y=df_total.iloc[0, constants.SUMMARY_COLUMNS_SIZE:],
            hovertemplate='%{x}<br>%{y:,.0f}',)
        figure.add_trace(fig, secondary_y=False)

        # Moving average
        figure.add_trace(
            go.Scatter(
                name='Total Yield MA', mode='lines',
                line=dict(
                    color='rgba(255, 0, 0, 0.35)', width=6, shape="spline"),
                x=total_x,
                y=rolling_mean_df(total_x, df_total.iloc[0, constants.SUMMARY_COLUMNS_SIZE:])),
            secondary_y=False)

        yields_x = list(df_yield.attrs['date_columns'])
        fig = go.Scatter(
            visible='legendonly',
            name='Yield', mode='lines+markers',
            line=dict(
                color='#3D9970', width=6, dash='solid', shape="spline"),
            x=yields_x,
            y=df_yield.iloc[0, constants.SUMMARY_COLUMNS_SIZE:],
            hovertemplate='%{x}<br>%{y:,.0f}')
        figure.add_trace(fig, secondary_y=False)

        # Moving average
        figure.add_trace(
            go.Scatter(
                visible='legendonly',
                name='Yield MA', mode='lines',
                line=dict(
                    color='rgba(61, 153, 112, 0.35)', width=6, shape="spline"),
                x=yields_x,
                y=rolling_mean_df(yields_x, df_yield.iloc[0, constants.SUMMARY_COLUMNS_SIZE:])),
            secondary_y=False)

        # Percents
        figure.add_trace(
            go.Scatter(
                visible='legendonly',
                name=df_percents.iloc[0, 0] + '%', mode='lines+markers',
                line=dict(
                    color='rgba(0, 128, 255, 0.35)', width=6, dash='solid',
                    shape="spline"),
                x=list(df_percents.attrs['date_columns']),
                y=df_percents.iloc[0, constants.SUMMARY_COLUMNS_SIZE:],
                hovertemplate='%{x}<br>%{y:,.1f}%'),
            secondary_y=True)

        # XIRR
        figure.add_trace(go.Scatter(
            visible='legendonly',
            name='XIRR',
            mode='lines+markers',
            line=dict(color='rgba(255, 128, 64, 0.55)', width=6, shape="spline"),
            x=list(df_xirrs.attrs['date_columns']),
            y=df_xirrs.iloc[0, constants.SUMMARY_COLUMNS_SIZE:],
            hovertemplate='%{x}<br>%{y:,.1f}%'),
            secondary_y=True)

        figure.add_trace(go.Scatter(
            name='XIRR MA',
            mode='lines',
            line=dict(color='rgba(255, 128, 64, 0.35)',
                      width=6, shape="spline"),
            x=list(df_xirrs.attrs['date_columns']),
            y=rolling_mean_df(
                df_xirrs.attrs['date_columns'], df_xirrs.iloc[0, constants.SUMMARY_COLUMNS_SIZE:]),
            hovertemplate='%{x}<br>%{y:,.1f}%'),        secondary_y=True)

        # USD
        figure.add_trace(go.Scatter(
            visible='legendonly',
            name='USD %',
            mode='lines+markers',
            line=dict(color='rgba(0, 0, 255, 0.55)', width=6, shape="spline"),
            x=df_usd[0],
            y=df_usd[1],
            hovertemplate='%{x}<br>%{y:,.1f}%'),
            secondary_y=True)

        figure.add_trace(
            go.Scatter(
                visible='legendonly',
                name='USD %, MA', mode='lines',
                line=dict(
                    color='rgba(0, 0, 255, 0.35)', width=6, shape="spline"),
                x=df_usd[0],
                y=rolling_mean_df(df_usd[0], df_usd[1]),
                hovertemplate='%{x}<br>%{y:,.1f}%'),
            secondary_y=True)

        figure.update_layout(showlegend=True, height=700,
                             legend=dict(orientation="h", yanchor="top",
                                         y=1.05, x=.5, xanchor="center"),
                             margin=dict(l=0, r=0, t=0, b=0))
        figure.update_yaxes(
            secondary_y=False, title_text="Yield/Total Yield",
            title_font=dict(color='#3D9970'),
            tickfont=dict(color='#3D9970'))
        figure.update_yaxes(
            secondary_y=True, title_text="USD, %", rangemode='normal',
            title_font=dict(color='blue'),
            tickfont=dict(color="blue"),)
        figure.update_xaxes(tickformat="%a %b %d\n%Y")
        figure.update_traces(marker=dict(size=8))

        return dcc.Graph(figure=figure)

    @staticmethod
    def getItemsPlot(
            df, clamp_range=None, compare_to_total=False, inverse=False):
        if len(df) == 0:
            return dcc.Graph()
        if compare_to_total:
            total_value = df.iloc[0, -1]
        else:
            total_value = 0
        no_total_df = df.set_index('Name', inplace=False)
        no_total_df.drop(
            no_total_df.index.difference(df.attrs['allowed_items']),
            axis=0, inplace=True)
        no_total_df.reset_index(inplace=True)
        no_total_df = no_total_df.sort_values(by=[df.columns[-1]])
        figure = px.bar(
            no_total_df, x=no_total_df.columns[0],
            y=no_total_df.columns[-1],
            color=np.where(
                no_total_df[no_total_df.columns[-1]] < total_value, "neg",
                "pos"),
            color_discrete_map={'neg': 'green', 'pos': 'red', }
            if inverse else{'neg': 'red', 'pos': 'green', })
        figure.update_layout(showlegend=False, height=700)
        if clamp_range:
            figure.update_yaxes(range=clamp_range)
        figure.update_traces(marker_coloraxis=None,
                             hovertemplate='%{x}<br>%{y:,.0f}')
        return dcc.Graph(figure=figure)

    @staticmethod
    def getAllItemsPlot(df, stack_group_name=None):
        if len(df) == 0:
            return dcc.Graph()
        if stack_group_name:
            df = df.drop(0)
        df = df.sort_values(by=[df.columns[-1]], ascending=True)
        figure = make_subplots()
        for i in range(len(df.index)):
            fig = go.Scatter(
                name=df.iloc[i, 0], mode='lines+markers',
                line=dict(width=8, dash='solid', shape="spline"),
                x=list(df.attrs['date_columns']),
                y=df.iloc[i, constants.SUMMARY_COLUMNS_SIZE:],
                hovertemplate='%{x}<br>%{y:,.0f}',
                stackgroup=stack_group_name)
            figure.add_trace(fig)
        figure.update_traces(marker=dict(size=8))
        figure.update_layout(showlegend=True, height=750,
                            legend=dict(orientation="h"))
        return dcc.Graph(figure=figure)

    @staticmethod
    def getSunburstPlot(df):
        if len(df) == 0:
            return dcc.Graph()
        df = df.drop(0)
        wrapper = textwrap.TextWrapper(width=10)
        df[df.columns[0]] = df[df.columns[0]].apply(
            lambda x: wrapper.fill(text=x).replace("\n", "<br>"))
        figure = px.sunburst(
            df,
            #maxdepth=3,
            values=df[df.columns[-1]],
            names=df[df.columns[0]],
            path=[px.Constant("All"),
                  df["Type"],
                  df["Sector"],
                  df["Currency"],
                  df["Name"]])
        figure.update_layout(showlegend=False, height=800)
        figure.update_traces(
            sort=True,
            hoverinfo='name+label+percent entry',
            hovertemplate='%{label}<br>%{value:,.0f}<br>%{percentEntry:,.1%}',
            textinfo='label+percent entry', textfont_size=20,
            marker=dict(line=dict(color='#000000', width=2)))
        return dcc.Graph(figure=figure)


    @staticmethod
    def getTreeMapPlotWithNeg(df, diff_col_name, with_neg=True):
        mask = df[df.columns[0]].str.contains(r'\[', na=False)
        df = df[~mask]
        df = df[df[diff_col_name] != 0]
        df = df[df[diff_col_name] != np.inf]

        # Skip empty dataframes
        if df.empty:
            return None

        df = df.sort_values(['Name'])
        df['values_sign'] = df[diff_col_name].apply(_values_sign_func)
        df["values"] = df[diff_col_name].abs()
        grouped_dict = {}
        if with_neg:
            group_by_list = ["values_sign", "Currency", "Sector"]
        else:
            group_by_list = ["Currency", "Sector"]
        while any(group_by_list):
            grouped_dict.update(df.groupby(group_by_list)[
                                diff_col_name].sum().to_dict())
            group_by_list.pop()

        grouped_dict = dict(
            ('All/' + ('/'.join(x[0]) if isinstance(x[0], tuple) else x[0]), x[1])
            for x in grouped_dict.items())
        grouped_dict['All'] = df[diff_col_name].sum()

        wrapper = textwrap.TextWrapper(width=15)
        df["Name"] = df["Name"].apply(
            lambda x: wrapper.fill(text=x).replace("\n", "<br>"))

        if with_neg:
            path=[px.Constant("All"),
                  df['values_sign'],
                  df['Currency'],
                  df['Sector'],
                  'labels']
        else:
            path=[px.Constant("All"),
                  df['Currency'],
                  df['Sector'],
                  'labels']

        figure = px.treemap(
            {"labels": df["Name"],
             "values": df['values'],
             "colors": df[diff_col_name]},
            path=path,
            branchvalues="total", values='values', color="colors",
            color_continuous_midpoint=0.0,
            color_continuous_scale=px.colors.diverging.PiYG,
        )

        figure.update_traces(hovertemplate='%{label}<br>%{customdata:,.0f}')
        figure.update_traces(texttemplate="%{label}<br><br>%{customdata:,.0f}")
        figure.update_layout(
            showlegend=False, height=800, extendtreemapcolors=True,
            uniformtext=dict(minsize=12, mode='hide'))

        grouped_list = list(grouped_dict[x]
                            for x in figure.data[0].ids[df.shape[0]:])
        figure.data[0].customdata = np.append(
            figure.data[0].marker.colors[:df.shape[0]], grouped_list)
        figure.data[0].marker.colors = np.append(
            figure.data[0].marker.colors[:df.shape[0]], grouped_list)

        for i in range(len(figure.data[0].labels)):
            if figure.data[0].ids[i] in grouped_dict:
                figure.data[0].labels[i] = f"<b>{figure.data[0].labels[i]}: {grouped_dict[figure.data[0].ids[i]]:,.0f}</b>"

        if 0 in figure.data:
            figure.data[0]['textfont']['size'] = 20

        return dcc.Graph(figure=figure)

    @staticmethod
    def getCandlesPlot(df):
        df = df.set_index('Name', inplace=False)
        df.drop(
            df.index.difference(df.attrs['allowed_items']),
            axis=0, inplace=True)
        df.reset_index(inplace=True)
        df = df.sort_values(by=[df.columns[-1]])

        days_diffs = []
        for i in range(len(df.attrs['date_columns']) - 1):
            days_diffs.append(
                (df.attrs['date_columns'][i + 1] - df.attrs['date_columns'][i]).days)
        days_diffs.append(1)

        figure = go.Figure()
        for _, row in df.iterrows():
            plot_row = row[constants.SUMMARY_COLUMNS_SIZE:]
            tmp_plot_row = []
            assert len(days_diffs) == plot_row.size
            for index, r in enumerate(plot_row):
                tmp_plot_row.extend([r] * days_diffs[index])
            plot_row = pd.Series(tmp_plot_row)
            plot_row = plot_row[plot_row != 0]
            q1 = plot_row.quantile(0.15)
            q2 = plot_row.quantile(0.85)
            plot_row = plot_row[(plot_row >= q1) & (plot_row <= q2)]

            figure.add_trace(
                go.Box(
                    name=row.iloc[0],
                    y=plot_row,
                    boxpoints=False,
                    notched=False,
                    showlegend=False))
        figure.update_layout(showlegend=True, height=800)
        return dcc.Graph(figure=figure)
