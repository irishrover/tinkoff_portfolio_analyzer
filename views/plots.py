from dash import dcc
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import textwrap

from models import constants


class Plot:

    @staticmethod
    def getTotalWithMAPlot(df_yield, df_total, df_percents, df_usd):
        figure = make_subplots(specs=[[{"secondary_y": True}]])

        total_x = list(df_total.attrs['date_columns'])
        fig = go.Scatter(
            name='Total Yield', mode='lines+markers',
            line=dict(color='red', width=6, dash='solid', shape="spline"),
            x=total_x, y=df_total.iloc[0, constants.SUMMARY_COLUMNS_SIZE:],
            hovertemplate='%{x}<br>%{y:,.0f}',)
        figure.add_trace(fig, secondary_y=False)

        # Moving average
        figure.add_trace(
            go.Scatter(
                visible='legendonly',
                name='Total Yield MA', mode='lines',
                line=dict(
                    color='rgba(255, 0, 0, 0.35)', width=6, shape="spline"),
                x=total_x, y=df_total.iloc
                [0, constants.SUMMARY_COLUMNS_SIZE:].rolling(
                    constants.MOVING_AVERAGE_DAYS).mean()),
            secondary_y=False)

        yields_x = list(df_yield.attrs['date_columns'])
        fig = go.Scatter(
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
                x=yields_x, y=df_yield.iloc
                [0, constants.SUMMARY_COLUMNS_SIZE:].rolling(
                    constants.MOVING_AVERAGE_DAYS).mean()),
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
                y=df_usd[1].rolling(constants.MOVING_AVERAGE_DAYS).mean(),
                hovertemplate='%{x}<br>%{y:,.1f}%'),
            secondary_y=True)

        figure.update_layout(showlegend=True, height=750,
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
    def getTreeMapPlot(df, neg_values=True):
        df = df.drop(0)
        df = df[df[df.columns[-1]] != 0]
        df["values"] = df[df.columns[-1]].abs()
        wrapper = textwrap.TextWrapper(width=10)
        df[df.columns[0]] = df[df.columns[0]].apply(
            lambda x: wrapper.fill(text=x).replace("\n", "<br>"))
        figure = px.treemap(
            {"labels": df[df.columns[0]],
             "values": df[df.columns[-1]],
             "colors": df[df.columns[-2]]},
            path=[df[df.columns[1]],
                  df[df.columns[2]],
                  'labels'],
            values='values', color="colors", color_continuous_midpoint=0.0
            if neg_values else None,
            color_continuous_scale=px.colors.diverging.PiYG
            if neg_values else 'Plotly3')
        figure.update_layout(
            showlegend=False, height=800, extendtreemapcolors=True,
            uniformtext=dict(minsize=11, mode='hide'))
        return dcc.Graph(figure=figure)

    @staticmethod
    def getTreeMapPlotWithNegForStats(df):
        data_column = 4
        mask = df[df.columns[0]].str.contains('\[', na=False)
        df = df[~mask]
        df = df[df[df.columns[data_column]] != 0]
        df= df[df[df.columns[data_column]] != np.inf]
        df = df[df[df.columns[1]] != 'RUB']
        df = df[df[df.columns[1]] != 'USD000UTSTOM']
        df["values_sign"] = df[df.columns[data_column]] >= 0.0
        df["values"] = df[df.columns[data_column]].abs()
        wrapper = textwrap.TextWrapper(width=10)
        df[df.columns[0]] = df[df.columns[0]].apply(
            lambda x: wrapper.fill(text=x).replace("\n", "<br>"))
        figure = px.treemap(
            {"labels": df[df.columns[0]],
             "values": df[df.columns[-1]],
             "colors": df[df.columns[data_column]]},
            path=[df['values_sign'], 'labels'],
            values='values', color="colors", color_continuous_midpoint=0.0,
            color_continuous_scale=px.colors.diverging.PiYG)
        figure.update_layout(
            showlegend=False, height=800, extendtreemapcolors=True,
            uniformtext=dict(minsize=11, mode='hide'))
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
                (df.attrs['date_columns'][i+1]-df.attrs['date_columns'][i]).days)
        days_diffs.append(1)

        figure = go.Figure()
        for index, row in df.iterrows():
            plot_row = row[constants.SUMMARY_COLUMNS_SIZE:]
            tmp_plot_row = []
            assert len(days_diffs) == plot_row.size
            for index, r in enumerate(plot_row):
                tmp_plot_row.extend([r] * days_diffs[index])
            plot_row = pd.Series(tmp_plot_row)
            plot_row = plot_row[plot_row != 0]
            Q1 = plot_row.quantile(0.15)
            Q2 = plot_row.quantile(0.85)
            plot_row = plot_row[(plot_row >= Q1) & (plot_row <= Q2)]

            figure.add_trace(
                go.Box(
                    name=row.iloc[0],
                    y=plot_row,
                    boxpoints=False,
                    notched=False,
                    showlegend=False))
        figure.update_layout(showlegend=True, height=800)
        return dcc.Graph(figure=figure)
