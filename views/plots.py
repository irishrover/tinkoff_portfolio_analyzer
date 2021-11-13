import plotly.express as px
import plotly.graph_objects as go
from dash import dcc
from plotly.subplots import make_subplots
import numpy as np

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
        figure.update_layout(showlegend=False, height=500)
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
    def getCandlesPlot(df):
        df = df.set_index('Name', inplace=False)
        df.drop(
            df.index.difference(df.attrs['allowed_items']),
            axis=0, inplace=True)
        df.reset_index(inplace=True)
        df = df.sort_values(by=[df.columns[-1]])

        figure = go.Figure()
        for index, row in df.iterrows():
            rr = row[constants.SUMMARY_COLUMNS_SIZE:]
            rr = rr[rr != 0]
            Q1 = rr.quantile(0.10)
            Q2 = rr.quantile(0.90)
            rr = rr[(rr >= Q1) & (rr <= Q2)]
            figure.add_trace(
                go.Box(
                    name=row.iloc[0],
                    y=rr,
                    boxpoints=False,
                    notched=False,
                    showlegend=False))
        figure.update_layout(showlegend=True, height=800)
        return dcc.Graph(figure=figure)
