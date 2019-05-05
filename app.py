"""module"""
# -*- coding: utf-8 -*-
from datetime import datetime
import os
import json
import numpy as np
import pandas as pd
import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from google.cloud import bigquery
from google.oauth2 import service_account
from settings import DASH_ENV

# https://simpleit.rocks/apis/google-cloud/using-google-cloud-with-heroku/#in-heroku
CREDENTIALS_RAW = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

SERVICE_ACCOUNT_INFO = json.loads(CREDENTIALS_RAW)

CREDENTIALS = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO)

CLIENT = bigquery.Client(
    project='hacker-news-visualization',
    credentials=CREDENTIALS)

# https://stackoverflow.com/q/53177427/7010222
# https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.useQueryCache
JOB_CONFIG = bigquery.QueryJobConfig()
JOB_CONFIG.use_query_cache = True

FD = open('query.sql', 'r')
SQL = FD.read()
FD.close()

print('gathering data')
QUERY_JOB = CLIENT.query(SQL, job_config=JOB_CONFIG)
print('running', QUERY_JOB.running())
print('done', QUERY_JOB.done())
print('cache', QUERY_JOB.cache_hit)

print('loading DataFrame')
DF = QUERY_JOB.to_dataframe()

pd.set_option('display.max_rows', 11)
print(DF[[
    'title',
    'author',
    'type',
    'score',
    'descendants',
    'days',
    'year',
    'threadId',
]])
print('memory usage', DF.info(memory_usage='deep'))

EXTERNAL_STYLESHEETS = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

SLIDER_YEAR_ID = 'slider-year'

APP = dash.Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEETS)

SERVER = APP.server

COLORS = {
    'background': 'white',
    'text': 'grey'
}

MARKDOWN_TEXT = '''
### Hacker News

Project

Jukka-Pekka Salo
'''

APP.layout = html.Div(
    style={
        'marginLeft': '1em',
        'marginRight': '1em',
        'backgroundColor': COLORS['background'],
    },
    children=[
        dcc.Markdown(children=MARKDOWN_TEXT),
        html.Div(
            style={'padding': 10},
            children=[
                dcc.Graph(
                    id='scatter-stories',
                    config={'modeBarButtonsToRemove': ['select2d', 'lasso2d']},
                ),
                dcc.Slider(
                    id=SLIDER_YEAR_ID,
                    min=DF['year'].min(),
                    max=DF['year'].max(),
                    value=DF['year'].max(),
                    marks={
                        str(year): str(year) for year in DF['year'].unique()
                    },
                ),
            ],
        ),
        html.Div(
            style={'padding': 10},
            children=[
                dcc.Graph(
                    id='bar-chart-monthly',
                    config={'modeBarButtonsToRemove': ['select2d', 'lasso2d']},
                )
            ],
        ),
    ]
)


@APP.callback(
    Output(component_id='scatter-stories', component_property='figure'),
    [
        Input(component_id=SLIDER_YEAR_ID, component_property='value'),
        Input('bar-chart-monthly', 'selectedData'),
    ])
def update_stories(selected_year, selected_data):
    """
    update_stories
    """
    is_year_select = False
    ctx = dash.callback_context

    if ctx.triggered:
        input_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if input_id == SLIDER_YEAR_ID:
            is_year_select = True

    filtered_df = DF[DF['year'] == selected_year]

    traces = []
    for i in filtered_df.type.unique():
        df_by_type = filtered_df[filtered_df['type'] == i]
        size = df_by_type['days']/np.log(df_by_type['descendants'])
        sizeref = 2.*max(size)/(20.**2)
        selected_points = None

        if selected_data is not None and is_year_select is False:
            thread_id = selected_data['points'][0]['customdata']
            thread = DF.loc[DF['threadId'] == thread_id]
            thread_type = thread.iloc[0]['type']
            thread_year = thread.iloc[0]['year']

            if thread_year == selected_year:
                selected_points = []

                if thread_type == i and thread_year == selected_year:
                    thread_index = df_by_type['threadId'].tolist().index(
                        thread_id)
                    selected_points.append(thread_index)

        traces.append(go.Scatter(
            x=df_by_type['descendants'],
            y=df_by_type['score'],
            text=df_by_type['title'],
            customdata=df_by_type['threadId'],
            selectedpoints=selected_points,
            mode='markers',
            opacity=0.7,
            marker={
                'size': size,
                # https://plot.ly/python/bubble-charts/#scaling-the-size-of-bubble-charts
                'sizeref': sizeref,
                'sizemode': 'area',
                'sizemin': 5,
                'line': {'width': 0.5, 'color': 'white'},
                },
            name=i,
        ))

    return {
        'data': traces,
        'layout': go.Layout(
            xaxis={'type': 'log', 'title': 'Comments'},
            yaxis={'title': 'Votes'},
            # FIXME
            margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
            showlegend=True,
            clickmode='event+select',
            legend={'x': 0, 'y': 1},
            hovermode='closest',
            )
        }


def create_bar_chart(df_by_year_month, year, month, thread_id):
    """
    create_bar_chart
    """
    selected_points = None

    if thread_id is not None:
        thread_index = df_by_year_month['threadId'].tolist().index(thread_id)
        selected_points = [thread_index]

    return {
        'data': [go.Bar(
            x=df_by_year_month['title'],
            y=df_by_year_month['score'],
            customdata=df_by_year_month['threadId'],
            selectedpoints=selected_points,
        )],
        'layout': go.Layout(
            title={
                'text': f'Stats for {year}/{month}',
                },
            xaxis={'automargin': True},
            yaxis={'title': 'Votes'},
            hovermode='closest',
            clickmode='event+select',
            ),
        }


@APP.callback(
    Output('bar-chart-monthly', 'figure'),
    [
        Input('scatter-stories', 'selectedData'),
        Input(component_id=SLIDER_YEAR_ID, component_property='value'),
        Input('scatter-stories', 'clickData'),
    ])
def update_monthly_stories(selected_data, selected_year, click_data):
    """
    update_monthly_stories
    """

    # Change year
    if selected_data is None and click_data is None:
        thread_id = None
        today = datetime.today()
        current_year = today.year
        current_month = today.month
        year = selected_year
        month = current_month if selected_year == current_year else 1

    # Select
    if selected_data is not None:
        thread_id = selected_data['points'][0]['customdata']
        thread = DF.loc[DF['threadId'] == thread_id]
        timestamp = thread.iloc[0]['timestamp']
        year = timestamp.year
        month = timestamp.month

        if timestamp.year != selected_year:
            thread_id = None
            today = datetime.today()
            current_year = today.year
            current_month = today.month
            year = selected_year
            month = current_month if selected_year == current_year else 1

    # Unselect
    if selected_data is None and click_data is not None:
        thread_id = None
        previous_thread_id = click_data['points'][0]['customdata']
        thread = DF.loc[DF['threadId'] == previous_thread_id]
        timestamp = thread.iloc[0]['timestamp']
        year = timestamp.year
        month = timestamp.month

        if timestamp.year != selected_year:
            today = datetime.today()
            current_year = today.year
            current_month = today.month
            year = selected_year
            month = current_month if selected_year == current_year else 1

    df_by_year_month = DF[
        (DF['year'] == year) &
        (pd.to_datetime(DF['timestamp']).dt.month == month)
    ].sort_values('score', ascending=False)

    return create_bar_chart(df_by_year_month, year, month, thread_id)


if __name__ == '__main__':
    DEBUG = True if DASH_ENV == 'development' else False
    APP.run_server(debug=DEBUG, port=4200)
