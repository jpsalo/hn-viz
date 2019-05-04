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
                dcc.Graph(id='scatter-stories'),
                dcc.Slider(
                    id='slider-year',
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
            children=[dcc.Graph(id='bar-chart-monthly')],
        ),
    ]
)


@APP.callback(
    Output(component_id='scatter-stories', component_property='figure'),
    [Input(component_id='slider-year', component_property='value')])
def update_stories(selected_year):
    """
    update_stories
    """
    filtered_df = DF[DF['year'] == selected_year]

    traces = []
    for i in filtered_df.type.unique():
        df_by_type = filtered_df[filtered_df['type'] == i]
        size = df_by_type['days']/np.log(df_by_type['descendants'])
        sizeref = 2.*max(size)/(20.**2)

        traces.append(go.Scatter(
            x=df_by_type['descendants'],
            y=df_by_type['score'],
            text=df_by_type['title'],
            customdata=df_by_type['threadId'],
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
    num_rows = len(df_by_year_month.index)
    colors = ['rgb(49,130,189)'] * num_rows

    if thread_id is not None:
        thread_index = df_by_year_month['threadId'].tolist().index(thread_id)
        for i, color in enumerate(colors):
            if i == thread_index:
                colors[i] = 'rgba(222,45,38,0.8)'

    return {
        'data': [go.Bar(
            x=df_by_year_month['title'],
            y=df_by_year_month['score'],
            marker={
                'color': colors,
                },
            )],
        'layout': go.Layout(
            title={
                'text': f'Stats for {year}/{month}',
                },
            xaxis={'automargin': True},
            yaxis={'title': 'Votes'},
            hovermode='closest',
            ),
        }


@APP.callback(
    Output('bar-chart-monthly', 'figure'),
    [Input('scatter-stories', 'selectedData')])
def update_monthly_stories(selected_data):
    """
    update_monthly_stories
    """
    if selected_data is None:
        # Optimistic defaults
        thread_id = None
        today = datetime.today()
        year = today.year
        month = today.month
    else:
        thread_id = selected_data['points'][0]['customdata']
        thread = DF.loc[DF['threadId'] == thread_id]
        timestamp = thread.iloc[0]['timestamp']
        year = timestamp.year
        month = timestamp.month

    df_by_year_month = DF[
        (DF['year'] == year) &
        (pd.to_datetime(DF['timestamp']).dt.month == month)
    ].sort_values('score', ascending=False)

    return create_bar_chart(df_by_year_month, year, month, thread_id)


if __name__ == '__main__':
    DEBUG = True if DASH_ENV == 'development' else False
    APP.run_server(debug=DEBUG, port=4200)
