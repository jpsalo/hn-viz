"""module"""
# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from google.cloud import bigquery
from google.oauth2 import service_account

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
print(DF)

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

        dcc.Graph(
            id='scatter-stories',
        ),

        dcc.Slider(
            id='slider-year',
            min=DF['year'].min(),
            max=DF['year'].max(),
            value=DF['year'].max(),
            marks={str(year): str(year) for year in DF['year'].unique()},
        ),
    ]
)


@APP.callback(
    Output(component_id='scatter-stories', component_property='figure'),
    [Input(component_id='slider-year', component_property='value')]
    )
def update_stories(selected_year):
    """
    update_stories
    """
    filtered_df = DF[DF.year == selected_year]

    traces = []
    for i in filtered_df.type.unique():
        df_by_type = filtered_df[filtered_df['type'] == i]
        traces.append(go.Scatter(
            x=df_by_type['descendants'],
            y=df_by_type['score'],
            text=df_by_type['title'],
            mode='markers',
            opacity=0.7,
            marker={
                'size': 15,
                'line': {'width': 0.5, 'color': 'white'},
                },
            name=i,
        ))

    return {
        'data': traces,
        'layout': go.Layout(
            xaxis={'type': 'log', 'title': 'Comments'},
            yaxis={'title': 'Votes'},
            margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
            showlegend=True,
            legend={'x': 0, 'y': 1},
            hovermode='closest',
            )
        }


if __name__ == '__main__':
    APP.run_server(debug=True, port=4200)
