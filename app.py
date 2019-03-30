"""module"""
# -*- coding: utf-8 -*-
import os
import json
import dash
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

# Using WHERE reduces the amount of data scanned / quota used
SQL = """
SELECT title, score
FROM `bigquery-public-data.hacker_news.stories`
WHERE
  NOT (score IS NULL)
ORDER BY score DESC
LIMIT 10
"""

DF = CLIENT.query(SQL).to_dataframe()

DF = DF.head()

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
    style={'backgroundColor': COLORS['background']},
    children=[

        dcc.Markdown(children=MARKDOWN_TEXT),

        dcc.Graph(
            id='TODO',
            figure={
                'data': [
                    go.Bar(
                        x=DF['title'],
                        y=DF['score'],
                        name='TODO',
                    ),
                ],
                'layout': go.Layout(
                    title='TODO',
                    showlegend=True,
                )
            },
        ),

    ]
)

if __name__ == '__main__':
    APP.run_server(debug=True, port=4200)
