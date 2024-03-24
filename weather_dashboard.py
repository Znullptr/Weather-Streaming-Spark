from dash import dcc
from dash.dependencies import Input, Output
from dash import dash_table
import pandas as pd
import dash
from dash import html
import requests
import time
import plotly
import daemonize

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__,external_stylesheets=external_stylesheets)
app.scripts.config.serve_locally = True
current_refresh_time_temp = None

df = None

api_endpoint = "http://127.0.0.1:5000/wm/v1"
api_response = requests.get(api_endpoint)
records_list = api_response.json()
df = pd.DataFrame(records_list,columns=["CityName","Temperature","Humidity","WindSpeed","WindDirection","CreationTime"])
df['index'] = range(1, len(df) + 1)

PAGE_SIZE = 5


app.layout = html.Div([
    html.H2(
        children="Real-Time Dashboard for Weather Monitoring",
        style={
            "textAlign": "center",
            "color": "#4205F4",
            "font-weight": "bold",
            "font-family": "Verdana"
        }
    ),
    html.Div(
        children="{ An Idle Place To Know Your City's Weather! }",
        style={
            "textAlign": "center",
            "color": "#0F9D58",
            "font-weight": "bold",
            "fontSize": 16
        }
    ),
    html.Br(),
    html.Div(
        id="current_refresh_time",
        children="Current Refresh Time",
        style={
            "textAlign": "center",
            "color": "black",
            "font-weight": "bold",
            "fontSize": 10,
            "font-family": "Verdana"
        }
    ),
    html.Div( 
              [
        html.Div([
            dcc.Graph(id='live-update-graph-bar')
        ], className="three columns"),
        html.Div([
            html.Br(),
            html.Br(),
            html.Br(),
            html.Br(),
            html.Br(),
            html.Br(),
            dash_table.DataTable(
                id="datatable-paging",
                columns=[{"name": i, "id": i} for i in sorted(["CityName", "Temperature", "Humidity", "CreationTime"])],
                page_current=0,
                data=df.to_dict('records'),
                page_size=PAGE_SIZE,
                page_action="custom"
            )
        ], className="three columns")
    ], className='row',style={'alignItems': 'center','justifyContent': 'center','display': 'flex'}),
    dcc.Interval(
        id="interval-component",
        interval=60000,
        n_intervals=0
    )
])

@app.callback(
    Output("current_refresh_time", "children"),
    [Input("interval-component", "n_intervals")]
)

def update_layout(n):
    global current_refresh_time_temp
    current_refresh_time_temp = time.strftime("%Y-%m-%d %H:%M")
    return "Current Refresh Time: {}".format(current_refresh_time_temp)

@app.callback(
    Output("datatable-paging", "data"),
    [Input("datatable-paging", "page_current"),
     Input("datatable-paging", "page_size"),
     Input("interval-component", "n_intervals")]
)

def update_table(page_current, page_size, n):
    global df
    api_endpoint = "http://127.0.0.1:5000/wm/v1"
    api_response = requests.get(api_endpoint)
    records_list = api_response.json()
    df = pd.DataFrame(records_list,columns=["CityName","Temperature","Humidity","WindSpeed","WindDirection","CreationTime"])
    df['index'] = range(1, len(df) + 1)
    start_index = page_current * page_size
    end_index = (page_current + 1) * page_size
    return df.iloc[start_index:end_index].to_dict('records')

@app.callback(
    Output("live-update-graph-bar", "figure"),
    [Input("interval-component", "n_intervals")]
)

def update_graph_bar(n):
    traces = list()
    bar_1 = plotly.graph_objs.Bar(
        x = df['CityName'].head(3),
        y = df['Temperature'].head(3),
        marker=dict(color='red'),
        name = 'Temperature'
    )
    traces.append(bar_1)
    bar_2 = plotly.graph_objs.Bar(
        x = df['CityName'].head(3),
        y = df['Humidity'].head(3),
        marker=dict(color='blue'),
        name = 'Humidity'
    )
    traces.append(bar_2)
    layout = plotly.graph_objs.Layout(
        barmode='group',
        xaxis_tickangle=-45,
        title_text="City's Temperature and Humidity",
        title_font=dict(
            family = 'Verdana',
            size = 12,
            color = 'black'    
        )
    )
    return {'data': traces, 'layout': layout}

def run_app():

     app.run(port=8191)

if __name__=='__main__':

    daemon = daemonize.Daemonize(app="weather_streaming_dash", pid="/tmp/weather_streaming_dash.pid", action=run_app)
    daemon.start()

   
