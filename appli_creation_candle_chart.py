from flask import Flask, render_template
import pandas as pd
import plotly
import plotly.graph_objects as go
import json

app = Flask(__name__)

@app.route('/')
def index():
    # Load your dataset
    df = pd.read_csv('glbx-mdp3-20170601-20170630.ohlcv-1m.csv')
    df['ts_event'] = pd.to_datetime(df['ts_event']).dt.strftime('%d-%m-%Y %H:%M')
    df.set_index('ts_event', inplace=True)

    # Create the candlestick chart
    fig = go.Figure(data=[go.Candlestick(x=df.index,
                                         open=df['open'],
                                         high=df['high'],
                                         low=df['low'],
                                         close=df['close'])])

    fig.update_layout(title='Interactive Candlestick Chart',
                      xaxis_title='Date',
                      yaxis_title='Price')

    # Convert the plot to JSON for easy rendering in HTML
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('index.html', graphJSON=graphJSON)

if __name__ == '__main__':
    app.run(debug=True)
