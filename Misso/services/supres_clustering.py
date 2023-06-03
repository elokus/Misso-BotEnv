import pandas as pd
import numpy as np
from sklearn.cluster import AgglomerativeClustering
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def cluster_support_resistance(df, rolling_wave_length: int, num_clusters: int, return_array: bool=False):
    min_waves = _cluster_support_resistance(df, rolling_wave_length, num_clusters, target="min")
    max_waves = _cluster_support_resistance(df, rolling_wave_length, num_clusters, target="max")
    if return_array:
        min_waves = np.array(min_waves.sort_values())
        max_waves = np.array(max_waves.sort_values())
    return min_waves, max_waves

def _cluster_support_resistance(_df: pd.DataFrame, rolling_wave_length: int, num_clusters: int, target: str="min", return_waves: bool=False):
    df = _df.copy()
    date = df.index
    # Reset index for merging
    df.reset_index(inplace=True)
    # Create min aor max waves

    if target == "min":
        min_waves_temp = df.Low.rolling(rolling_wave_length).min().rename('waves')
        min_waves = pd.concat([min_waves_temp, pd.Series(np.zeros(len(min_waves_temp)) + -1)], axis=1)
        min_waves.drop_duplicates('waves', inplace=True)
        waves = min_waves.dropna()
    else:
        max_waves_temp = df.High.rolling(rolling_wave_length).max().rename('waves')
        max_waves = pd.concat([max_waves_temp, pd.Series(np.zeros(len(max_waves_temp)) + 1)], axis=1)
        max_waves.drop_duplicates('waves', inplace=True)
        waves = max_waves.dropna()

    # Find Support/Resistance with clustering using the rolling stats
    # Create [x,y] array where y is always 1
    x = np.concatenate((waves.waves.values.reshape(-1, 1),
                        (np.zeros(len(waves)) + 1).reshape(-1, 1)), axis=1)
    # Initialize Agglomerative Clustering
    cluster = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
    cluster.fit_predict(x)
    waves = waves.assign(clusters=cluster.labels_)
    #waves['clusters'] = cluster.labels_
    # Get index of the max wave for each cluster
    if target == "min":
        waves2 = waves.loc[waves.groupby('clusters')['waves'].idxmin()]
    else:
        waves2 = waves.loc[waves.groupby('clusters')['waves'].idxmax()]
    df.index = date
    waves2.waves.drop_duplicates(keep="first", inplace=True)
    if return_waves:
        return waves2.reset_index().waves, waves
    return waves2.reset_index().waves


def prepare_mm_df(df, check_neutral=False):
    _df = df.copy()
    tx_df = df.copy()
    tx_df = tx_df.set_index(_df.Date)
    n = 10
    for i in _df.index:
        if i < n:
            _df.loc[i, :] = None
            continue
        avg_volume = df.Volume.iloc[i-n:i].sum()/n
        if _df.Volume.loc[i] < avg_volume*2:
            _df.loc[i, :] = None
        else:
            if check_neutral:
                if _df.Open.loc[i] > _df.Close.loc[i]: #short
                    v = {"type":"short", "high": _df.High.loc[i], "low": _df.Low.loc[i], "half": (_df.High.loc[i] + _df.Low.loc[i])/2}
                else:
                    v = {"type":"long", "high": _df.High.loc[i], "low": _df.Low.loc[i], "half": (_df.High.loc[i] + _df.Low.loc[i])/2}
                if is_mm_neutralized(tx_df, _df.Date.loc[i], v):
                    _df.loc[i, :] = None
    return _df

def prepare_pva_plot(symbol, df, waves):
    df_neutral = prepare_mm_df(df, check_neutral=False)
    df_open = prepare_mm_df(df, check_neutral=True)
    fig = plot_mm_chart(symbol, df, df_neutral, support_resistance_levels=waves, df_neutral=df_open)
    return fig

def find_unrecovered_mm(df: pd.DataFrame, vol_factor: int=2, n: int=10, filter_type: str=None, return_last: bool=False):
    d = find_mm_candles(df, vol_factor, n)
    d = are_mm_neutralized(df, d)
    d = are_mm_triggered(df, d)

    mms = []
    for tx, v in d.items():
        if filter_type is not None and v["type"] != filter_type:
            continue  #skips element
        if not v["neutral"]:
            v["tx"] = tx
            mms.append(v)
    if len(mms) == 0:
        return None
    return mms[-1] if return_last else mms

def find_mm_candles(df, vol_factor=2, n=10, ):
    d = {}
    for i in df.index:
        if i < n:
            continue
        avg_volume = df.Volume.iloc[i-n:i].sum()/n
        if df.Volume.loc[i] > avg_volume*vol_factor:
            if df.Open.loc[i] > df.Close.loc[i]: #short
                d[df.Date.loc[i]] = {"type":"short", "high": df.High.loc[i], "low": df.Low.loc[i], "half": (df.High.loc[i] + df.Low.loc[i])/2, "target": df.High.loc[i],"triggered":False, "neutral":False}
            else:
                d[df.Date.loc[i]] = {"type":"long", "high": df.High.loc[i], "low": df.Low.loc[i], "half": (df.High.loc[i] + df.Low.loc[i])/2, "target": df.Low.loc[i],"triggered":False, "neutral":False}
    return d

def is_mm_neutralized(df, tx, v):
    if v["type"] == "short":
        if v["high"] <= df.High.loc[tx:].iloc[1:].max():
            return True
        return False
    else:
        if v["low"] >= df.Low.loc[tx:].iloc[1:].min():
            return True
        return False

def is_mm_triggered(df, tx, v):
    if v["type"] == "short":
        if v["half"] <= df.High.loc[tx:].iloc[1:].max():
            return True
        return False
    else:
        if v["half"] >= df.Low.loc[tx:].iloc[1:].min():
            return True
        return False

def are_mm_neutralized(df: pd.DataFrame, mm_candles: dict):
    _df = df.copy()
    _df = _df.set_index(_df.Date)

    for tx, v in mm_candles.items():
        mm_candles[tx]["neutral"] = is_mm_neutralized(_df, tx, v)
    return mm_candles

def are_mm_triggered(df: pd.DataFrame, mm_candles: dict, not_neutral_only: bool=True):
    _df = df.copy()
    _df = _df.set_index(_df.Date)

    for tx, v in mm_candles.items():
        if not_neutral_only and v["neutral"]:
            continue
        mm_candles[tx]["triggered"] = is_mm_triggered(_df, tx, v)
    return mm_candles

        # if v["type"] == "short":
        #     if v["high"] <= _df.High.loc[tx:].iloc[1:].max():
        #         mm_candles[tx]["neutral"] = True
        # else:
        #     if v["low"] >= _df.Low.loc[tx:].iloc[1:].min():
        #         mm_candles[tx]["neutral"] = True


# def plot_mm_chart(symbol, df, _df, support_resistance_levels, df_neutral=None):
def plot_mm_chart(symbol, df, support_resistance_levels, plot_pvas=False):

    _df = prepare_mm_df(df, check_neutral=False)
    df_neutral = prepare_mm_df(df, check_neutral=True)
    light_palette = {}
    light_palette["bg_color"] = "#ffffff"
    light_palette["plot_bg_color"] = "#ffffff"
    light_palette["grid_color"] = "#e6e6e6"
    light_palette["text_color"] = "#2e2e2e"
    light_palette["dark_candle"] = "#4d98c4"
    light_palette["light_candle"] = "#b1b7ba"
    light_palette["volume_color"] = "#c74e96"
    light_palette["border_color"] = "#2e2e2e"
    light_palette["open_mm_up_candle"] = "#18BF64"
    light_palette["open_mm_down_candle"] = "#AA47D2"
    light_palette["neutral_mm_candle"] = "#141615"
    light_palette["color_1"] = "#5c285b"
    light_palette["color_2"] = "#802c62"
    light_palette["color_3"] = "#a33262"
    light_palette["color_4"] = "#c43d5c"
    light_palette["color_5"] = "#de4f51"
    light_palette["color_6"] = "#f26841"
    light_palette["color_7"] = "#fd862b"
    light_palette["color_8"] = "#ffa600"
    light_palette["color_9"] = "#3366d6"
    palette = light_palette
    #  Array of colors for support/resistance lines
    support_resistance_colors = ["#5c285b", "#802c62", "#a33262", "#c43d5c", "#de4f51","#f26841", "#fd862b", "#ffa600","#3366d6"]
    #  Create sub plots
    fig = go.Figure()
    fig = make_subplots(rows=1, cols=1, subplot_titles=[f"{symbol} Chart"],
                        specs=[[{"secondary_y": False}]],
                        vertical_spacing=0.04, shared_xaxes=True)
    #  Add legend with the support/resistance prices
    support_resistance_prices = ""
    for level in support_resistance_levels.to_list():
        support_resistance_prices += "$ {:.2f}".format(level) + "<br>"
    fig.add_annotation(text=support_resistance_prices,
                       align='left',
                       showarrow=False,
                       xref='paper',
                       yref='paper',
                       x=1.0,
                       y=0.9,
                       bordercolor='black',
                       borderwidth=1)
    #  Plot close price
    fig.add_trace(go.Candlestick(x=df.Date,
                                 open=df['Open'],
                                 close=df['Close'],
                                 low=df['Low'],
                                 high=df['High'],
                                 increasing_line_color=palette['light_candle'],
                                 decreasing_line_color=palette['dark_candle'], name='Price'), row=1, col=1)
    if plot_pvas:
        fig.add_trace(go.Candlestick(x=df.Date,
                                     open=_df['Open'],
                                     close=_df['Close'],
                                     low=_df['Low'],
                                     high=_df['High'],
                                     increasing_line_color=palette['neutral_mm_candle'],
                                     decreasing_line_color=palette['neutral_mm_candle'], name='PVA neutral'), row=1, col=1)

        fig.add_trace(go.Candlestick(x=df.Date,
                                     open=df_neutral['Open'],
                                     close=df_neutral['Close'],
                                     low=df_neutral['Low'],
                                     high=df_neutral['High'],
                                     increasing_line_color=palette['open_mm_up_candle'],
                                     decreasing_line_color=palette['open_mm_down_candle'], name='PVA open'), row=1, col=1)

    #  Add Support/Resistance levels
    i = 0
    for level in support_resistance_levels.to_list():
        line_color = support_resistance_colors[i] if i < len(support_resistance_colors) else support_resistance_colors[0]
        fig.add_hline(y=level, line_width=1, line_dash="dash", line_color=line_color, row=1, col=1)
        i += 1
    fig.update_layout(
        title={'text': '', 'x': 0.5},
        font=dict(family="Verdana", size=12, color=palette["text_color"]),
        autosize=True,
        width=1280, height=720,
        xaxis={"rangeslider": {"visible": False}},
        plot_bgcolor=palette["plot_bg_color"],
        paper_bgcolor=palette["bg_color"])
    fig.update_yaxes(visible=False, secondary_y=True)
    #  Change grid color
    fig.update_xaxes(showline=True, linewidth=1, linecolor=palette["grid_color"], gridcolor=palette["grid_color"])
    fig.update_yaxes(showline=True, linewidth=1, linecolor=palette["grid_color"], gridcolor=palette["grid_color"])
    return fig


# def old_plot_chart(symbol, df, support_resistance_levels):
#     import plotly.graph_objects as go
#     from plotly.subplots import make_subplots
#     light_palette = {}
#     light_palette["bg_color"] = "#ffffff"
#     light_palette["plot_bg_color"] = "#ffffff"
#     light_palette["grid_color"] = "#e6e6e6"
#     light_palette["text_color"] = "#2e2e2e"
#     light_palette["dark_candle"] = "#4d98c4"
#     light_palette["light_candle"] = "#b1b7ba"
#     light_palette["volume_color"] = "#c74e96"
#     light_palette["border_color"] = "#2e2e2e"
#     light_palette["color_1"] = "#5c285b"
#     light_palette["color_2"] = "#802c62"
#     light_palette["color_3"] = "#a33262"
#     light_palette["color_4"] = "#c43d5c"
#     light_palette["color_5"] = "#de4f51"
#     light_palette["color_6"] = "#f26841"
#     light_palette["color_7"] = "#fd862b"
#     light_palette["color_8"] = "#ffa600"
#     light_palette["color_9"] = "#3366d6"
#     palette = light_palette
#     #  Array of colors for support/resistance lines
#     support_resistance_colors = ["#5c285b", "#802c62", "#a33262", "#c43d5c", "#de4f51","#f26841", "#fd862b", "#ffa600","#3366d6"]
#     #  Create sub plots
#     fig = make_subplots(rows=1, cols=1, subplot_titles=[f"{symbol} Chart",],
#                         specs=[[{"secondary_y": False}]],
#                         vertical_spacing=0.04, shared_xaxes=True)
#     #  Add legend with the support/resistance prices
#     support_resistance_prices = ""
#     for level in support_resistance_levels.to_list():
#         support_resistance_prices += "$ {:.2f}".format(level) + "<br>"
#     fig.add_annotation(text=support_resistance_prices,
#                        align='left',
#                        showarrow=False,
#                        xref='paper',
#                        yref='paper',
#                        x=1.0,
#                        y=0.9,
#                        bordercolor='black',
#                        borderwidth=1)
#     #  Plot close price
#     fig.add_trace(go.Candlestick(x=df.index,
#                                  open=df['Open'],
#                                  close=df['Close'],
#                                  low=df['Low'],
#                                  high=df['High'],
#                                  increasing_line_color=palette['light_candle'],
#                                  decreasing_line_color=palette['dark_candle'], name='Price'), row=1, col=1)
#     #  Add Support/Resistance levels
#     i = 0
#     for level in support_resistance_levels.to_list():
#         line_color = support_resistance_colors[i] if i < len(support_resistance_colors) else support_resistance_colors[0]
#         fig.add_hline(y=level, line_width=1, line_dash="dash", line_color=line_color, row=1, col=1)
#         i += 1
#     fig.update_layout(
#         title={'text': '', 'x': 0.5},
#         font=dict(family="Verdana", size=12, color=palette["text_color"]),
#         autosize=True,
#         width=1280, height=720,
#         xaxis={"rangeslider": {"visible": False}},
#         plot_bgcolor=palette["plot_bg_color"],
#         paper_bgcolor=palette["bg_color"])
#     fig.update_yaxes(visible=False, secondary_y=True)
#     #  Change grid color
#     fig.update_xaxes(showline=True, linewidth=1, linecolor=palette["grid_color"], gridcolor=palette["grid_color"])
#     fig.update_yaxes(showline=True, linewidth=1, linecolor=palette["grid_color"], gridcolor=palette["grid_color"])
#     return fig



## Clustering with max and min level concatenated:


# def cluster_support_resistance(df, rolling_wave_length, num_clusters, target="min"):
#     date = df.index
#     # Reset index for merging
#     df.reset_index(inplace=True)
#     # Create min and max waves
#     max_waves_temp = df.High.rolling(rolling_wave_length).max().rename('waves')
#     min_waves_temp = df.Low.rolling(rolling_wave_length).min().rename('waves')
#     max_waves = pd.concat([max_waves_temp, pd.Series(np.zeros(len(max_waves_temp)) + 1)], axis=1)
#     min_waves = pd.concat([min_waves_temp, pd.Series(np.zeros(len(min_waves_temp)) + -1)], axis=1)
#     #  Remove dups
#     max_waves.drop_duplicates('waves', inplace=True)
#     min_waves.drop_duplicates('waves', inplace=True)
#     #  Merge max and min waves
#     #waves = max_waves.append(min_waves).sort_index()
#     waves = pd.concat([max_waves, min_waves]).sort_index()
#     waves = waves[waves[0] != waves[0].shift()].dropna()
#     # Find Support/Resistance with clustering using the rolling stats
#     # Create [x,y] array where y is always 1
#     x = np.concatenate((waves.waves.values.reshape(-1, 1),
#                         (np.zeros(len(waves)) + 1).reshape(-1, 1)), axis=1)
#     # Initialize Agglomerative Clustering
#     cluster = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
#     cluster.fit_predict(x)
#     waves['clusters'] = cluster.labels_
#     # Get index of the max wave for each cluster
#     if target == "min":
#         waves2 = waves.loc[waves.groupby('clusters')['waves'].idxmin()]
#     else:
#         waves2 = waves.loc[waves.groupby('clusters')['waves'].idxmax()]
#     df.index = date
#     waves2.waves.drop_duplicates(keep="first", inplace=True)
#     return waves2.reset_index().waves, waves



if __name__ == '__main__':
    from dash import Dash, dcc, html, Input, Output
    import pandas as pd
    import Misso.services.helper as mh
    import Misso.services.async_helper as ah
    import numpy as np
    import asyncio


    ftx = mh.initialize_exchange_driver("HFT", init_async=True)


    limit = 1000
    range_width = 500
    cluster = 4
    since=None
    symbols = ["BTC/USD:USD", "ETH/USD:USD", "CEL/USD:USD-220930"]
    timeframes = ["1m", "15m", "1h", "4h"]

    symbols_tx = {}
    for s in symbols:
        for t in timeframes:
            symbols_tx[s+"_"+ t] = (s, t)
    data = asyncio.get_event_loop().run_until_complete(ah.get_ohlcv_data_multi_timeframe(ftx, symbols, limit, close=True, timeframes=timeframes, since=since))

    ### single market
    # symbol = "BTC/USD:USD"
    # tm = "1h"
    # data = asyncio.run(ah.get_ohlcv_data(ftx, symbol, tm, limit, close=True))
    # df = mh.candles_to_df(data["BTC/USD:USD"])



    #  Run Dash server
    app = Dash(__name__)
    app.layout = html.Div([
        html.H2('Support/Resistance Levels'),
        dcc.Slider(0, limit-range_width-1,1, value=0, id='slider'),
        dcc.Dropdown(
            id='wave_type',
            options=[{'label':'Swing Waves Max', 'value': "max"},
                     {'label':'Swing Waves Min', 'value': "min"}],
            clearable=False,
            value='max'
        ),
        # dcc.Dropdown(
        #     id='symbol_timeframe',
        #     options=[{'label':i, 'value': i} for i in list(symbols_tx.keys())],
        #     clearable=True,
        #     value=list(symbols_tx.keys())[0]
        # ),
        dcc.Dropdown(
            id='symbols',
            options=[{'label':i, 'value': i} for i in symbols],
            clearable=False,
            value=symbols[0]
        ),
        dcc.Dropdown(
            id='timeframe',
            options=[{'label':i, 'value': i} for i in timeframes],
            clearable=False,
            value=timeframes[0]
        ),
        dcc.Graph(id="graph")
    ])
    @app.callback(
        Output("graph", "figure"),
        Input("slider", "value"),
        #Input("symbol_timeframe", "value"),
        Input("timeframe", "value"),
        Input("symbols", "value"),
        Input("wave_type", "value"))
    def display_chart(selected_slide, selected_timeframe, selected_symbol, selected_waves): #selected_symbol_tx,
        symbol, timeframe = selected_symbol, selected_timeframe #symbols_tx[selected_symbol_tx]
        # step = int((limit-range_width)/45)
        # start = selected_slide * step
        # end = start + range_width
        step = int((limit-range_width)/45)
        start = selected_slide
        end = start + range_width
        df = mh.candles_to_df(data[symbol][timeframe]).iloc[start:end,:]
        min_waves, max_waves = cluster_support_resistance(df, 40, 4)
        waves = min_waves if selected_waves == "min" else max_waves

        fig = plot_mm_chart(symbol, df, waves)
        #fig = prepare_pva_plot(symbol, df, waves)
        return fig
    # app.run_server(debug=True)
    app.run_server(port = 8050, debug=True)