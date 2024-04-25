import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta
import matplotlib.pyplot as plt
import mplfinance as mpf
from c_labelling.catalysts import breakout_up


class Labeller:
    def __init__(self, catalyst_func):
        self.catalyst = catalyst_func

    def get_events(self, data, look_back):
        is_catalyst = []
        for i in range(look_back, data.shape[0]):
            section = data.iloc[i - look_back:i]
            if self.catalyst(section):
                is_catalyst.append(section.index[-1])

        return pd.DatetimeIndex(is_catalyst)

    @staticmethod
    def get_triple_barrier_label(close, events, b_up, b_down, timeout, scaling_ts=None, zero_when_timedout=False):
        labels = dict()
        for e in events:
            section = (close.loc[e: e + timeout] - close.loc[e]).to_frame('diff2start')
            if scaling_ts is None:
                section['up'] = section['diff2start']-b_up
                section['down'] = -section['diff2start']-b_down
            else:
                scaling_section = scaling_ts.loc[e: e + timeout]
                section['up'] = section['diff2start'] - b_up * scaling_section
                section['down'] = -section['diff2start'] - b_down * scaling_section
            touch_ups = section.loc[section['up'] > 0]
            touch_downs = section.loc[section['down'] > 0]
            if touch_ups.shape[0] > 0 and touch_downs.shape[0] > 0:
                up1st = touch_ups.index[0]
                down1st = touch_downs.index[0]
                if up1st < down1st:
                    labels[e] = {'label': 1, 'outcome_time': up1st}
                else:
                    labels[e] = {'label': -1, 'outcome_time': down1st}
            elif touch_ups.shape[0] > 0 and touch_downs.shape[0] == 0:
                labels[e] = {'label': 1, 'outcome_time': touch_ups.index[0]}
            elif touch_ups.shape[0] == 0 and touch_downs.shape[0] > 0:
                labels[e] = {'label': -1, 'outcome_time': touch_downs.index[0]}
            else:
                if zero_when_timedout:
                    lbl = 0
                else:
                    lbl = np.sign(section['diff2start'].iloc[-1])
                labels[e] = {'label': lbl, 'outcome_time': section.index[-1]}
        lbl_frame = pd.DataFrame.from_dict(labels, orient='index')
        lbl_frame.index.name = 'event_time'
        return lbl_frame

    @staticmethod
    def measure_outcome(close, events, timeout):
        #labels = dict()
        post_event_potential_up = []
        for e in events:
            section = (close.loc[e: e + timeout] - close.loc[e])#.to_frame('diff2start')
            post_event_potential_up.append(section.max())
        potentials = pd.Series(post_event_potential_up)
        return potentials


def generate_null_distrib(close, events, length, iter):
    deviation = close.diff(1).std()
    rand_res = []
    for i in range(iter):
        randoms = np.random.randn(len(events), length) * deviation
        trajectories = randoms.cumsum(axis=1)
        rand_potential = trajectories.max(axis=1)

        rand_res.append(np.mean(rand_potential))

    return pd.Series(rand_res)


# def breakout_up(df):
#     if df['close'].iloc[-1] > df['high'].iloc[:df.shape[0]-1].max():
#         return True
#     return False


def label_search(func):
    dir_path = Path(__file__).parent.parent.resolve() / 'b_data_refactoring' / 'data'
    data = pd.read_csv(dir_path / 'scaled_adj-CL-30min.csv', index_col='datetime', parse_dates=True)
    labeller = Labeller(func)
    events = labeller.get_events(data, 20)
    kellys = dict()
    for b_up in range(1, 5):
        for b_down in range(1, 5):
            labels = labeller.get_triple_barrier_label(data['close'], events, b_up, b_down, timedelta(days=50),
                                                       zero_when_timedout=True)
            p_win = labels[labels['label'] > 0].shape[0] / labels[labels['label'] != 0].shape[0]
            p_loss = labels[labels['label'] < 0].shape[0] / labels[labels['label'] != 0].shape[0]
            print(f'bup = {b_up} ; bdown = {b_down} ; positive labels {p_win}')
            kelly = p_win - p_loss / (b_up / b_down)
            print(f'kelly: {kelly}')
            kellys[(b_up, b_down)] = kelly
    kellys_serie = pd.Series(kellys)
    print(kellys_serie)
    print(kellys_serie.argmax())


def get_labels(func):
    dir_path = Path(__file__).parent.parent.resolve() / 'b_data_refactoring' / 'data'
    data = pd.read_csv(dir_path / 'scaled_adj-CL-30min.csv', index_col='datetime', parse_dates=True)
    labeller = Labeller(func)
    events = labeller.get_events(data, 20)

    labels = labeller.get_triple_barrier_label(data['close'], events, 2, 1, timedelta(days=50),
                                                       zero_when_timedout=True)
    otp_name = f'{func.__name__}_label.csv'
    labels.to_csv(Path(__file__).parent / 'data' / otp_name)

    pnl = dict()
    for e, row in labels.iterrows():
        pnl[e] = data.loc[row['outcome_time'], 'close'] - data.loc[e, 'close']

    return pd.Series(pnl)


def investigation(func):
    dir_path = Path(__file__).parent.parent.resolve() / 'b_data_refactoring' / 'data'
    data = pd.read_csv(dir_path / 'scaled_adj-CL-30min.csv', index_col='datetime', parse_dates=True)
    labeller = Labeller(func)
    events = labeller.get_events(data, 20)

    #labels = labeller.get_triple_barrier_label(data['close'], events, 2, 1, timedelta(days=50),
    #                                           zero_when_timedout=True)
    null_distrib_up_potential = generate_null_distrib(data['close'], events, 6, 100)
    up_potential = labeller.measure_outcome(data['close'], events, timedelta(minutes=180))
    average_up_potential = up_potential.mean()

    score = (average_up_potential-null_distrib_up_potential.mean())/null_distrib_up_potential.std()
    #labels_loss = labels[labels['label'] == -1]
    return


def main():
    dir_path = Path(__file__).parent.parent.resolve() / 'b_data_refactoring' / 'data'
    data = pd.read_csv(dir_path / 'scaled_adj-CL-30min.csv', index_col='datetime', parse_dates=True)

    labeller = Labeller(breakout_up)
    events = labeller.get_events(data, 20)

    #chart_signal(data, events)

    vol_scale = data['close'].diff(1).rolling(48).std()*np.sqrt(24)
    labels = labeller.get_triple_barrier_label(data['close'], events, 2, 2, scaling_ts=vol_scale,
                                               timeout=timedelta(days=50),
                                               zero_when_timedout=True)
    chart_signal(data, labels)
    duration = (labels['outcome_time']-labels.index).dt.total_seconds() / 86400
    return

def chart_signal(df, labels):
    upper_bol = df['close'].ewm(span=20).mean() + df['close'].diff(1).ewm(span=20).std() * 2
    win_event = labels.loc[labels['label'] == 1, 'label']
    win_events_price = df.loc[win_event.index, 'close']
    win_events_price = win_events_price.reindex(df.index)

    loss_event = labels.loc[labels['label'] == -1, 'label']
    loss_events_price = df.loc[loss_event.index, 'close']
    loss_events_price = loss_events_price.reindex(df.index)

    win_events_plot = mpf.make_addplot(win_events_price, type='scatter', markersize=50, marker='^', color='green')
    loss_events_plot = mpf.make_addplot(loss_events_price, type='scatter', markersize=50, marker='^', color='red')
    ubol_plot = mpf.make_addplot(upper_bol, type='line', color='blue')
    mpf.plot(df, addplot=[win_events_plot, loss_events_plot, ubol_plot])

    return


if __name__ == "__main__":
    main()
    #label_search(breakout_up)
    #backtest = get_labels(breakout_up)
    #investigation(breakout_up)

    a = 0

