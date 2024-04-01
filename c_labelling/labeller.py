import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta
import matplotlib.pyplot as plt


class Labeller:
    def __init__(self, catalyst_func):
        self.catalyst = catalyst_func

    def get_events(self, data, look_back):
        is_catalyst = []
        for i in range(data.shape[0]):
            if i < look_back:
                is_catalyst.append(False)
            else:
                section = data.iloc[i-look_back:i]
                is_catalyst.append(self.catalyst(section))
        is_event_ts = pd.Series(index=data.index, data=is_catalyst)
        return is_event_ts.loc[is_event_ts].index

    @staticmethod
    def get_triple_barrier_label(close, events, b_up, b_down, timeout, vol_scaled=False, zero_when_timedout=False):
        labels = dict()
        for e in events:
            section = (close.loc[e: e + timeout] - close.loc[e]).to_frame('diff2start')
            section['up'] = section['diff2start']-b_up
            section['down'] = section['diff2start']+b_down
            touch_ups = section.loc[section['up'] > 0]
            touch_downs = section.loc[section['down'] < 0]
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

def breakout_up(df):
    if df['close'].iloc[-1] > df['high'].iloc[:df.shape[0]-1].max():
        return True
    return False


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



if __name__ == "__main__":
    #label_search(breakout_up)
    #backtest = get_labels(breakout_up)
    investigation(breakout_up)
    a = 0

