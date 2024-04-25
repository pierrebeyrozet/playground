import mplfinance as mpf
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta
import matplotlib.pyplot as plt
from c_labelling.labeller import Labeller
from c_labelling.catalysts import breakout_up


def main():
    dir_path = Path(__file__).parent.parent.resolve() / 'b_data_refactoring' / 'data'
    data = pd.read_csv(dir_path / 'scaled_adj-CL-30min.csv', index_col='datetime', parse_dates=True)

    labeller = Labeller(breakout_up)
    events = labeller.get_events(data, 20)

    vol_scale = data['close'].diff(1).rolling(48).std()*np.sqrt(24)
    labels = labeller.get_triple_barrier_label(data['close'], events, 2, 2, scaling_ts=vol_scale,
                                               timeout=timedelta(days=50),
                                               zero_when_timedout=True)
    chart_signal(data, labels)
    #duration = (labels['outcome_time']-labels.index).dt.total_seconds() / 86400
    return


def breakout_count(df, win_count, breakout_win):
    rolling_max_t1 = df['high'].rolling(breakout_win).max().shift(1)
    break_up = df['close'] > rolling_max_t1
    break_up_int = break_up.astype(int)
    break_up_count = break_up_int.rolling(win_count).sum()
    break_up_count.name = 'breakout_count'
    return break_up_count


def calculate_indicators(df):
    break_up_counter = breakout_count(df, 20,20)
    upper_bol = df['close'].ewm(span=20).mean() + df['close'].diff(1).ewm(span=20).std() * 2
    upper_bol.name = 'ubol'
    i_frame = pd.concat([df, upper_bol, break_up_counter], axis=1)

    dir_path = Path(__file__).parent.resolve() / 'data'
    i_frame.to_csv(dir_path / 'features.csv')
    return i_frame


def chart_signal(frame, labels):
    df = calculate_indicators(frame)
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
