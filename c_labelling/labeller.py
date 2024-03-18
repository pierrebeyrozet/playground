import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta

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
            section['down'] = section['diff2start']-b_down
            touch_ups = section.loc[section['up'] > 0]
            touch_downs = section.loc[section['down'] < 0]
            if touch_ups.shape[0] > 0 and touch_downs.shape[0] > 0:
                up1st = touch_ups.index[0]
                down1st = touch_downs.index[0]
                if up1st < down1st:
                    labels[up1st] = 1
                else:
                    labels[down1st] = -1
            elif touch_ups.shape[0] > 0 and touch_downs.shape[0] == 0:
                labels[touch_ups.index[0]] = 1
            elif touch_ups.shape[0] == 0 and touch_downs.shape[0] > 0:
                labels[touch_downs.index[0]] = -1
            else:
                if zero_when_timedout:
                    lbl = 0
                else:
                    lbl = np.sign(section['diff2start'].iloc[-1])
                labels[section.index[-1]] = lbl
        return pd.Series(labels)


def breakout_up(df):
    if df['close'].iloc[-1] > df['high'].iloc[:df.shape[0]-1].max():
        return True
    return False


def main():
    dir_path = Path(__file__).parent.parent.resolve() / 'b_data_refactoring' / 'data'
    data = pd.read_csv(dir_path / 'scaled_adj-CL-30min.csv', index_col='datetime', parse_dates=True)
    labeller = Labeller(breakout_up)
    events = labeller.get_events(data, 20)
    labels = labeller.get_triple_barrier_label(data['close'], events, 1, 1, timedelta(days=1))
    return


if __name__ == "__main__":
    main()
