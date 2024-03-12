import pandas as pd
import os
from datetime import timedelta, datetime

class DataRefactorer:
    def __init__(self, ct_f_path, sample_freq=5, days_to_expi=1):
        self.contracts_file_path = ct_f_path
        self.contracts_frame = pd.read_csv(self.contracts_file_path, parse_dates=True, index_col='expi_date').sort_index()
        self.sample_freq = sample_freq
        self.days_to_expi = days_to_expi
        self.schedule = pd.DataFrame()

    def create_schedule(self):
        schedule_dict = dict()
        prev_roll_date = datetime(2099, 1, 1)
        for i in range(0, self.contracts_frame.shape[0]):
            expi = self.contracts_frame.index[i]
            roll_date = expi - pd.tseries.offsets.BusinessDay(self.days_to_expi) + timedelta(minutes=14 * 60 + 25)
            if i == 0:
                local_start_date = (roll_date - timedelta(30)).replace(hour=2, minute=0)
            else:
                local_start_date = prev_roll_date #  + timedelta(minutes=1)
            schedule_dict[expi] = {'local_symbol': self.contracts_frame['localSymbol'].iloc[i],
                                   'symbol': self.contracts_frame['symbol'].iloc[i],
                                   'local_sdate': local_start_date,
                                   'roll_date': roll_date}
            prev_roll_date = roll_date
        self.schedule = pd.DataFrame.from_dict(schedule_dict, orient='index')
        return

    def create_roll_adjusted_time_serie(self):
        count = 0
        full_bar_list = []
        adjustments = []
        adj = 0
        for idx, row in self.schedule.iterrows():
            dir_path = r'C:\Users\pierr\PycharmProjects\playground\a_ib_connections\data'
            fname = f"{row['symbol']}-{row['local_symbol']}.csv"
            temp_df = self.get_clean_bars(os.path.join(dir_path, fname))
            temp_df = temp_df.loc[row['local_sdate']:row['roll_date']]
            if temp_df.shape[0] == 0:
                break
            if count == 0:
                full_bar_list.append(temp_df)
                count += 1
            else:
                adj = full_bar_list[-1]['close'].iloc[-1]-temp_df['close'].iloc[0]
                temp_df[['open', 'high', 'low', 'close']] = temp_df[['open', 'high', 'low', 'close']] + adj
                full_bar_list.append(temp_df.iloc[1:])
                adjustments.append(pd.Series(index=[temp_df.index[1],], data=adj))

        adjustments_ts = pd.concat(adjustments, axis=0).sort_index()
        adjusted_bars = pd.concat(full_bar_list, axis=0).sort_index()

        return adjusted_bars, adjustments_ts

    def get_clean_bars(self, f_fullname):
        temp_df = pd.read_csv(f_fullname, index_col='datetime', parse_dates=True).sort_index()
        temp_df = temp_df[temp_df['volume'] > 0]
        duplicated_index = temp_df.index[temp_df.index.duplicated(keep=False)]
        if len(duplicated_index) > 0:
            print('ERROR. DUPLICATED BAR')
            return
        return temp_df

    @staticmethod
    def resample_bars(bars, freq):
        o = bars['open'].resample(freq).first()
        h = bars['high'].resample(freq).max()
        l = bars['low'].resample(freq).min()
        c = bars['close'].resample(freq).last()
        count = bars['bar_count'].resample(freq).sum()
        vlu = bars['volume'].resample(freq).sum()
        avg = bars['average'].resample(freq).mean()
        resampled_bars = pd.concat([o, h, l, c, count, vlu, avg], axis=1,
                                   keys=['open', 'high', 'low', 'close', 'bar_count', 'volume', 'average'])
        return resampled_bars.dropna()


def main():
    ticker = 'CL'
    ct_file_path = rf'C:\Users\pierr\PycharmProjects\playground\a_ib_connections\data\{ticker}-contracts.csv'
    ra = DataRefactorer(ct_file_path)
    ra.create_schedule()
    adj_bars, adjustments = ra.create_roll_adjusted_time_serie()
    bar_freq = '30min'
    new_bars = ra.resample_bars(adj_bars, freq=bar_freq)
    new_bars.to_csv(rf"data\adj-{ticker}-{bar_freq}.csv")
    return

if __name__ == "__main__":
    main()