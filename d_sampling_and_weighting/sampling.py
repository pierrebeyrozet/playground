import pandas as pd
import numpy as np
from pathlib import Path

def get_ind_matrix(bar_index, events):
    ind_m = pd.DataFrame(0, index=bar_index, columns=range(events.shape[0]))
    for i in range(events.shape[0]):
        ind_m.loc[events.index[i]:events.iloc[i], i] = 1
    return ind_m

def get_average_uniqueness(ind_m):
    c = ind_m.sum(axis=1)
    u = ind_m.div(c, axis=0)
    avg = u[u > 0].mean()
    return avg


def sequential_bootstrap(ind_m, s_length=None):
    """
    generate a sample via sequential bootstrap (seems incredibly slow and inefficient
    goal is to take into account the fact that draws according to a changing proba
    to take into account redundancy.
    original paper was just drawing a sample until a specific number of draws
    :param ind_m:
    :param s_length:
    :return:
    """
    if s_length is None:
        s_length = ind_m.shape[1]
    phi = []
    while len(phi) < s_length:
        avg_u = pd.Series()
        for i in ind_m:
            ind_m_ = ind_m[phi+[i]]
            uniq = get_average_uniqueness(ind_m_)
            avg_u.loc[i] = uniq.iloc[-1]
        prob = avg_u/avg_u.sum()
        phi += [np.random.choice(ind_m.columns, p=prob)]
    return phi


def bootstrap2(ind_m, sample_length, num_samples):
    """
    how to generate a sample that is close to iid. issue is that consecutive labels are overlapping,
    as a result uniform sampling means the resulting sample is not iid.
    To get an iid sample, need to modify the proba to try to not take samples that
    are overlapping with the samples that have been already selected
    :param ind_m:
    :param sample_length:
    :param num_samples:
    :return:
    """
    return


if __name__ == "__main__":
    dir_path = Path(__file__).parent.parent.resolve() / 'b_data_refactoring' / 'data'
    data = pd.read_csv(dir_path / 'scaled_adj-CL-30min.csv', index_col='datetime', parse_dates=True)
    dir_path = Path(__file__).parent.parent.resolve() / 'c_labelling' / 'data'
    labels = pd.read_csv(dir_path / 'breakout_up_label.csv', index_col='event_time', parse_dates=True)
    ind_matrix = get_ind_matrix(data.index, labels['outcome_time'])
    avg_uniq = get_average_uniqueness(ind_matrix)
    seqboot = sequential_bootstrap(ind_matrix, 10)
    a = 0