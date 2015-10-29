__author__ = 'mlevi'

import copy
import datetime as dt

import numpy as np
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkstudy.EventProfiler as ep

def find_events(ls_symbols, d_data):
    ''' Finding the event dataframe '''
    df_close = d_data['actual_close']

    print "Finding Events"

    # Creating an empty dataframe
    df_events = copy.deepcopy(df_close)
    df_events = df_events * np.NAN

    # Time stamps for the event range
    ldt_timestamps = df_close.index

    print ldt_timestamps


    for s_sym in ls_symbols:
        for i in range(1, len(ldt_timestamps)):
            # Calculating the returns for this timestamp
            f_sym_price_today = df_close[s_sym].ix[ldt_timestamps[i]]
            f_sym_price_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]

            if f_sym_price_yest >= 5.0 > f_sym_price_today:
                df_events[s_sym].ix[ldt_timestamps[i]] = 1

    return df_events


if __name__ == '__main__':
    dt_start = dt.datetime(2008, 1, 1)
    dt_end = dt.datetime(2009, 12, 31)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

    data_obj = da.DataAccess('Yahoo')
    ls_symbols = data_obj.get_symbols_from_list('sp5002008')
    ls_symbols.append('SPY')

    ls_keys = ['actual_close']
    ldf_data = data_obj.get_data(ldt_timestamps, ls_symbols, ls_keys)

    d_data = dict(zip(ls_keys, ldf_data))
    print d_data

    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)

    df_events = find_events(ls_symbols, d_data)
    print "Creating Study"
    ep.eventprofiler(df_events, d_data, i_lookback=20, i_lookforward=20,
                     s_filename='Homework2.pdf', b_market_neutral=True, b_errorbars=True,
                     s_market_sym='SPY')

