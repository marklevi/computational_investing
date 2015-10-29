__author__ = 'mlevi'

import sys
import csv
import datetime as dt
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.DataAccess as da
import pandas as pd


def main(argv):
    orders_file = argv[0]
    values_file = argv[1]

    symbols = []
    dates = []
    order_file = []

    reader = csv.reader(open(orders_file, 'rU'), delimiter=",")
    for row in reader:
        order_file.append(row)
        symbols.append(row[3])
        dates.append(map(int, row[:3]))

    symbols.append("_CASH")
    uniq_sym = sorted(list(set(symbols)))

    dt_start = dt.datetime(dates[0][0], dates[0][1], dates[0][2])
    dt_end = dt.datetime(dates[-1][0], dates[-1][1], dates[-1][2])
    dt_end_read = dt_end + dt.timedelta(days=1)

    data_obj = da.DataAccess('Yahoo')
    ls_keys = ['close', 'actual_close']
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end_read, dt.timedelta(hours=16))

    ldf_data = data_obj.get_data(ldt_timestamps, uniq_sym, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))

    trades_data = pd.DataFrame(index=list(ldt_timestamps), columns=list(uniq_sym))
    curr_stocks = dict()

    for sym in uniq_sym:
        curr_stocks[sym] = 0
        trades_data[sym][ldt_timestamps[0]] = 0

    curr_cash = 1000000
    trades_data["_CASH"][ldt_timestamps[0]] = curr_cash

    for index, row in enumerate(order_file):
        curr_date = dt.datetime(dates[index][0], dates[index][1], dates[index][2], 16)
        sym = row[3]
        stock_value = int(d_data['close'][sym][curr_date])
        quantity = int(row[5])
        position = row[4]

        if position == "Buy":
            curr_cash -= stock_value * quantity
            trades_data["_CASH"][curr_date] = curr_cash
            curr_stocks[sym] += quantity
            trades_data[sym][curr_date] = curr_stocks[sym]

        else:
            curr_cash += stock_value * quantity
            trades_data["_CASH"][curr_date] = curr_cash
            curr_stocks[sym] -= quantity
            trades_data[sym][curr_date] = curr_stocks[sym]

    trades_data = trades_data.fillna(method="pad")

    writer = csv.writer(open(values_file, 'wb'), delimiter=',')

    for curr_date in trades_data.index:
        value_of_portfolio = 0

        for sym in uniq_sym:
            if sym == "_CASH":
                value_of_portfolio += trades_data[sym][curr_date]
            else:
                value_of_portfolio += trades_data[sym][curr_date] * int(d_data['close'][sym][curr_date])

        writer.writerow([curr_date, value_of_portfolio])


if __name__ == '__main__':
    main(sys.argv[1:])
