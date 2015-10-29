__author__ = 'mlevi'

import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da

import datetime as dt
import numpy as np


def main():
    ''' Main Function'''
    symbols = ['AAPL', 'GLD', 'GOOG', 'XOM']
    allocations = [0.0, 0.0, 0.0, 1.0]

    start_date = dt.datetime(2011, 1, 1)
    end_date = dt.datetime(2011, 12, 31)

    volatility, daily_return, sharpe_ratio, cumulative_return = simulate(start_date, end_date, symbols, allocations)

    print_simulate(allocations, cumulative_return, daily_return, sharpe_ratio, symbols, volatility)

    max_sharpe, optimal_alloc = optimizer(end_date, start_date, symbols)

    print_optimizer(max_sharpe, optimal_alloc)


def optimizer(end_date, start_date, symbols):
    max_sharpe = -1
    optimal_alloc = []
    for i in range(0, 11):
        for j in range(0, 11 - i):
            for k in range(0, 11 - i - j):
                l = 10 - i - j - k
                if (i + j + k + l) == 10:
                    allocations = [i / 10.0, j / 10.0, k / 10.0, l / 10.0]
                    std, d_ret, sharpe_ratio, c_ret = simulate(start_date, end_date, symbols, allocations)
                    if sharpe_ratio > max_sharpe:
                        max_sharpe = sharpe_ratio
                        optimal_alloc = allocations
    return max_sharpe, optimal_alloc


def print_optimizer(max_sharpe, optimal_alloc):
    print "OPTIMIZER RESULTS:"
    print "max sharpe ratio: %s" % max_sharpe
    print "optimal allocation: %s" % optimal_alloc


def print_simulate(allocations, cumulative_return, daily_return, sharpe_ratio, symbols, volatility):
    print "symbols %s" % symbols
    print "allocation %s" % allocations
    print "volatility (standard deviation): %s" % volatility
    print "daily_return: %s" % daily_return
    print "sharpe_ratio: %s" % sharpe_ratio
    print "cumulative_return: %s" % cumulative_return


def simulate(start_date, end_date, symbols, allocations):
    daily_returns = get_daily_returns(end_date, start_date, symbols)

    daily_portfolio_returns = np.dot(daily_returns, allocations)

    average_daily_return_of_portfolio = get_avg_daily_portfolio_return(daily_portfolio_returns)

    cumulative_portfolio_return = np.cumprod(daily_portfolio_returns + 1)[-1]

    standard_deviation = get_standard_deviation(daily_portfolio_returns)

    sharpe_ratio = get_sharpe_ratio(daily_portfolio_returns)

    return standard_deviation, average_daily_return_of_portfolio, sharpe_ratio, cumulative_portfolio_return


def get_avg_daily_portfolio_return(daily_portfolio_returns):
    return np.mean(daily_portfolio_returns)


def get_sharpe_ratio(daily_returns, n=252):
    return np.sqrt(n) * get_avg_daily_portfolio_return(daily_returns) / get_standard_deviation(daily_returns)


def get_standard_deviation(daily_portfolio_returns):
    return np.std(daily_portfolio_returns)


def get_daily_returns(end_date, start_date, symbols):
    hours_in_a_day = dt.timedelta(hours=16)
    ldt_timestamps = du.getNYSEdays(start_date, end_date, hours_in_a_day)
    source = da.DataAccess('Yahoo', cachestalltime=0)
    ls_keys = ['close']
    ldf_data = source.get_data(ldt_timestamps, symbols, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))

    normalize_price = d_data['close'].values
    na_normalized_price = normalize_price / normalize_price[0, :]

    return tsu.returnize0(na_normalized_price)


if __name__ == '__main__':
    main()
