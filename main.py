import timeit as ti
from datetime import datetime
import dask
import dask.dataframe as dd
from dask.distributed import Client
from dask import delayed
from dask import compute
import pandas as pd
import os
from glob import glob
import numpy as np
from functools import lru_cache
import plotly.express as px


@lru_cache()
def correct_time_cols(time):
    if time == '2400':
        time = '2359'
    if len(time) != 4:
        time = ''.join(('000', time))
        time = time[-4:]
    return time


# exit()

# return datetime.strptime(time, '%H%M').time()
# return pd.to_datetime(time, format='%H%M').time()


@lru_cache(maxsize=1024)
def reformat_time(time: str) -> datetime.time:
    try:
        return datetime.strptime(time, '%H%M').time()
    except ValueError as ve:
        if time == '2400':
            return datetime.strptime('2359', '%H%M').time()
        elif len(time) == 1:
            time = ''.join(('0', time))
            return datetime.strptime(time, '%H%M').time()
        else:
            print(ve)


def demo_ops_dask(df: dd.DataFrame) -> dd.DataFrame:
    """function with demo tasks using dask"""
    # departure delays avg
    dep_del_mean = df['DepDelay'].mean()
    # arrival delays avg
    # arr_del_mean = df['ArrDelay'].mean()
    # avg arrival delays by Origin of flight
    origin_delay = df.groupby('Origin')['DepDelay'].mean()
    # the day with lowest avg delay
    day_delay = df.groupby('DayOfWeek')['DepDelay'].mean()

    # compute the results in parallel, might need some modification based on the size of data and machines memory
    # mean_delay, dep_del_mean, day_delay = compute(mean_delay, dep_del_mean, day_delay)
    day_delay, dep_del_mean = compute(day_delay, dep_del_mean)
    # dep_del_mean, = compute(dep_del_mean)
    origin_delay, = compute(origin_delay)
    # dep_del_mean, arr_del_mean = compute(dep_del_mean, arr_del_mean)

    # control prints
    print("Avg dep delay for JFK is {}".format(origin_delay['JFK']))
    print("Avg dep delay is {}".format(dep_del_mean))
    # print("Avg arr delay is {}".format(arr_del_mean))
    print("Avg day delays:\n{}".format(day_delay.sort_values(ascending=False)))
    return df


def demo_ops_pandas(df: pd.DataFrame) -> list:
    """function with demo tasks using pandas"""
    dep_group = df.groupby('Origin')
    result = []
    # data for JFK airport departure delay avg
    result.append(dep_group['DepDelay'].count()['JFK'])
    result.append(dep_group['DepDelay'].sum()['JFK'])
    # data for avg departure delay
    result.append(df['DepDelay'].count())
    result.append(df['DepDelay'].sum())
    day_group = df.groupby('DayOfWeek')
    result.append(day_group['DepDelay'].sum().tolist())
    result.append(day_group['DepDelay'].count().tolist())

    return result


def pandas_task():
    start_time = ti.default_timer()
    file = os.path.join('data', 'DataExpo2009', '1987.csv')
    df = pd.read_csv(file)
    demo_ops_pandas(df)

    print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))


def pandas_more_files():
    # start_time = ti.default_timer()

    files = glob(os.path.join('data', 'DataExpo2009', 'years_to_compute', '2008.csv'))
    # print("pandas files: {}".format(files))
    # file_count = len(files)
    cols = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepDelay', 'CRSDepTime', 'Origin', 'Dest',
            'ArrDelay']
    types_dict = {'CancellationCode': 'object',
                  'CRSDepTime': 'string',
                  'Year': 'string',
                  'Month': 'string',
                  'DayofMonth': 'string'}
    # lists for intermediate results when working over multiple files
    jfk_cnt, jfk_sum, del_cnt, del_sum, day_sums, day_counts = [], [], [], [], [], [],
    day_result_sum, day_result_cnt = [0 for _ in range(7)], [0 for _ in range(7)]
    day_results = {'day': [], 'avgDelay': []}
    for file in files:
        # print("reading file {}".format(file))
        res = pd.read_csv(file, encoding_errors='replace', usecols=cols,
                          dtype=types_dict)
        # res['CRSDepTime'] = res['CRSDepTime'].apply(reformat_time)
        res = demo_ops_pandas(res)

        jfk_cnt.append(res[0])
        jfk_sum.append(res[1])
        if len(res) > 2:
            del_cnt.append(res[2])
            del_sum.append(res[3])
            day_sums.append(res[4])
            day_counts.append(res[5])
    print("Average departure delay for JFK is {}".format(sum(jfk_sum) / sum(jfk_cnt)))
    if len(del_cnt):
        print("Average departure delay of all flights is {}".format(sum(del_sum) / sum(del_cnt)))
    # calculate avg departure delay for each day
    for sums in day_sums:
        for idx, day in enumerate(sums):
            day_result_sum[idx] += day
    for counts in day_counts:
        for idx, day in enumerate(counts):
            day_result_cnt[idx] += day
    counter = 1
    for sums, counts in zip(day_result_sum, day_result_cnt):
        day_results['day'].append(counter)
        day_results['avgDelay'].append(sums / counts)
        counter += 1
    day_results = pd.DataFrame(day_results)
    day_results.set_index('day', inplace=True)
    fig = px.bar(day_results)
    fig.show()
    # todo enhance the graph, add the graph to other results, push to github and create readme.md
    # print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))
    # print(reformat_time.cache_info())


def dask_main():
    # start_time = ti.default_timer()
    files = os.path.join('data', 'DataExpo2009', 'years_to_compute', '*.csv')
    cols = ['Year', 'Month', 'DayofMonth', 'Month', 'DayOfWeek', 'DepDelay', 'CRSDepTime', 'Origin', 'Dest',
            'ArrDelay']
    col_types = {'ActualElapsedTime': 'float64',
                 'ArrDelay': 'float64',
                 'ArrTime': 'float64',
                 'DepDelay': 'float64',
                 'DepTime': 'float64',
                 'Distance': 'float64',
                 'CRSElapsedTime': 'float64',
                 'CancellationCode': 'object',
                 'TailNum': 'object',
                 'AirTime': 'float64',
                 'TaxiIn': 'float64',
                 'TaxiOut': 'float64',
                 'CRSDepTime': 'string'
                 }
    # df = dd.read_csv(files, encoding_errors='replace', parse_dates=[[0, 1, 2]], dtype=col_types, usecols=cols,
    #                  infer_datetime_format=True)
    # df = df.rename(columns={'Year_Month_DayofMonth': 'Date'})

    df = dd.read_csv(files, encoding_errors='replace', dtype=col_types, usecols=cols)
    # df['CRSDepTime'] = df['CRSDepTime'].apply(reformat_time, meta='datetime.time')
    # print(df['CRSDepTime'].head())

    # df['Date'] = (pd.to_datetime(df['Year'].astype(str) + '-' +
    #                              df['Month'].astype(str) + '-' +
    #                              df['DayofMonth'].astype(str) + ' ' +
    #                              df['CRSDepTime'].apply(reformat_time, meta=('CRSDepTime', 'string'))
    #                              )
    #               )

    # call demo functions on the dataframe
    demo_ops_dask(df)
    # print("Dask took {} seconds.".format(ti.default_timer() - start_time))
    print(reformat_time.cache_info())


def pandas_main():
    # start_time = ti.default_timer()
    files = sorted(glob(os.path.join('data', 'DataExpo2009', '*.csv')))
    total_len = 0
    for file in files:
        temp_df = dd.read_csv(file, assume_missing=True)
        total_len += len(temp_df)
        # del temp_df
    print(total_len)

    # print("Pandas took {} seconds.".format(ti.default_timer() - start_time))


if __name__ == '__main__':

    # execute the tasks
    runs = 1
    print("Pandas finished in {xtime} seconds.".format(
        xtime=ti.timeit(stmt=pandas_more_files, number=runs, setup="gc.enable()")))
    exit()
    # create a local cluster
    client, kill_client = Client(n_workers=4), True

    # open the cluster dashboard in browser
    # webbrowser.open(client.dashboard_link, new=2, autoraise=True)

    # connect to a remote cluster'CRSDepTime'
    # client = Client('tcp://10.0.2.15:8786')
    print(client.dashboard_link)

    print("Dask finished in {xtime} seconds.".format(
        xtime=ti.timeit(stmt=dask_main, number=runs, setup="gc.enable()")))

    # pandas_task()

    # close the local cluster
    try:
        kill_client
    except NameError:
        exit(0)
    else:
        client.close()
