import timeit as ti
import webbrowser

import dask
import dask.dataframe as dd
from dask.distributed import Client
from dask import delayed
from dask import compute
import pandas as pd
import os
from glob import glob
import time


def demo_ops_dask(df: dd.DataFrame) -> dd.DataFrame:
    dep_del_mean = df['DepDelay'].mean()
    # arr_del_mean = df['ArrDelay'].mean()
    mean_delay = df.groupby('Origin')['DepDelay'].mean()
    # dep_del_mean, arr_del_mean = compute(dep_del_mean, arr_del_mean)
    mean_delay, = compute(mean_delay)
    dep_del_mean, = compute(dep_del_mean)

    print("Avg dep delay for ABE is {}".format(mean_delay['ABE']))
    print("Avg dep delay is {}".format(dep_del_mean))
    # print("Avg arr delay is {}".format(arr_del_mean))
    return df


def demo_ops_pandas(df: pd.DataFrame) -> list:
    dep_group = df.groupby('Origin')
    abe_delay_cnt = dep_group['DepDelay'].count()['ABE']
    abe_delay_sum = dep_group['DepDelay'].sum()['ABE']
    dep_delay_cnt = df['DepDelay'].count()
    dep_delay_sum = df['DepDelay'].sum()

    # print(df['DepDelay'].mean())
    # print(df['ArrDelay'].mean())
    return [abe_delay_cnt, abe_delay_sum, dep_delay_cnt, dep_delay_sum]


def pandas_task():
    start_time = ti.default_timer()
    file = os.path.join('data', 'DataExpo2009', '1987.csv')
    df = pd.read_csv(file)
    demo_ops_pandas(df)

    print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))


def pandas_more_files():
    # start_time = ti.default_timer()

    files = glob(os.path.join('data', 'DataExpo2009', '*.csv'))
    # print("pandas files: {}".format(files))
    file_count = len(files)
    abe_cnt, abe_sum, del_cnt, del_sum = [], [], [], []
    for file in files:
        res = pd.read_csv(file)
        res = demo_ops_pandas(res)
        abe_cnt.append(res[0])
        abe_sum.append(res[1])
        del_cnt.append(res[2])
        del_sum.append(res[3])
    print("Average departure delay for ABE is {}".format(sum(abe_sum) / sum(abe_cnt)))
    print("Average departure delay is {}".format(sum(del_sum) / sum(del_cnt)))
    # print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))


def dask_main():
    # start_time = ti.default_timer()
    files = os.path.join('data', 'DataExpo2009', '*.csv')
    # print("dask files: {}".format(files))
    df = dd.read_csv(files, dtype={'ActualElapsedTime': 'float64',
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
                                   'TaxiOut': 'float64'
                                   })

    # print(df['CRSElapsedTime'].mean().compute())
    demo_ops_dask(df)

    # print(len(df))

    # print("Dask took {} seconds.".format(ti.default_timer() - start_time))


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

    # create a local cluster
    # client = Client(n_workers=4)

    # open the cluster dashboard in browser
    # webbrowser.open(client.dashboard_link, new=2, autoraise=True)

    # connect to a remote cluster
    client = Client('tcp://10.0.2.15:8786')
    print(client.dashboard_link)

    print("Dask finished in {xtime} seconds.".format(
        xtime=ti.timeit(stmt=dask_main, number=runs, setup="gc.enable()")))

    # pandas_task()

    # close the cluster
    # client.close()
