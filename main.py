import timeit as ti
import webbrowser
from datetime import datetime
import dask
import dask.dataframe as dd
from dask.distributed import Client
from dask import delayed
from dask import compute
import pandas as pd
import os
from glob import glob


def reformat_time(time: str) -> datetime.time:
    if time == '2400':
        time = '2359'
    if len(time) != 4:
        time = ''.join(('000', time))
        time = time[-4:]
    # try:
    #     result = datetime.strptime(time, '%H%M').time()
    # except ValueError:
    #     print(time)
    #     exit()
    # print(time)
    return datetime.strptime(time, '%H%M').time()


def demo_ops_dask(df: dd.DataFrame) -> dd.DataFrame:
    """function with demo tasks using dask"""
    # departure delays avg
    dep_del_mean = df['DepDelay'].mean()
    # arrival delays avg
    arr_del_mean = df['ArrDelay'].mean()
    # avg arrival delays by Origin of flight
    mean_delay = df.groupby('Origin')['DepDelay'].mean()
    # the day with lowest avg delay
    day_delay = df.groupby('DayOfWeek')['DepDelay'].mean()

    # compute the results in parallel, might need some modification based on the size of data and machines memory
    mean_delay, dep_del_mean, day_delay = compute(mean_delay, dep_del_mean, day_delay)
    # day_delay,  = compute(day_delay)
    # dep_del_mean, = compute(dep_del_mean)
    # dep_del_mean, arr_del_mean = compute(dep_del_mean, arr_del_mean)

    # control prints
    print("Avg dep delay for ABE is {}".format(mean_delay['ABE']))
    print("Avg dep delay is {}".format(dep_del_mean))
    # print("Avg arr delay is {}".format(arr_del_mean))
    print("Avg day delays:\n{}".format(day_delay.sort_values()))
    return df


def demo_ops_pandas(df: pd.DataFrame) -> list:
    """function with demo tasks using pandas"""
    dep_group = df.groupby('Origin')
    # data for ABE airport departure delay avg
    abe_delay_cnt = dep_group['DepDelay'].count()['ABE']
    abe_delay_sum = dep_group['DepDelay'].sum()['ABE']
    # data for avg departure delay
    dep_delay_cnt = df['DepDelay'].count()
    dep_delay_sum = df['DepDelay'].sum()
    day_group = df.groupby('DayOfWeek')['DepDelay'].mean()

    return [abe_delay_cnt, abe_delay_sum, dep_delay_cnt, dep_delay_sum, day_group]
    # return [abe_delay_cnt, abe_delay_sum]


def pandas_task():
    start_time = ti.default_timer()
    file = os.path.join('data', 'DataExpo2009', '1987.csv')
    df = pd.read_csv(file)
    demo_ops_pandas(df)

    print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))


def pandas_more_files():
    # start_time = ti.default_timer()

    files = glob(os.path.join('data', 'DataExpo2009', 'years_to_compute', '*.csv'))
    # print("pandas files: {}".format(files))
    # file_count = len(files)
    cols = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepDelay', 'CRSDepTime', 'Origin', 'Dest',
            'ArrDelay']
    types_dict = {'CancellationCode': 'object',
                  'CRSDepTime': 'string',
                  'Year': 'string',
                  'Month': 'string',
                  'DayofMonth': 'string'}
    abe_cnt, abe_sum, del_cnt, del_sum, day_delay = [], [], [], [], []
    for file in files:
        print("reading file {}".format(file))

        res = pd.read_csv(file, encoding_errors='replace', usecols=cols,
                          dtype=types_dict)
        res = demo_ops_pandas(res)

        abe_cnt.append(res[0])
        abe_sum.append(res[1])
        if len(res) > 2:
            del_cnt.append(res[2])
            del_sum.append(res[3])
            day_delay.append(res[4])
    print("Average departure delay for ABE is {}".format(sum(abe_sum) / sum(abe_cnt)))
    if len(del_cnt):
        print("Average departure delay is {}".format(sum(del_sum) / sum(del_cnt)))
    if len(day_delay):
        print("Avg day delays:\n{}".format(day_delay))
    # print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))


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
    df['CRSDepTime'] = df['CRSDepTime'].apply(reformat_time, meta='datetime.time')
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
