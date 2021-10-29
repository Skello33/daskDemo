import getopt
import sys
import timeit as ti
from datetime import datetime
from glob import glob
from functools import lru_cache

import dask.dataframe as dd
from dask.distributed import Client
from dask import compute

import pandas as pd

import plotly.express as px


@lru_cache(maxsize=1024)    # added cache for increased performance
def reformat_time(time: str) -> datetime.time:
    """
    function converts time columns into datetime.time() and handles exceptions where the time value is specified
    in wrong format
    """
    try:
        return datetime.strptime(time, '%H%M').time()
    except ValueError as ve:
        if time == '2400':
            return datetime.strptime('2359', '%H%M').time()
        elif len(time) == 1:
            time = ''.join(('0', time))
            return datetime.strptime(time, '%H%M').time()
        else:
            # raise the exception for any other invalid input
            raise ve


def demo_ops_dask(df: dd.DataFrame):
    """
    function with demo tasks using dask
    :param df: dataframe for demo tasks
    """
    # departure delays avg
    dep_del_mean = df['DepDelay'].mean()
    # arrival delays avg
    # arr_del_mean = df['ArrDelay'].mean()
    # avg arrival delays by Origin of flight
    origin_delay = df.groupby('Origin')['DepDelay'].mean()
    # the day with lowest avg delay
    day_delay = df.groupby('DayOfWeek')['DepDelay'].mean()

    # compute the results in parallel, might need some modification based on the size of data and machines memory
    origin_delay, dep_del_mean, day_delay = compute(origin_delay, dep_del_mean, day_delay)
    # day_delay, = compute(day_delay)
    # dep_del_mean, = compute(dep_del_mean)
    # origin_delay, = compute(origin_delay)
    # dep_del_mean, arr_del_mean = compute(dep_del_mean, arr_del_mean)

    # control prints
    print("Avg dep delay for JFK is {}".format(origin_delay['JFK']))
    print("Avg dep delay is {}".format(dep_del_mean))
    # print("Avg arr delay is {}".format(arr_del_mean))
    print("Avg day delays:\n{}".format(day_delay.sort_values(ascending=False)))


def demo_ops_pandas(df: pd.DataFrame) -> list:
    """
    function with demo tasks using pandas
    :param df: dataframe for demo tasks
    :return: list of lists containing results of demo task
    """
    result = list()

    origin_group = df.groupby('Origin')
    # data for JFK airport departure delay avg
    result.append(origin_group['DepDelay'].count()['JFK'])
    result.append(origin_group['DepDelay'].sum()['JFK'])
    # data for avg departure delay
    result.append(df['DepDelay'].count())
    result.append(df['DepDelay'].sum())
    # data for average delays for each day of week
    day_group = df.groupby('DayOfWeek')
    result.append(day_group['DepDelay'].sum().tolist())
    result.append(day_group['DepDelay'].count().tolist())

    return result


def pandas_more_files():
    """
    main pandas function
    """
    # start_time = ti.default_timer()

    files = glob(path)
    if not len(files):
        print('No files found on the specified path.')
        usage()
        exit(0)
    # pandas read options
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

    # read in the files one by one
    for file in files:
        # print("reading file {}".format(file))
        res = pd.read_csv(file, encoding_errors='replace', usecols=cols, dtype=types_dict)

        # scheduled departure time column reformat, implemented but it is expensive to execute
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
    fig = px.bar(day_results, color='avgDelay', y='avgDelay', hover_data=['avgDelay'], height=500,
                 labels={'avgDelay': 'Average departure delay (minutes)',
                         'day': 'Day of week'})
    fig.show()
    # todo enhance the graph, add the graph to other results, push to github and create readme.md
    # print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))
    # print(reformat_time.cache_info())


def dask_main():
    """
    main dask function
    """
    # start_time = ti.default_timer()
    files = glob(path)
    if not len(files):
        print('No files found on the specified path.')
        usage()
        exit(0)
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

    # read the input files
    df = dd.read_csv(files, encoding_errors='replace', dtype=col_types, usecols=cols)

    # scheduled departure time column reformat, implemented but it is expensive to execute
    # df['CRSDepTime'] = df['CRSDepTime'].apply(reformat_time, meta='datetime.time')

    # df['Date'] = (pd.to_datetime(df['Year'].astype(str) + '-' +
    #                              df['Month'].astype(str) + '-' +
    #                              df['DayofMonth'].astype(str) + ' ' +
    #                              df['CRSDepTime'].apply(reformat_time, meta=('CRSDepTime', 'string'))
    #                              )
    #               )

    # call demo functions on the dataframe
    demo_ops_dask(df)
    # print("Dask took {} seconds.".format(ti.default_timer() - start_time))

    # print reformat_time cache info
    # print(reformat_time.cache_info())


def usage():
    """
    print program usage
    """
    print('Dask + pandas demo program.\n'
          'Options:\n'
          '-h --help\tdisplay this help and exit the program\n'
          '-r --runs\tspecify the number of runs for each task\n'
          '-p --path\tspecify the path to the dataset files\n'
          'Usage:\n'
          'python daskDemo.py <-h | -p \'path\' -r number>')


def parse_options() -> (int, str):
    """
    function for command line options parsing
    :return: pair runs, path
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], '-h -r: -p:', longopts=['help', 'runs=', 'path='])
    except getopt.GetoptError as ge:
        print(ge)
        usage()
        exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
            exit(0)
        elif o in ('-r', '--runs'):
            try:
                runs = int(a)
            except ValueError as ve:
                print('Invalid number of runs: {}. Please give an integer.'.format(a))
                usage()
                exit(2)
        elif o in ('-p', '--path'):
            path = a
    try:
        path, runs
    except NameError as ne:
        print(ne)
        usage()
        exit(2)
    return runs, path


if __name__ == '__main__':

    runs, path = parse_options()
    # PANDAS
    print("Pandas finished in {xtime} seconds.".format(
        xtime=ti.timeit(stmt=pandas_more_files, number=runs, globals=globals())))
    # DASK
    # create a local cluster
    client, kill_client = Client(n_workers=4), True

    # open the cluster dashboard in browser
    # webbrowser.open(client.dashboard_link, new=2, autoraise=True)

    # connect to a remote cluster - need to start with start_cluster.sh or manually
    # client = Client('tcp://10.0.2.15:8786')
    print('Cluster dashboard running on: {}'.format(client.dashboard_link))

    print("Dask finished in {xtime} seconds.".format(
        xtime=ti.timeit(stmt=dask_main, number=runs, globals=globals())))

    # close the local cluster
    try:
        kill_client
    except NameError:
        exit(0)
    else:
        client.close()
