import argparse
import timeit as ti
from datetime import datetime
from glob import glob
from functools import lru_cache

import dask.dataframe as dd
from dask.distributed import Client
from dask import compute

import pandas as pd
# import modin.pandas as pd

import plotly.express as px

import resource

import ctypes


@lru_cache(maxsize=1024)  # added cache for increased performance
def reformat_time(time: str) -> datetime.time:
    """
    function converts time columns into datetime.time() and handles exceptions where the time value is specified
    in wrong format
    :param time: string for time conversion
    :returns: datetime.time object with converted time
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
    # dep_del_mean, day_delay = compute(dep_del_mean, day_delay)

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
        print('No files found on the specified path: {}'.format(path))
        parser.print_help()
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
    # day_result_sum, day_result_cnt = [0 for _ in range(7)], [0 for _ in range(7)]
    day_result_sum = [0 for _ in range(7)]
    day_result_cnt = day_result_sum.copy()
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
    # fig = px.bar(day_results, color='avgDelay', y='avgDelay', hover_data=['avgDelay'], height=500,
    #              labels={'avgDelay': 'Average departure delay (minutes)',
    #                      'day': 'Day of week'})
    # fig.show()
    # todo enhance the graph, add the graph to other results, push to github and create readme.md
    # print("Pandas task took {} seconds.".format(ti.default_timer() - start_time))
    # print(reformat_time.cache_info())
    usage_self = resource.getrusage(resource.RUSAGE_SELF)
    print('user mode time = {}s'.format(usage_self.ru_utime))
    print('max resident size used = {}kB'.format(usage_self.ru_maxrss))
    # print('thread = {}'.format(resource.getrusage(resource.RUSAGE_THREAD)))
    # print('children = {}'.format(resource.getrusage(resource.RUSAGE_CHILDREN)))


def pandas_main(runs: int):
    """
    runs the pandas task n times
    :param runs: specifies how many times the task should be run
    """
    if runs is None or runs == 1:
        pandas_more_files()
    elif runs > 1:
        print("Pandas finished in {xtime} seconds.".format(
            xtime=ti.timeit(stmt=pandas_more_files, number=runs, globals=globals())))
    else:
        ve = ValueError('Invalid number of runs specified. Please provide an integer higher than 0.')
        raise ve


def dask_task():
    """
    main dask function
    """
    # start_time = ti.default_timer()
    files = glob(path)
    if not len(files):
        print('No files found on the specified path: {}'.format(path))
        parser.print_help()
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
    # client.run(trim_memory)
    # print("Dask took {} seconds.".format(ti.default_timer() - start_time))

    # print(reformat_time.cache_info())


def dask_main(arguments):
    """
    runs the dask task either on local cluster or on a remote one and measures the execution time
    :param arguments: program cmd line arguments
    """
    try:
        cluster = arguments.cluster
        # connect to a remote cluster - need to start with start_cluster.sh or manually
        client = Client(cluster)
    except NameError:
        # create a local cluster
        client, kill_client = Client(n_workers=4), True
    finally:
        print('Cluster dashboard running on: {}'.format(client.dashboard_link))
    # open the cluster dashboard in browser
    # webbrowser.open(client.dashboard_link, new=2, autoraise=True)

    # client.run(trim_memory)

    if arguments.runs is None or arguments.runs == 1:
        dask_task()
    elif arguments.runs > 1:
        print("Dask finished in {xtime} seconds.".format(
            xtime=ti.timeit(stmt=dask_task, number=arguments.runs, globals=globals())))
    else:
        ve = ValueError('Invalid number of runs specified. Please provide an integer higher than 0.')
        raise ve

    # print some basic usage info
    usage_self = resource.getrusage(resource.RUSAGE_SELF)
    print('user mode time = {}s'.format(usage_self.ru_utime))
    print('max resident size used = {}kB'.format(usage_self.ru_maxrss))
    # close the local cluster
    try:
        kill_client
    except NameError:
        exit(0)
    else:
        client.close()


def create_arg_parser() -> argparse.ArgumentParser:
    """
    create command line arguments parser
    :return: cmd arguments parser
    """
    new_parser = argparse.ArgumentParser(description='Demo dask & pandas task')
    new_parser.add_argument('-p', '--path', type=str, required=True, help='path to the file with dataset')
    new_parser.add_argument('-r', '--runs', type=int, required=False, help='number of program runs')
    new_parser.add_argument('--cluster', type=str, required=False,
                            help='address of the remote cluster that should be used, if not specified, program uses a '
                                 'locally created cluster')
    new_parser.add_argument('--task', type=str, required=False,
                            help='specify which task to execute, if not specified, all tasks will be run',
                            choices=['pandas', 'dask'])
    return new_parser


def trim_memory() -> int:
    """function for trimming unused worker memory"""
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


if __name__ == '__main__':

    """parse the cmd options"""
    parser = create_arg_parser()
    args = parser.parse_args()
    path = args.path

    """decide what task to launch"""
    if args.task == 'pandas':
        pandas_main(args.runs)
    elif args.task == 'dask':
        dask_main(args)
    elif args.task is None:
        # task not specified, execute both
        pandas_main(args.runs)
        dask_main(args)
