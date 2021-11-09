# DASK demo

This demo demonstrates the possibilities of ***DASK*** and ***PANDAS*** libraries. It compares the different approach to
processing multiple large data files and also the speed and memory efficiency of such operations.

### The dataset

The dataset used in this demo consists of flight arrival and departure details for all commercial flights within the
USA, from October 1987 to April 2008 and can be downloaded from
here <https://community.amstat.org/jointscsg-section/dataexpo/dataexpo2009>.

### Demo description

Shows the time difference between pandas and dask. Displays some data as a plotly graph. Dask approach can work on a
local cluster created either directly in the program or by using the ***start_cluster.sh*** script (separately created
cluster had better performance during my tests).

### Usage

usage: daskDemo.py [-h] -p PATH -r RUNS [--cluster CLUSTER] [--task {pandas,dask}]

optional arguments:
- -h, --help            show this help message and exit
- -p PATH, --path PATH  path to the file with dataset
- -r RUNS, --runs RUNS  number of program runs
- --cluster CLUSTER     address of the remote cluster that should be used, if not specified, program uses a locally created cluster
- --task {pandas,dask}  specify which task to execute, if not specified, all tasks will be run

