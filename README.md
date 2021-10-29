# DASK demo

This demo demonstrates the possibilities of ***DASK*** and ***PANDAS*** libraries. It compares the different approach to
processing multiple large data files and also the speed and memory efficiency of such operations.

### The dataset

The dataset used in this demo consists of flight arrival and departure details for all commercial flights within the
USA, from October 1987 to April 2008 and can be downloaded from
here <https://community.amstat.org/jointscsg-section/dataexpo/dataexpo2009>.

### Demo description

Shows the time difference between pandas and dask. Displays some data as a plotly graph. Dask approach can work on
a local cluster created either directly in the program or by using the ***start_cluster.sh*** script (separately
created cluster had better performance during my tests).

### Usage
python daskDemo.py <-h | -p \'path\' -r number>

Options:
- -h --help display this help and exit the program 
- -r --runs specify the number of runs for each task 
- -p --path specify the path to the dataset files