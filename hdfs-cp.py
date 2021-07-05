
#
#  Copyright 2019-2021 Jose Rivadeneira Lopez-Bravo, Saul Alonso Monsalve, Felix Garcia Carballeira, Alejandro Calderon Mateos
#
#  This file is part of DaLoFlow.
#
#  DaLoFlow is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  DaLoFlow is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with DaLoFlow.  If not, see <http://www.gnu.org/licenses/>.
#


# import
import os
import argparse
import time
import sys
import random
import dask.bag as db
import dask.distributed
import multiprocessing as mp
from   snakebite.client import Client

#
def costly_simulation(list_param):
    time.sleep(random.random())
    return sum(list_param)

def copy_hdfs_to_local(args):
    nfiles = 0
    if not os.path.exists(args.destination):
         os.mkdir(args.destination, 0o770)

    # verbose
    print(' * Copy from '+ args.source + ' to ' + args.destination + '...')

    # get time
    start_time = time.time()

    # hdfs client
    client = Client(args.hdfs_host, args.hdfs_port)
    for f in client.copyToLocal([args.source], args.destination):
         nfiles = nfiles + 1
         if f['result'] == False:
             print('File ' + f['path'] + ' NOT copied because "' + str(f['error']) + '", sorry !')
         if (nfiles % 100) == 0:
             print('C', end='')

    # get time
    stop_time = time.time()

    # verbose
    print(" * Execution time:  %s seconds" % (stop_time - start_time))
    print(" * Number of files: %s seconds" % (nfiles))
    return True


# default values
hdfs_host   = '10.0.40.19'
hdfs_port   = 9600
action      = 'hdfs2local'
source      = '/daloflow/dataset32x32/'
destination = '/mnt/local-storage/tmp/dataset32x32/'

# parse
parser = argparse.ArgumentParser(description='HDFS client')
parser.add_argument('--hdfs_host',   type=str, default=hdfs_host,   nargs=1, required=False, help='HDFS host')
parser.add_argument('--hdfs_port',   type=str, default=hdfs_port,   nargs=1, required=False, help='HDFS port')
parser.add_argument('--action',      type=str, default=action,      nargs=1, required=False, help='Action')
parser.add_argument('--source',      type=str, default=source,      nargs=1, required=False, help='Source path')
parser.add_argument('--destination', type=str, default=destination, nargs=1, required=False, help='Destination path')
args   = parser.parse_args()

# copy
print("Number of processors: ", mp.cpu_count())

copy_hdfs_to_local(args)
sys.exit()

