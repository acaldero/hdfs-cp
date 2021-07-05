
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
import gzip
import multiprocessing as mp
from   snakebite.client import Client


def show_header():
    print('')
    print(' HDFS-Copy (Daloflow)')
    print(' --------------------')
    print('')

def init_worker(function):
    # initialize
    function.client = Client(args.hdfs_host, args.hdfs_port)

def copy_hdfs_to_local(args, elto):
    # copy
    src_file_name = args.hdfs_path   + elto
    dst_file_name = args.destination + elto
    try:
        dst_dir_name = os.path.dirname(dst_file_name)
        os.makedirs(dst_dir_name, 0o770, exist_ok=True)

        for f in copy_hdfs_to_local.client.copyToLocal([src_file_name], dst_file_name):
            if f['result'] == False:
                print('File ' + f['path'] + ' NOT copied because "' + str(f['error']) + '", sorry !')
    except:
        print('Exception ' + str(sys.exc_info()[0]) + ' on file ' + src_file_name)
        return 0

    # return
    return 1

def copy_parallel(args):
    # read data
    print(' * Reading list of file...', end='')
    data = []
    with gzip.open(args.source, mode='rt') as data_file:
        for line in data_file:
            data.append(line.rstrip('\n'))
    print(' Done!')

    # Starting
    print(' * Number of processors: ', mp.cpu_count())
    start_time = time.time()
    pool = mp.Pool(mp.cpu_count(), initializer=init_worker, initargs=(copy_hdfs_to_local,))

    # do action...
    results = []
    if args.action == 'hdfs2local':
        results = pool.starmap(copy_hdfs_to_local, [(args, row) for row in data])

    # Stopping
    pool.close()
    stop_time = time.time()

    # verbose
    print(" * Execution time: %s seconds" % (stop_time - start_time))
    print(" * Number files:   %s"         % (results))



# default values
hdfs_host   = '10.0.40.19'
hdfs_port   = 9600
hdfs_path   = '/daloflow/'
action      = 'hdfs2local'
source      = 'dataset128x128.list.txt.gz'
destination = '/mnt/local-storage/tmp/'

# parse
parser = argparse.ArgumentParser(description='HDFS client')
parser.add_argument('--hdfs_host',   type=str, default=hdfs_host,   required=False, help='HDFS host')
parser.add_argument('--hdfs_port',   type=str, default=hdfs_port,   required=False, help='HDFS port')
parser.add_argument('--hdfs_path',   type=str, default=hdfs_path,   required=False, help='HDFS path')
parser.add_argument('--action',      type=str, default=action,      required=False, help='Action')
parser.add_argument('--source',      type=str, default=source,      required=False, help='Source path')
parser.add_argument('--destination', type=str, default=destination, required=False, help='Destination path')
args   = parser.parse_args()

# do main
show_header()
copy_parallel(args)

