#!/bin/bash
set -x

#
# Get configuration
#

BASE_DIR=$(dirname "$0")
. $BASE_DIR/config.test

if [ ! -d "$BASE_CACHE" ]; then
        echo "Directory not found: $BASE_CACHE"
        exit
fi

if [ ! -f "$LIST_CACHE" ]; then
        echo "File not found: $LIST_CACHE"
        exit
fi


#
# Copy files into a local directory ($BASE_CACHE + "container name")
#
BASE_CACHE_IN_CONTAINER=$BASE_CACHE"/"$(hostname)

# Remove old "train*.tar.gz" files at $BASE_CACHE...
rm -fr $BASE_CACHE_IN_CONTAINER

# Copy new files
$BASE_DIR/hdfs-cp.sh $BASE_HDFS $LIST_CACHE $BASE_CACHE_IN_CONTAINER

