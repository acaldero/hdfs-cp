#set -x

# Load default configuration
BASE_DIR=$(dirname "$0")
. $BASE_DIR/config.hdfs-cp

# For HADOOP (José)
export PATH=$BASE_HDFS/bin:$BASE_JAVA/bin::$BASE_JAVA/sbin:$PATH
export JAVA_HOME=$BASE_JAVA/jre
export HADOOP_INSTALL=$BASE_HDFS
export HADOOP_MAPRED_HOME=$BASE_HDFS
export HADOOP_COMMON_HOME=$BASE_HDFS
export HADOOP_HDFS_HOME=$BASE_HDFS
export YARN_HOME=$BASE_HDFS
export HADOOP_COMMON_LIB_NATIVE=$BASE_HDFS/lib/native
export HADOOP_OPTS="-Djava.library.path=$BASE_HDFS/lib"
export HADOOP_PORT=0

# LD_LIBRARY_PATH + CLASSPATH
FULL_LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$BASE_LIBHDFS/lib:$BASE_JAVA/jre/lib/amd64/server
FULL_CLASSPATH=$CLASSPATH:$(hadoop classpath --glob)

# Parameters
PARAMETER_HDFS_CMD=$1
PARAMETER_HDFS_DIR=$2
PARAMETER_HDFS_LIST=$3
PARAMETER_CACHE_DIR=$4

# If necessary then remove the cache_directory first
if [ "${REMOVE_CACHE_FIST,,}" == "true" ]; then
     rm -fr $PARAMETER_CACHE_DIR
fi

# Run hdfs-cp
env CLASSPATH=$FULL_CLASSPATH LD_LIBRARY_PATH=$FULL_LD_LIBRARY_PATH $BASE_DIR/hdfs-cp -a $PARAMETER_HDFS_CMD -h $PARAMETER_HDFS_DIR -f $PARAMETER_HDFS_LIST

