
#
# Const.
#

BASE_HDFS=/mnt/local-storage/prueba-hdfs/hadoop/
BASE_JAVA=/usr/lib/jvm/java-8-openjdk-amd64/jre/
INCLUDE_PATH=-I$(BASE_HDFS)/include
LIB_PATH=-L$(BASE_JAVA)/lib/amd64/server -L$(BASE_HDFS)/lib/native


#
# Rules
#

all:: hdfs-cp

clean::
	rm -fr hdfs-cp

hdfs-cp:
	gcc -Wall -O2        $(INCLUDE_PATH) $(LIB_PATH) hdfs-cp.c -o hdfs-cp -lhdfs -lpthread -ljvm

