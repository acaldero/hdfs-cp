<html>
 <h1 align="center">HDFS-cp: <br>Parallel copy of a list of files at HDFS to local directory</h1>
 <br>
</html>


## Alternative for hdfs-cp
In order to copy from a HDFS directory into a local directory, the distcp option can be used:
```bash
/mnt/local-storage/prueba-hdfs/hadoop  distcp  hdfs://ip-namenode:port-namenode/HDFS\_directory/  file:///full/local/path 
``` 

This alternative use a map-reduce job to copy files in parallel with a MAJOR CAVEAT [stackoverflow](https://stackoverflow.com/questions/25816813/effective-ways-to-load-data-from-hdfs-to-local-system):
"This will be a distributed copy process so the destination you specify on the command line needs to be a place visible to all nodes. 
To do this you can mount a network share on all nodes and specify a directory in that network share (NFS, Samba, Other) as the destination for your files."

Another option is to use hdfs-cp, that can copy in parallel from one single node.

## Getting hdfs-cp and initial setup:
1. To clone from github:
```bash
 git clone https://github.com/acaldero/hdfs-cp.git
 cd hdfs-cp
 chmod +x *.sh
 make clean; make
``` 
2. To configure for your Java, HDFS and LIBHDFS installation:
```bash
cat <<EOF > config.hdfs-cp
#!/bin/bash

BASE_HDFS=/mnt/local-storage/prueba-hdfs/hadoop/
BASE_JAVA=/usr/lib/jvm/java-8-openjdk-amd64/
BASE_LIBHDFS=/mnt/local-storage/prueba-hdfs/lib-hdfs/
EOF
```
  
## Typical work session:
<html>
 <table>
  <tr>
  <td>
</html>

To list the main metadata for a list of HDFS files:
```bash
./hdfs-cp.sh stats4hdfs \
             /HDFS_root_directory \
             example.txt \
             ./tmp
```

In this example, "example.txt" has a list of files (one per line) relatives to "HDFS\_root\_directory".

<html>
  </td>
  <td>
</html>

To copy a HDFS list of files into a local directory:
```bash
rm -fr ./tmp; mkdir -p ./tmp

./hdfs-cp.sh hdfs2local \
             /HDFS_root_directory \
             example.txt \
             ./tmp
```

All files listed on "example.txt" are going to be copied into the "./tmp" directory.
In this example, "example.txt" has a list of files (one per line) relatives to "HDFS\_root\_directory".

<html>
  </td>
  </tr>
 </table>
</html>


## Authors
* :technologist: Saúl Alonso Monsalve
* :technologist: Félix García-Carballeira
* :technologist: José Rivadeneira López-Bravo 
* :technologist: Alejandro Calderón Mateos

