<html>
 <h1 align="center">HDFS-cp: <br>Parallel copy of a list of files at HDFS to local directory</h1>
 <br>
</html>


## Alternative for hdfs-cp
In order to copy from a HDFS directory into a local directory, the distcp option can be used:
```bash
/mnt/local-storage/prueba-hdfs/hadoop  distcp  \
                                       hdfs://ip-namenode:port-namenode/HDFS\_directory/ \
                                       file:///full/local/path 
``` 

This alternative use a map-reduce job to copy files in parallel with a MAJOR CAVEAT (from [stackoverflow](https://stackoverflow.com/questions/25816813/effective-ways-to-load-data-from-hdfs-to-local-system)):

> "This will be a distributed copy process so the destination you specify on the command line needs to be a place visible to all nodes. 
> To do this you can mount a network share on all nodes and specify a directory in that network share (NFS, Samba, Other) as the destination for your files."

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

### 1. Copy a local list of files from local node into a HDFS directory (in parallel)...
<html>
 <table width="100%">
  <tr valign="top">
  <td>
</html>

Example: copy all files listed on "example.txt" into the "./HDFS_root_directory" directory.

```bash
: To copy files into tmp...
cp -a /path/to/dataset ./tmp

: To get the list of files...
find tmp > example.txt

: To copy local files to HDFS...
./hdfs-cp.sh local2hdfs  /HDFS_root_directory  example.txt  ./tmp
```

In this example, "example.txt" has a list of files (one per line) relatives to "tmp".
  
<html>
  </td>
  </tr>
 </table>
</html>

### 2. Copy a HDFS list of files from HDFS into a local directory (in parallel)...
<html>
 <table width="100%">
  <tr valign="top">
  <td>
</html>

Example: to copy all files listed on "example.txt" to the "./tmp" directory.

```bash
: To remove previous tmp content...
rm -fr ./tmp
mkdir -p ./tmp

: To copy a HDFS files to the local node...
./hdfs-cp.sh   hdfs2local  /HDFS_root_directory  example.txt  ./tmp
```

In this example, "example.txt" has a list of files (one per line) relatives to "HDFS\_root\_directory".

<html>
  </td>
  </tr>
 </table>
</html>
 
### 3. List the main metadata for a list of HDFS files...

<html>
 <table width="100%">
  <tr valign="top">
  <td>
</html>

Example: to show the main matadata from associated to a list of files.
  
```bash
: To show the main matadata ...
./hdfs-ls.sh  stats4hdfs  /HDFS_root_directory  example.txt
```

In this example, "example.txt" has a list of files (one per line) relatives to "HDFS\_root\_directory". 
 
<html>
  </td>
  </tr>
 </table>
</html>


## Some additional useful options for debugging:
1. To print a brief report on current work in progress each 12 seconds:
   1. First, please open a new terminal.
   2. Then execute:
   ```bash
   watch -n 12 kill -10 $(ps axu | grep hdfs-cp | awk '{print $2}')
   ```

## Authors
* :technologist: Saúl Alonso Monsalve
* :technologist: Félix García-Carballeira
* :technologist: José Rivadeneira López-Bravo 
* :technologist: Alejandro Calderón Mateos

