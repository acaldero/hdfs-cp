
/*
 *
 *  Copyright 2019-2021 Jose Rivadeneira Lopez-Bravo, Saul Alonso Monsalve, Felix Garcia Carballeira, Alejandro Calderon Mateos
 *
 *  This file is part of DaLoFlow.
 *
 *  DaLoFlow is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  DaLoFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with DaLoFlow.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <libgen.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <time.h>
#include <sys/time.h>
#include "hdfs.h"

#define BUFFER_SIZE  (      128 * 1024)
#define BLOCKSIZE    (64 * 1024 * 1024)

#ifdef DEBUG
#define DEBUG_PRINT(fmt, args...)    fprintf(stderr, fmt, ## args)
#else
#define DEBUG_PRINT(fmt, args...)    /* Don't do anything in release builds */
#endif


/*
 * Copy from/to HDFS
 */

void mkdir_recursive ( const char *path )
{
     int    ret ;
     char  *subpath, *fullpath;
     struct stat s;

     /* If directories exist then return */
     ret = stat(path, &s);
     if (ret >= 0)
     {
         if (!S_ISDIR(s.st_mode)) {
             DEBUG_PRINT("ERROR[%s]:\t path '%s' is not a directory.\n", __FUNCTION__, path) ;
         }

         return ;
     }

     /* Duplicate string (malloc inside) */
     fullpath = strdup(path);

     /* Get last directory */
     subpath = dirname(fullpath);
     if (strlen(subpath) > 1) {
         mkdir_recursive(subpath);
     }

     /* mkdir last directory */
     ret = mkdir(path, 0700);
     //if (ret < 0) {
     //    DEBUG_PRINT("ERROR[%s]:\t creating directory '%s'.\n", __FUNCTION__, path) ;
     //    perror("mkdir:") ;
     //}

     /* Free duplicate string */
     free(fullpath);
}

int create_local_file ( char *local_file_name )
{
     /* mkdir directory structure */
     struct stat st = {0} ;
     char        basename_org[PATH_MAX] ;
     char       *dirname_org ;

     strcpy(basename_org, local_file_name) ;
     dirname_org = dirname(basename_org) ;
     if (stat(dirname_org, &st) == -1) {
         mkdir_recursive(dirname_org) ;
     }

     /* Open local file */
     int write_fd = open(local_file_name, O_WRONLY | O_CREAT, 0700) ;
     if (write_fd < 0) {
         DEBUG_PRINT("ERROR[%s]:\t open fails to create '%s' file.\n", __FUNCTION__, local_file_name) ;
     }

     return write_fd ;
}

hdfsFile create_hdfs_file ( hdfsFS fs, char *hdfs_file_name )
{
     hdfsFile write_fd ;
     char   basename_org[PATH_MAX] ;
     char  *dirname_org ;

     /* mkdir directory structure */
     strcpy(basename_org, hdfs_file_name) ;
     dirname_org = dirname(basename_org) ;
     hdfsCreateDirectory(fs, dirname_org);

     /* Open hdfs file */
     write_fd = hdfsOpenFile(fs, hdfs_file_name, O_WRONLY|O_CREAT, 0, 0, 0) ;
     if (!write_fd) {
         DEBUG_PRINT("ERROR[%s]:\t hdfsOpenFile fails to create '%s'.\n", __FUNCTION__, hdfs_file_name) ;
     }

     return write_fd ;
}


int cmptime_hdfs_local ( hdfsFS fs, char *hdfs_file_name, char *local_file_name )
{
     int ret ;
     time_t t1 ;
     time_t t2 ;
     struct stat   local_attr ;
     hdfsFileInfo *hdfs_attr ;

     /* Metadata for local file */
     ret = stat(local_file_name, &local_attr) ;
     if (ret < 0) {
         return -1 ;
     }
     t1 = local_attr.st_mtime ;

     /* Metadata for HDFS file */
     hdfs_attr = hdfsGetPathInfo(fs, hdfs_file_name) ;
     if (NULL == hdfs_attr) {
         return -1 ;
     }
     t2 = hdfs_attr->mLastMod ;
     hdfsFreeFileInfo(hdfs_attr, 1) ;

     // DEBUG
     DEBUG_PRINT("INFO[%s]:\t Modification Time:\n |\tLocal = %s |\tHDFS  = %s\n", __FUNCTION__, ctime(&t1), ctime(&t2)) ;

     /* 
      * Return
      *   +: local is newer, 
      *   -: hdfs is newer, 
      *   0: 'unmodified', 
      *  -1: for errors ('return 2*x' to avoid -1 but on errors ;-))
      */
     return 2 * (t1 - t2) ;
}


int write_buffer ( hdfsFS fs, hdfsFile write_fd1, int write_fd2, void *buffer, int buffer_size, int num_readed_bytes )
{
     ssize_t write_num_bytes       = -1 ;
     ssize_t write_remaining_bytes = num_readed_bytes ;
     void   *write_buffer          = buffer ;

     while (write_remaining_bytes > 0)
     {
	 /* Write into hdfs (fs+write_fd1) or local file (write_fd2)... */
	 if ((fs != NULL) && (write_fd1 != NULL)) {
             write_num_bytes = hdfsWrite(fs, write_fd1, write_buffer, write_remaining_bytes) ;
	 }
	 if (write_fd2 != -1) {
             write_num_bytes = write(write_fd2, write_buffer, write_remaining_bytes) ;
	 }

	 /* Check errors */
         if (write_num_bytes == -1) {
	     DEBUG_PRINT("ERROR[%s]:\t write fails to write data.\n", __FUNCTION__) ;
	     return -1 ;
         }

         write_remaining_bytes -= write_num_bytes ;
         write_buffer          += write_num_bytes ;
     }

     return num_readed_bytes ;
}

int read_buffer ( hdfsFS fs, hdfsFile read_fd1, int read_fd2, void *buffer, int buffer_size )
{
     ssize_t read_num_bytes       = -1 ;
     ssize_t read_remaining_bytes = buffer_size ;
     void   *read_buffer          = buffer ;

     while (read_remaining_bytes > 0)
     {
	 /* Read from hdfs (fs+read_fd1) or local file (read_fd2)... */
	 if ((fs != NULL) && (read_fd1 != NULL)) {
             read_num_bytes = hdfsRead(fs, read_fd1, read_buffer, read_remaining_bytes) ;
	 }
	 if (read_fd2 != -1) {
             read_num_bytes = read(read_fd2, read_buffer, read_remaining_bytes) ;
	 }

	 /* Check errors */
         if (read_num_bytes == -1) {
	     DEBUG_PRINT("ERROR[%s]:\t read fails to read data.\n", __FUNCTION__) ;
	     return -1 ;
         }

	 /* Check end of file */
         if (read_num_bytes == 0) {
	     DEBUG_PRINT("INFO[%s]:\t end of file, readed %ld.\n", __FUNCTION__, (buffer_size - read_remaining_bytes)) ;
	     return (buffer_size - read_remaining_bytes) ;
         }

         read_remaining_bytes -= read_num_bytes ;
         read_buffer          += read_num_bytes ;
     }

     return buffer_size ;
}


int copy_from_hdfs_to_local ( hdfsFS fs, char *hdfs_file_name, char *local_file_name )
{
     int ret = -1 ;
     int write_fd ;
     hdfsFile read_file ;
     tSize num_readed_bytes = 0 ;
     unsigned char *buffer = NULL ;

     /* Allocate intermediate buffer */
     buffer = malloc(BUFFER_SIZE) ;
     if (NULL == buffer) {
         DEBUG_PRINT("ERROR[%s]:\t malloc for '%d'.\n", __FUNCTION__, BUFFER_SIZE) ;
         return -1 ;
     }
     memset(buffer, 0, BUFFER_SIZE) ;

     // DEBUG
     DEBUG_PRINT("INFO[%s]:\t copy from '%s' to '%s'...\n", __FUNCTION__, hdfs_file_name, local_file_name) ;

     /* Data from HDFS */
     read_file = hdfsOpenFile(fs, hdfs_file_name, O_RDONLY, 0, 0, 0) ;
     if (!read_file) {
         free(buffer) ;
         DEBUG_PRINT("ERROR[%s]:\t hdfsOpenFile on '%s' for reading.\n", __FUNCTION__, hdfs_file_name) ;
         return -1 ;
     }

     /* Data to local file */
     write_fd = create_local_file(local_file_name) ;
     if (write_fd < 0) {
         hdfsCloseFile(fs, read_file) ;
         free(buffer) ;
         return -1 ;
     }

     /* Copy from HDFS to local */
     do
     {
         num_readed_bytes = read_buffer(fs, read_file, -1, (void *)buffer, BUFFER_SIZE) ;
         if (num_readed_bytes != -1) {
             ret = write_buffer(NULL, NULL, write_fd, (void *)buffer, BUFFER_SIZE, num_readed_bytes) ;
         }
     }
     while ( (ret != -1) && (num_readed_bytes > 0) ) ;

     // Free resources (ok)
     hdfsCloseFile(fs, read_file) ;
     close(write_fd) ;
     free(buffer) ;

     return ret ;
}

int copy_from_local_to_hdfs ( hdfsFS fs, char *hdfs_file_name, char *local_file_name )
{
     int ret = -1 ;
     hdfsFile write_fd ;
     int read_file ;
     tSize num_readed_bytes = 0 ;
     unsigned char *buffer = NULL ;

     /* Allocate intermediate buffer */
     buffer = malloc(BUFFER_SIZE) ;
     if (NULL == buffer) {
         DEBUG_PRINT("ERROR[%s]:\t malloc for '%d'.\n", __FUNCTION__, BUFFER_SIZE) ;
         return -1 ;
     }
     memset(buffer, 0, BUFFER_SIZE) ;

     // DEBUG
     DEBUG_PRINT("INFO[%s]:\t copy from '%s' to '%s'...\n", __FUNCTION__, hdfs_file_name, local_file_name) ;

     /* Data to HDFS */
     write_fd = create_hdfs_file(fs, hdfs_file_name) ;
     if (!write_fd) {
         free(buffer) ;
         return -1 ;
     }

     /* Data from local file */
     read_file = open(local_file_name, O_RDONLY, 0700) ;
     if (read_file < 0) {
         hdfsCloseFile(fs, write_fd) ;
         free(buffer) ;
         DEBUG_PRINT("ERROR[%s]:\t open fails for reading from '%s' file.\n", __FUNCTION__, local_file_name) ;
         return -1 ;
     }

     /* Copy from local to HDFS */
     do
     {
         num_readed_bytes = read_buffer(NULL, NULL, read_file, (void *)buffer, BUFFER_SIZE) ;
         if (num_readed_bytes != -1) {
             ret = write_buffer(fs, write_fd, -1, (void *)buffer, BUFFER_SIZE, num_readed_bytes) ;
         }
     }
     while ( (ret != -1) && (num_readed_bytes > 0) ) ;

     /* Free resources (ok) */
     hdfsFlush(fs, write_fd) ;
     hdfsCloseFile(fs, write_fd) ;
     close(read_file) ;
     free(buffer) ;

     return ret ;
}

int hdfs_stats ( char *hdfs_file_name, char *machine_name, char ***blocks_information )
{
     char hostname_list[1024] ;

     /* Get hostnames */
     strcpy(hostname_list, "") ;
     for (int i=0; blocks_information[0][i] != NULL; i++) {
	  sprintf(hostname_list, "%s+%s", hostname_list, blocks_information[0][i]) ;
     }

     /* Get is_remote */
     int is_remote = (strncmp(machine_name, blocks_information[0][0], strlen(machine_name)) != 0) ;

     /* Print metadata for this file */
     printf("{ name:'%s', is_remote:%d, hostnames:'%s' },\n", hdfs_file_name, is_remote, hostname_list) ;

     return 0 ;
}


/*
 * Threads
 */

typedef struct thargs {
       hdfsFS    fs ;
       char      hdfs_path_org   [PATH_MAX] ;
       char      file_name_org   [PATH_MAX] ;
       char      machine_name    [HOST_NAME_MAX + 1] ;
       char      destination_dir [PATH_MAX] ;
       char      action          [PATH_MAX] ;
       char      list_files      [PATH_MAX] ;
} thargs_t ;

#define MAX_BUFFER 128
thargs_t buffer[MAX_BUFFER];

int  has_started  = 0;
int  the_end      = 0;
int  n_eltos      = 0;
int  pos_receptor = 0;
int  pos_servicio = 0;

pthread_mutex_t  mutex;
pthread_cond_t   c_no_full;
pthread_cond_t   c_no_empty;
pthread_cond_t   c_started;
pthread_cond_t   c_stopped;

char * do_reception ( FILE *fd, thargs_t *p )
{
       char *str = p->file_name_org ;
       int   len = sizeof(p->file_name_org) ;

       bzero(str, len) ;
       char *ret = fgets(str, len-1, fd) ;
       if (NULL == ret) {
           return NULL ;
       }

       str[strlen(str)-1] = '\0' ;
       return str ;
}

void * receptor ( void * param )
{
      char    *ret ;
      thargs_t p ;

      // Initializate...
      memcpy(&p, param, sizeof(thargs_t)) ;
      DEBUG_PRINT("INFO[%s]:\t 'receptor' initialized...\n", __FUNCTION__);

      // Open listing file
      FILE *list_fd = fopen(p.list_files, "ro") ;
      if (NULL == list_fd) {
          hdfsDisconnect(p.fs) ;
          perror("fopen: ") ;
          exit(-1) ;
      }

      // Get file name from listing
      ret = do_reception(list_fd, &p) ;
      while (ret != NULL)
      {
	    // Lock when not full...
            pthread_mutex_lock(&mutex);
            while (n_eltos == MAX_BUFFER) {
                   pthread_cond_wait(&c_no_full, &mutex);
	    }

	    // Inserting element into the buffer
            DEBUG_PRINT("INFO[%s]:\t 'receptor' enqueue request for '%s' at %d.\n", __FUNCTION__, p.file_name_org, pos_receptor);
            memcpy((void *)&(buffer[pos_receptor]), (void *)&p, sizeof(thargs_t)) ;
            pos_receptor = (pos_receptor + 1) % MAX_BUFFER;
            n_eltos++;

	    // signal not empty...
            pthread_cond_signal(&c_no_empty);
            pthread_mutex_unlock(&mutex);

            ret = do_reception(list_fd, &p) ;
      }

      // Signal end
      pthread_mutex_lock(&mutex);
      the_end = 1;
      pthread_cond_broadcast(&c_no_empty);
      pthread_mutex_unlock(&mutex);
      DEBUG_PRINT("INFO[%s]:\t 'receptor' finalized...\n", __FUNCTION__);

      // close local
      fclose(list_fd) ;

      pthread_exit(0);
      return NULL;
}

void * do_service ( void *params )
{
       int       ret ;
       thargs_t  thargs ;
       char      file_name_dst[2*PATH_MAX] ;
       char      file_name_org[2*PATH_MAX] ;

       /* Default return value */
       ret = 0 ;

       /* Set the initial org/dst file name... */
       memcpy(&thargs, params, sizeof(thargs_t)) ;
       sprintf(file_name_org, "%s/%s", thargs.hdfs_path_org,   thargs.file_name_org) ;
       sprintf(file_name_dst, "%s/%s", thargs.destination_dir, thargs.file_name_org) ;

       /* Do action with file... */
       if (!strcmp(thargs.action, "hdfs2local"))
       {
	   // local file is newer than HDFS file so skip it
	   int diff_time = cmptime_hdfs_local(thargs.fs, file_name_org, file_name_dst) ;
           if (diff_time > 0) {
               return NULL ;
           }

	   // copy remote to local...
           ret = copy_from_hdfs_to_local(thargs.fs, file_name_org, file_name_dst) ;
       }
       if (!strcmp(thargs.action, "local2hdfs"))
       {
           sprintf(file_name_org, "%s/%s", thargs.hdfs_path_org,   thargs.file_name_org) ;
           sprintf(file_name_dst,  "./%s",                         thargs.file_name_org) ;

	   // copy local to remote...
           ret = copy_from_local_to_hdfs(thargs.fs, file_name_org, file_name_dst) ;
       }
       if (!strcmp(thargs.action, "stats4hdfs"))
       {
	   char *** blocks_information;

	   /* Get HDFS information */
	   blocks_information = hdfsGetHosts(thargs.fs, file_name_org, 0, BLOCKSIZE) ;
	   if (NULL == blocks_information) {
	       DEBUG_PRINT("ERROR[%s]:\t hdfsGetHosts for '%s'.\n", __FUNCTION__, thargs.file_name_org) ;
	       pthread_exit((void *)(long)ret) ;
	   }

	   // file metadata...
           ret = hdfs_stats(file_name_org, thargs.machine_name, blocks_information) ;

           hdfsFreeHosts(blocks_information);
       }

       /* The End */
       return NULL ;
}

void * servicio ( void * param )
{
      thargs_t p;

      // Signal initializate...
      pthread_mutex_lock(&mutex);
      has_started = 1;
      pthread_cond_signal(&c_started);
      pthread_mutex_unlock(&mutex);
      DEBUG_PRINT("INFO[%s]:\t 'service' initialized...\n", __FUNCTION__);

      for (;;)
      {
	   // Lock when not empty and not ended...
           pthread_mutex_lock(&mutex);
           while (n_eltos == 0)
	   {
                if (the_end == 1) {
                    DEBUG_PRINT("INFO[%s]:\t 'service' finalized.\n", __FUNCTION__);
                    pthread_cond_signal(&c_stopped);
                    pthread_mutex_unlock(&mutex);
                    pthread_exit(0);
                }

                pthread_cond_wait(&c_no_empty, &mutex);
           } // while

	   // Removing element from buffer...
           DEBUG_PRINT("INFO[%s]:\t 'service' dequeue request at %d\n", __FUNCTION__, pos_servicio);
           memcpy((void *)&p, (void *)&(buffer[pos_servicio]), sizeof(thargs_t)) ;
           pos_servicio = (pos_servicio + 1) % MAX_BUFFER;
           n_eltos--;

	   // Signal not full...
           pthread_cond_signal(&c_no_full);
           pthread_mutex_unlock(&mutex);

	   // Process and response...
           do_service(&p) ;
    }

    pthread_exit(0);
    return NULL;
}


void main_usage ( char *app_name )
{
       printf("\n") ;
       printf("  HDFS Copy 1.1\n") ;
       printf(" ---------------\n") ;
       printf("\n") ;
       printf("  Usage:\n") ;
       printf("\n") ;
       printf("  > %s hdfs2local <hdfs/path> <file_list.txt> <cache/path>\n", app_name) ;
       printf("    Copy from a HDFS path to a local cache path a list of files (within file_list.txt).\n") ;
       printf("\n") ;
       printf("  > %s local2hdfs <hdfs/path> <file_list.txt> <cache/path>\n", app_name) ;
       printf("    Copy to a HDFS path a list of files (within file_list.txt).\n") ;
       printf("\n") ;
       printf("  > %s stats4hdfs <hdfs/path> <file_list.txt> <cache/path>\n", app_name) ;
       printf("    List HDFS metadata from the list of files within file_list.txt.\n") ;
       printf("\n") ;
       printf("\n") ;
}

int main ( int argc, char *argv[] )
{
    struct timeval timenow;
    long t1, t2;
    pthread_t  thr;
    pthread_t *ths;
    int MAX_SERVICIO;
    thargs_t p;

    // Check arguments
    if (argc != 5) {
        main_usage(argv[0]) ;
        exit(-1) ;
    }

    // t1
    gettimeofday(&timenow, NULL) ;
    t1 = (long)timenow.tv_sec * 1000 + (long)timenow.tv_usec / 1000 ;

    // Initialize threads...
    MAX_SERVICIO = 3 * get_nprocs_conf() ;
    ths = malloc(MAX_SERVICIO * sizeof(pthread_t)) ;

    pthread_mutex_init(&mutex,NULL);
    pthread_cond_init(&c_no_full, NULL);
    pthread_cond_init(&c_no_empty, NULL);
    pthread_cond_init(&c_started, NULL);
    pthread_cond_init(&c_stopped, NULL);

    // Initialize th_args...
    bzero(&p, sizeof(thargs_t)) ;
    strcpy(p.action,          argv[1]) ;
    strcpy(p.hdfs_path_org,   argv[2]) ;
    strcpy(p.list_files,      argv[3]) ;
    strcpy(p.destination_dir, argv[4]) ;
    gethostname(p.machine_name, HOST_NAME_MAX + 1) ;
    p.fs = hdfsConnect("default", 0) ;
    if (NULL == p.fs) {
        perror("hdfsConnect: ") ;
        exit(-1) ;
    }

    // Create threads
    for (int i=0; i<MAX_SERVICIO; i++)
    {
          pthread_create(&ths[i], NULL, servicio, &p);

          // wait thread is started
          pthread_mutex_lock(&mutex) ;
	  while (!has_started) {
                 pthread_cond_wait(&c_started, &mutex) ;
	  }
          has_started = 0 ;
          pthread_mutex_unlock(&mutex) ;
    }

    pthread_create(&thr, NULL,receptor, &p);

    // Wait for all thread end
    pthread_mutex_lock(&mutex) ;
    while ( (!the_end) || (n_eltos > 0) ) {
             pthread_cond_wait(&c_stopped, &mutex) ;
    }
    pthread_mutex_unlock(&mutex) ;

    // Join threads
    pthread_join(thr, NULL);
    for (int i=0; i<MAX_SERVICIO; i++) {
         pthread_join(ths[i], NULL);
    }

    // Finalize
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&c_no_full);
    pthread_cond_destroy(&c_no_empty);
    pthread_cond_destroy(&c_started);
    pthread_cond_destroy(&c_stopped);

    free(ths) ;
    hdfsDisconnect(p.fs) ;

    // t2
    gettimeofday(&timenow, NULL) ;
    t2 = (long)timenow.tv_sec * 1000 + (long)timenow.tv_usec / 1000 ;

    // Imprimir t2-t1...
    printf("Total time: %lf seconds.\n", (t2-t1)/1000.0);
    return 0;
}

