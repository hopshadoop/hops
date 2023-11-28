# libhdfs.so written in Golang

This is drop in replacement for existing libhdfs.so. This is written in golang and it does not have 
any java dependencies.

## Supported Functions

New parameters are supplied using environment variables. 

### Logging 

- LIBHDFS_ENABLE_LOG: Boolean variable to enable/disable logging. By default logs are written to stdout
- LIBHDFS_LOG_FILE: File path where logs are written

### HopsFS TLS Parameters

- LIBHDFS_ROOT_CA_BUNDLE: File path for root CA bundle
- LIBHDFS_CLIENT_CERTIFICATE: File path for client certificate
- LIBHDFS_CLIENT_KEY: File path for client private key

### additional params 

- LIBHDFS_DEFAULT_FS: default file system URI 
- LIBHDFS_DEFAULT_USER: hdfs user name

If these parameters are not provided then it will use `HADOOP_HOME` and `HADOOP_CONF_DIR` to read
TLS configuration from core-site.xml 

## Supported Functions

This is an initial beta release. Only functions that are needed by pyarrow are implemented. 
The following functions are supported

- hdfsBuilderSetNameNode
- hdfsBuilderSetNameNodePort
- hdfsBuilderSetUserName
- hdfsBuilderSetForceNewInstance
- hdfsBuilderConnect
- hdfsCreateDirectory
- hdfsDelete
- hdfsDisconnect
- hdfsExists
- hdfsFreeFileInfo
- hdfsGetCapacity
- hdfsGetUsed
- hdfsGetPathInfo
- hdfsListDirectory
- hdfsChown
- hdfsChmod
- hdfsCloseFile
- hdfsFlush
- hdfsOpenFile
- hdfsRead
- hdfsSeek
- hdfsTell
- hdfsWrite
- hdfsRename
- hdfsAvailable
- hdfsGetDefaultBlockSize
- hdfsSetWorkingDirectory
- hdfsGetWorkingDirectory
- hdfsCopy
- hdfsMove
- hdfsPread
- hdfsSetReplication
- hdfsUtime

## Unsupported Functions

These functions are not yet implemented. 
Calling these functioins will return error and `errno` will be set to ENOSYS

- hdfsFileIsOpenForRead
- hdfsFileIsOpenForWrite
- hdfsFileGetReadStatistics
- hdfsReadStatisticsGetRemoteBytesRead
- hdfsFileClearReadStatistics
- hdfsFileFreeReadStatistics
- hdfsConnectAsUser
- hdfsConnect
- hdfsConnectAsUserNewInstance
- hdfsConnectNewInstance
- hdfsFreeBuilder
- hdfsBuilderConfSetStr
- hdfsConfGetStr
- hdfsConfGetInt
- hdfsConfStrFree
- hdfsTruncateFile
- hdfsUnbufferFile
- hdfsHFlush
- hdfsHSync
- hdfsFileIsEncrypted
- hdfsGetDefaultBlockSizeAtPath
- hadoopRzOptionsAlloc
- hadoopRzOptionsSetSkipChecksum
- hadoopRzOptionsSetByteBufferPool
- hadoopRzOptionsFree
- hadoopReadZero
- hadoopRzBufferLength
- hadoopRzBufferGet
- hadoopRzBufferFree
- hdfsGetHosts
- hdfsFreeHosts
- hdfsBuilderSetKerbTicketCachePath
