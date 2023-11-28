#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint16_t  tPort; /// port
typedef int32_t   tSize; /// size of data for read/write io ops
typedef time_t    tTime; /// time type in seconds
typedef int64_t   tOffset;/// offset within the file

typedef const char cchar_t;

typedef enum tObjectKind {
    kObjectKindFile = 'F',
    kObjectKindDirectory = 'D',
} tObjectKind;


typedef struct hdfsBuilderConfOpt {
    struct hdfsBuilderConfOpt *next;
    const char *key;
    const char *val;
}hdfsBuilderConfOpt;

typedef struct hdfsBuilder {
    int forceNewInstance;
    const char *nn;
    tPort port;
    const char *kerbTicketCachePath;
    const char *userName;
    struct hdfsBuilderConfOpt *opts;
}hdfsBuilder;

// hdfsFileInfo - Information about a file/directory.
typedef struct  hdfsFileInfo{
    tObjectKind mKind;   // file or directory
    char *mName;         // the name of the file
    tTime mLastMod;      // the last modification time for the file in seconds
    tOffset mSize;       // the size of the file in bytes
    short mReplication;  // the count of replicas
    tOffset mBlockSize;  // the block size for the file
    char *mOwner;        // the owner of the file
    char *mGroup;        // the group associated with the file
    short mPermissions;  // the permissions associated with the file
    tTime mLastAccess;   // the last access time for the file in seconds
} hdfsFileInfo;

//The C reflection of org.apache.org.hadoop.FileSystem .
struct hdfs_internal;
typedef struct hdfs_internal* hdfsFS;

enum hdfsStreamType
{
    HDFS_STREAM_UNINITIALIZED = 0,
    HDFS_STREAM_INPUT = 1,
    HDFS_STREAM_OUTPUT = 2,
};

typedef struct hdfsFile_internal {
    void* file;
    enum hdfsStreamType type;
    int flags;
}hdfsFile_internal;

typedef struct hdfsFile_internal* hdfsFile;


typedef struct hdfsReadStatistics {
  uint64_t totalBytesRead;
  uint64_t totalLocalBytesRead;
  uint64_t totalShortCircuitBytesRead;
  uint64_t totalZeroCopyBytesRead;
}hdfsReadStatistics;
typedef const hdfsReadStatistics* chdfsReadStatistics_t;

struct hadoopRzOptions;
typedef struct hadoopRzOptions hadoopRzOptions;

struct hadoopRzBuffer;
typedef struct hadoopRzBuffer hadoopRzBuffer;
typedef const hadoopRzBuffer* chadoopRzBuffer_t;

#ifdef __cplusplus
}
#endif
