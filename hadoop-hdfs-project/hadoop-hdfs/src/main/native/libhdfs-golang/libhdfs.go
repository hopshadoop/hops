package main

/*
#include "libhdfs_def.h"
void setErrno(int err);
int cGetTId();
*/
import (
	"C"
)

import (
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/colinmarc/hdfs/v2"
)

const defaultFilePermissions os.FileMode = 0666
const defaultDirPermissions os.FileMode = 0777

const LIBHDFS_ENABLE_LOG = "LIBHDFS_ENABLE_LOG"
const LIBHDFS_LOG_FILE = "LIBHDFS_LOG_FILE"

const LIBHDFS_ROOT_CA_BUNDLE = "LIBHDFS_ROOT_CA_BUNDLE"
const LIBHDFS_CLIENT_CERTIFICATE = "LIBHDFS_CLIENT_CERTIFICATE"
const LIBHDFS_CLIENT_KEY = "LIBHDFS_CLIENT_KEY"

const LIBHDFS_DEFAULT_FS = "LIBHDFS_DEFAULT_FS"
const LIBHDFS_DEFAULT_USER = "LIBHDFS_DEFAULT_USER"

// ------------------------------------------------------------------------------
func main() {
}

// ------------------------------------------------------------------------------

/**
* returns version string. Caller is reponsible for de-allocating the version string
* @return         version string. Caller is reponsible for
                  de-allocating the version string
*/
//export libhdfs_version
func libhdfs_version() *C.cchar_t {
	version := fmt.Sprintf("Version %s. Git commit %s, Build time: %s, "+
		"Hostname: %s, Branch %s\n   ",
		VERSION, GITCOMMIT, BUILDTIME, HOSTNAME, BRANCH)
	fmt.Print(version)
	return C.CString(version)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// struct hdfsBuilder *hdfsNewBuilder(void);
/**
 * Create an HDFS builder.
 *
 * @return The HDFS builder, or NULL on error.
 */
//export hdfsNewBuilder
func hdfsNewBuilder() *C.hdfsBuilder {
	init_logger()
	DEBUG("hdfsNewBuilder")
	hdfsBuilder := (*C.hdfsBuilder)(C.calloc(1, C.size_t(C.sizeof_hdfsBuilder)))
	if hdfsBuilder == nil {
		C.setErrno(C.int(syscall.ENOMEM))
		ERROR("ENOMEM in hdfsNewBuilder")
		return nil
	}

	return hdfsBuilder
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn);

/**
 * Set the HDFS NameNode to connect to.
 *
 * @param bld  The HDFS builder
 * @param nn   The NameNode to use.
 *
 *             If the string given is 'default', the default NameNode
 *             configuration will be used (from the XML configuration files)
 *
 *             If NULL is given, a LocalFileSystem will be created.
 *
 *             If the string starts with a protocol type such as file:// or
 *             hdfs://, this protocol type will be used.  If not, the
 *             hdfs:// protocol type will be used.
 *
 *             You may specify a NameNode port in the usual way by
 *             passing a string of the format hdfs://<hostname>:<port>.
 *             Alternately, you may set the port with
 *             hdfsBuilderSetNameNodePort.  However, you must not pass the
 *             port in two different ways.
 */
//export hdfsBuilderSetNameNode
func hdfsBuilderSetNameNode(bld *C.hdfsBuilder, nn *C.cchar_t) {
	DEBUG("hdfsBuilderSetNameNode: NN: %s", C.GoString(nn))
	bld.nn = nn
}

//-------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port);

/**
 * Set the port of the HDFS NameNode to connect to.
 *
 * @param bld The HDFS builder
 * @param port The port.
 */
//export hdfsBuilderSetNameNodePort
func hdfsBuilderSetNameNodePort(bld *C.hdfsBuilder, port C.tPort) {
	DEBUG("hdfsBuilderSetNameNodePort. Port: %d", port)
	bld.port = port
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName);

/**
 * Set the username to use when connecting to the HDFS cluster.
 *
 * @param bld The HDFS builder
 * @param userName The user name.  The string will be shallow-copied.
 */
//export hdfsBuilderSetUserName
func hdfsBuilderSetUserName(bld *C.hdfsBuilder, userName *C.cchar_t) {
	DEBUG("hdfsBuilderSetUserName. User: '%s'", C.GoString(userName))
	bld.userName = userName
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld,
// const char *kerbTicketCachePath);

/**
 * Set the path to the Kerberos ticket cache to use when connecting to
 * the HDFS cluster.
 *
 * @param bld The HDFS builder
 * @param kerbTicketCachePath The Kerberos ticket cache path.  The string
 *                            will be shallow-copied.
 */
//export hdfsBuilderSetKerbTicketCachePath
func hdfsBuilderSetKerbTicketCachePath(bld *C.hdfsBuilder,
	kerbTicketCachePath *C.cchar_t) {
	setCErrno(syscall.ENOSYS)
	ERROR("ERROR: hdfsBuilderSetKerbTicketCachePath." +
		" Kerberos is not supported in HopsFS")
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld);

/**
 * Force the builder to always create a new instance of the FileSystem,
 * rather than possibly finding one in the cache.
 *
 * @param bld The HDFS builder
 */
//export hdfsBuilderSetForceNewInstance
func hdfsBuilderSetForceNewInstance(bld *C.hdfsBuilder) {
	DEBUG("hdfsBuilderSetForceNewInstance")
	bld.forceNewInstance = 1
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
// const char *val);

/**
 * Set a configuration string for an HdfsBuilder.
 *
 * @param key      The key to set.
 * @param val      The value, or NULL to set no value.
 *                 This will be shallow-copied.  You are responsible for
 *                 ensuring that it remains valid until the builder is
 *                 freed.
 *
 * @return         0 on success; nonzero error code otherwise.
 */
//export hdfsBuilderConfSetStr
func hdfsBuilderConfSetStr(bld *C.hdfsBuilder, key *C.cchar_t, val *C.cchar_t) {
	ERROR("hdfsBuilderConfSetStr. Ignoring request to set client properties "+
		" because go client does not support it.  Property: %s, Value: %s",
		C.GoString(key), C.GoString(val))
}

//------------------------------------------------------------------------------

type HdfsClientWrapper struct {
	client        *hdfs.Client
	useURIPrepend bool
	uriPrepend    string

	//this is absolute path without scheme and host:port information
	currentWorkingDir string

	serverDefaults hdfs.ServerDefaults
	mutex          sync.Mutex
}

// to keep the clients from GC
var hdfsClients = make(map[*hdfs.Client]*HdfsClientWrapper)

// LIBHDFS_EXTERNAL
// hdfsFS hdfsConnect(const char* nn, tPort port);

/**
 * hdfsConnect - Connect to a hdfs file system.
 * Connect to the hdfs.
 * @param nn   The NameNode.  See hdfsBuilderSetNameNode for details.
 * @param port The port on which the server is listening.
 * @return Returns a handle to the filesystem or NULL on error.
 * @deprecated Use hdfsBuilderConnect instead.
 */
//export hdfsBuilderConnect
func hdfsBuilderConnect(bld *C.hdfsBuilder) C.hdfsFS {

	//store the client to prevent GC
	hdfsClientWrapper := HdfsClientWrapper{}

	var nn = C.GoString(bld.nn)
	if nn == "default" || nn == "" {
		defaultFS := os.Getenv(LIBHDFS_DEFAULT_FS)
		if defaultFS != "" {
			nn = defaultFS
		}
	}

	scheme, hostname, port, err := parseURL(nn)
	if err != nil {
		setCErrno(err)
		return nil
	}

	if port != "" {
		portInt, err := strconv.Atoi(port)
		if err == nil {
			bld.port = C.tPort(portInt)
		}
	}

	var address string
	if scheme != "" {
		hdfsClientWrapper.useURIPrepend = true
		hdfsClientWrapper.uriPrepend = fmt.Sprintf("hdfs://%s:%d", hostname, bld.port)
	}
	address = fmt.Sprintf("%s:%d", hostname, int(bld.port))

	DEBUG("hdfsBuilderConnect  %s", address)

	//if the user is not set then set the current user
	if bld.userName == nil {
		user, err := user.Current()
		if err != nil {
			setCErrno(err)
			return nil
		}
		bld.userName = C.CString(user.Username)
	}

	hdfsOptions := hdfs.ClientOptions{
		Addresses: []string{address},
		User:      C.GoString(bld.userName),
	}

	err = setTLSFromEnvVariables(&hdfsOptions)
	if err != nil {
		ERROR("%v", err)
		// failed to set TLS from ENV
		// try reading from core-site.xml
		err = setTLSFromHadoopConfigFiles(C.GoString(bld.userName), &hdfsOptions)
		if err != nil {
			ERROR("%v", err)
			INFO("Connecting without SSL. User: '%s'", C.GoString(bld.userName))
		}
	}

	hdfsClient, err := hdfs.NewClient(hdfsOptions)
	if err != nil {
		setCErrno(err)
		ERROR("Failed to connect to hdfs. hdfsBuilderConnect ")
		return nil
	} else {
		hdfsClientWrapper.client = hdfsClient
		INFO("Connected to hdfs. %s", address)
	}

	props, err := hdfsClient.ServerDefaults()
	if err != nil {
		setCErrno(err)
		ERROR("Failed to get server defaults. hdfsBuilderConnect ")
		return nil

	}

	hdfsClientWrapper.serverDefaults = props

	var userName string
	if bld.userName == nil || C.GoString(bld.userName) == "" {
		u, err := user.Current()
		if err != nil {
			ERROR("Failed to get system user in hdfsBuilderConnect fn. Err: %v", err)
			setCErrno(err)
			return nil
		}
		userName = u.Name
	} else {
		userName = C.GoString(bld.userName)
	}
	hdfsClientWrapper.currentWorkingDir = fmt.Sprintf("/user/%s", userName)

	hdfsClients[hdfsClient] = &hdfsClientWrapper
	return (C.hdfsFS)(unsafe.Pointer(hdfsClient))
}

func parseURL(urlStr string) (scheme, hostName, port string, err error) {

	var parsedURL *url.URL
	if !strings.Contains(urlStr, "://") {
		parsedURL = &url.URL{
			Scheme: "dummyScheme",
			Host:   urlStr,
		}
	} else {
		parsedURL, err = url.Parse(urlStr)
		if err != nil {
			return
		}
	}

	scheme = parsedURL.Scheme
	if scheme == "dummyScheme" {
		scheme = ""
	} else if scheme != "hdfs" {
		err = fmt.Errorf("wrong url scheme in %s. Scheme: %s", urlStr, scheme)
		return
	}

	hostName = parsedURL.Hostname()
	port = parsedURL.Port()
	return
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsCreateDirectory(hdfsFS fs, const char* path);

/**
 * hdfsCreateDirectory - Make the given file and all non-existent
 * parents into directories.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsCreateDirectory
func hdfsCreateDirectory(fs C.hdfsFS, path *C.cchar_t) C.int {
	DEBUG("hdfsCreateDirectory. %s ", C.GoString(path))

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))
	err := hdfsClientWrapper.client.MkdirAll(parsedPath, defaultDirPermissions)
	if err != nil {
		setCErrno(err)
		ERROR("Create dir failed, Error: %v\n", err)
		return -1
	}
	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsDelete(hdfsFS fs, const char* path, int recursive);

/**
 * hdfsDelete - Delete file.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @param recursive if path is a directory and set to
 * non-zero, the directory is deleted else throws an exception. In
 * case of a file the recursive argument is irrelevant.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsDelete
func hdfsDelete(fs C.hdfsFS, path *C.cchar_t, recursive C.int) C.int {
	DEBUG("hdfsDelete. Is recursive: %d", recursive)

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))

	var err error
	if recursive == 0 {
		err = hdfsClientWrapper.client.Remove(parsedPath)
	} else {
		err = hdfsClientWrapper.client.RemoveAll(parsedPath)
	}

	if err != nil {
		setCErrno(err)
		ERROR("Delete failed, Path: %s, Is recursive: %d,  Error: %v\n",
			C.GoString(path), recursive, err)
		return -1
	}

	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsDisconnect(hdfsFS fs);

/**
 * hdfsDisconnect - Disconnect from the hdfs file system.
 * Disconnect from hdfs.
 * @param fs The configured filesystem handle.
 * @return Returns 0 on success, -1 on error.
 *         Even if there is an error, the resources associated with the
 *         hdfsFS will be freed.
 */
//export hdfsDisconnect
func hdfsDisconnect(fs C.hdfsFS) C.int {
	DEBUG("hdfsDisconnect")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	if hdfsClientWrapper != nil {
		err := hdfsClientWrapper.client.Close()
		if err != nil {
			setCErrno(err)
			return -1
		}

		delete(hdfsClients, hdfsClientWrapper.client)
	}

	shutdown_logger()

	return 0
}

//------------------------------------------------------------------------------

// int hdfsExists(hdfsFS fs, const char *path)
//
//export hdfsExists
func hdfsExists(fs C.hdfsFS, path *C.cchar_t) C.int {
	DEBUG("hdfsExists")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))

	stat, err := hdfsClientWrapper.client.Stat(parsedPath)
	if stat == nil || err != nil {
		ERROR("hdfsExists failed. Path %s. Error: %v", C.GoString(path), err)
		return -1
	}
	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries);

/**
 * hdfsFreeFileInfo - Free up the hdfsFileInfo array (including fields)
 * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
 * objects.
 * @param numEntries The size of the array.
 */
//export hdfsFreeFileInfo
func hdfsFreeFileInfo(hdfsFileInfoParam *C.hdfsFileInfo, numEntries C.int) {
	DEBUG("hdfsFreeFileInfo")

	hdfsFileInfoSlice := unsafe.Slice((*C.hdfsFileInfo)(
		unsafe.Pointer(hdfsFileInfoParam)), numEntries)

	//Free the mName, mOwner, and mGroup
	for _, hdfsFileInfo := range hdfsFileInfoSlice {
		C.free(unsafe.Pointer(hdfsFileInfo.mName))
		C.free(unsafe.Pointer(hdfsFileInfo.mOwner))
		C.free(unsafe.Pointer(hdfsFileInfo.mGroup))
	}

	//Free entire block
	C.free(unsafe.Pointer(hdfsFileInfoParam))
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// tOffset hdfsGetCapacity(hdfsFS fs);

/**
 * hdfsGetCapacity - Return the raw capacity of the filesystem.
 * @param fs The configured filesystem handle.
 * @return Returns the raw-capacity; -1 on error.
 */
//export hdfsGetCapacity
func hdfsGetCapacity(fs C.hdfsFS) C.tOffset {
	DEBUG("hdfsGetCapacity")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	fsInfo, err := hdfsClientWrapper.client.StatFs()
	if err != nil {
		setCErrno(err)
		ERROR("hdfsGetCapacity. Error: %v", err)
		return -1
	}

	return C.tOffset(fsInfo.Capacity)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// tOffset hdfsGetUsed(hdfsFS fs);

/**
 * hdfsGetUsed - Return the total raw size of all files in the filesystem.
 * @param fs The configured filesystem handle.
 * @return Returns the total-size; -1 on error.
 */
//export hdfsGetUsed
func hdfsGetUsed(fs C.hdfsFS) C.tOffset {
	DEBUG("hdfsGetUsed")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	fsInfo, err := hdfsClientWrapper.client.StatFs()
	if err != nil {
		setCErrno(err)
		ERROR("hdfsGetUsed. Error: %v", err)
		return -1
	}

	return C.tOffset(fsInfo.Used)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path);

/**
 * hdfsGetPathInfo - Get information about a path as a (dynamically
 * allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
 * called when the pointer is no longer needed.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns a dynamically-allocated hdfsFileInfo object;
 * NULL on error.
 */
//export hdfsGetPathInfo
func hdfsGetPathInfo(fs C.hdfsFS, path *C.cchar_t) *C.hdfsFileInfo {
	DEBUG("hdfsGetPathInfo Path: %s", C.GoString(path))

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))

	if hdfsClientWrapper.client == nil {
		ERROR("hdfsclient is null")
		return nil
	}

	goFileInfo, err := hdfsClientWrapper.client.Stat(parsedPath)
	if err != nil {
		setCErrno(err)
		ERROR("hdfsGetPathInfo. Error: %v", err)
		return nil
	}

	cFileInfo := (*C.hdfsFileInfo)(C.calloc(1, C.size_t(C.sizeof_hdfsFileInfo)))
	if cFileInfo == nil {
		C.setErrno(C.int(syscall.ENOMEM))
		ERROR("ENOMEM in hdfsGetPathInfo")
		return nil
	}

	var completeBasePath string
	if hdfsClientWrapper.useURIPrepend {
		completeBasePath = fmt.Sprintf("%s%s", hdfsClientWrapper.uriPrepend, parsedPath)
	} else {
		completeBasePath = parsedPath
	}

	convertFileInfo(completeBasePath, goFileInfo, cFileInfo)
	return cFileInfo
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path,
// int *numEntries);

/**
 * hdfsListDirectory - Get list of files/directories for a given
 * directory-path. hdfsFreeFileInfo should be called to deallocate memory.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @param numEntries Set to the number of files/directories in path.
 * @return Returns a dynamically-allocated array of hdfsFileInfo
 * objects; NULL on error.
 */
//export hdfsListDirectory
func hdfsListDirectory(fs C.hdfsFS, path *C.cchar_t,
	numEntries *C.int) *C.hdfsFileInfo {
	DEBUG("hdfsListDirectory. Path: %s", C.GoString(path))

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))

	goFileInfos, err := hdfsClientWrapper.client.ReadDir(parsedPath)
	if err != nil {
		setCErrno(err)
		return nil
	}

	length := C.ulong(len(goFileInfos))
	cFileInfosPtr := (*C.hdfsFileInfo)(C.calloc(length,
		C.size_t(C.sizeof_hdfsFileInfo)))
	if cFileInfosPtr == nil {
		C.setErrno(C.int(syscall.ENOMEM))
		ERROR("Error ENOMEM in hdfsGetPathInfo")
		return nil
	}

	cFileInfosSlice := (*[1 << 30]C.hdfsFileInfo)(
		unsafe.Pointer(cFileInfosPtr))[:length:length]

	var completeBasePath string
	if hdfsClientWrapper.useURIPrepend {
		completeBasePath = fmt.Sprintf("%s%s",
			hdfsClientWrapper.uriPrepend, parsedPath)
	} else {
		completeBasePath = parsedPath
	}

	for i := 0; i < int(length); i++ {
		convertFileInfo(completeBasePath, goFileInfos[i], &cFileInfosSlice[i])
	}
	*numEntries = C.int(length)

	return cFileInfosPtr
}

//------------------------------------------------------------------------------

func convertFileInfo(basePath string, goFileInfo os.FileInfo,
	cFileInfo *C.hdfsFileInfo) {
	hdfsFileInfo := goFileInfo.(*hdfs.FileInfo)
	hdfsFileInfoProto := hdfsFileInfo.Sys().(*hdfs.FileStatus)
	cFileInfo.mBlockSize = C.tOffset(*hdfsFileInfoProto.Blocksize)
	if goFileInfo.IsDir() {
		cFileInfo.mKind = C.kObjectKindDirectory
	} else {
		cFileInfo.mKind = C.kObjectKindFile
	}

	cFileInfo.mLastAccess = C.tTime(*hdfsFileInfoProto.AccessTime / 1000)
	cFileInfo.mLastMod = C.tTime(*hdfsFileInfoProto.ModificationTime / 1000)

	cFileInfo.mPermissions = C.short(*hdfsFileInfoProto.Permission.Perm)
	cFileInfo.mReplication = C.short(*hdfsFileInfoProto.BlockReplication)
	cFileInfo.mSize = C.tOffset(*hdfsFileInfoProto.Length)

	// hdfs return complete path
	cFileInfo.mName = C.CString(completeFilePath(basePath, string(hdfsFileInfoProto.Path)))
	cFileInfo.mGroup = C.CString(hdfsFileInfo.OwnerGroup())
	cFileInfo.mOwner = C.CString(hdfsFileInfo.Owner())
}

// ------------------------------------------------------------------------------

func completeFilePath(parent, name string) string {
	if parent == "" {
		parent = "/"
	}

	return fmt.Sprintf("%s/%s", parent, name)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsChown(hdfsFS fs, const char* path, const char *owner,
// const char *group);

/**
 * Change the user and/or group of a file or directory.
 *
 * @param fs            The configured filesystem handle.
 * @param path          the path to the file or directory
 * @param owner         User string.  Set to NULL for 'no change'
 * @param group         Group string.  Set to NULL for 'no change'
 * @return              0 on success else -1
 */
//export hdfsChown
func hdfsChown(fs C.hdfsFS, path *C.cchar_t, owner *C.cchar_t,
	group *C.cchar_t) C.int {
	DEBUG("hdfsChown")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))
	err := hdfsClientWrapper.client.Chown(
		parsedPath,
		C.GoString(owner),
		C.GoString(group))
	if err != nil {
		setCErrno(err)
		ERROR("hdfsChown. Error: %v", err)
		return -1
	}
	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsChmod(hdfsFS fs, const char* path, short mode);

/**
 * hdfsChmod
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mode the bitmask to set it to
 * @return 0 on success else -1
 */
//export hdfsChmod
func hdfsChmod(fs C.hdfsFS, path *C.cchar_t, mode *C.short) C.int {
	DEBUG("hdfsChmod")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))
	err := hdfsClientWrapper.client.Chmod(parsedPath, os.FileMode(*mode))
	if err != nil {
		setCErrno(err)
		ERROR("hdfsChmod. Error: %v", err)
		return -1
	}
	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsCloseFile(hdfsFS fs, hdfsFile file);

/**
 * hdfsCloseFile - Close an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 *         On error, errno will be set appropriately.
 *         If the hdfs file was valid, the memory associated with it will
 *         be freed at the end of this call, even if there was an I/O
 *         error.
 */
//export hdfsCloseFile
func hdfsCloseFile(fs C.hdfsFS, file C.hdfsFile) C.int {
	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	DEBUG("hdfsCloseFile. type %d.", file._type)
	var fileWriter *hdfs.FileWriter
	var fileReader *hdfs.FileReader
	var err error

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	if file._type == C.HDFS_STREAM_OUTPUT {
		fileWriter = (*hdfs.FileWriter)(file.file)
		err = fileWriter.Close()
	} else if file._type == C.HDFS_STREAM_INPUT {
		fileReader = (*hdfs.FileReader)(file.file)
		err = fileReader.Close()
	} else {
		setCErrno(syscall.EBADF)
		ERROR("File flush failed. Bad file descriptor")
		return -1
	}

	if err != nil {
		setCErrno(err)
		ERROR("File close failed. Error: %v", err)
		return -1
	}

	if fileWriter != nil {
		delete(hdfsReadersWriters, unsafe.Pointer(fileWriter))
	}

	if fileReader != nil {
		delete(hdfsReadersWriters, unsafe.Pointer(fileReader))
	}

	C.free(unsafe.Pointer(file))
	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsFlush(hdfsFS fs, hdfsFile file);

/**
 * hdfsWrite - Flush the data.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsFlush
func hdfsFlush(fs C.hdfsFS, file C.hdfsFile) C.int {
	DEBUG("hdfsFlush")

	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	var fileWriter *hdfs.FileWriter
	if file._type == C.HDFS_STREAM_OUTPUT {
		fileWriter = (*hdfs.FileWriter)(file.file)
	} else {
		setCErrno(syscall.EBADF)
		ERROR("File flush failed. Bad file descriptor")
		return -1
	}

	err := fileWriter.Flush()
	if err != nil {
		setCErrno(err)
		ERROR("ERROR: File flush failed. Error: %v", err)
		return -1
	}

	return 0
}

//------------------------------------------------------------------------------

type ReaderWriterWithLock struct {
	mutex        sync.Mutex
	readerWriter interface{}
}

var hdfsReadersWriters = make(map[unsafe.Pointer]*ReaderWriterWithLock)

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
// int bufferSize, short replication, tSize blocksize);

/**
 * hdfsOpenFile - Open a hdfs file in given mode.
 * @param fs The configured filesystem handle.
 * @param path The full path to the file.
 * @param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY,
 * O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 * O_WRONLY|O_APPEND. Other flags are generally ignored other than
 * (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param bufferSize Size of buffer for read/write - pass 0 if you want
 * to use the default configured values.
 * @param replication Block replication - pass 0 if you want to use
 * the default configured values.
 * @param blocksize Size of block - pass 0 if you want to use the
 * default configured values.
 * @return Returns the handle to the open file or NULL on error.
 */
//export hdfsOpenFile
func hdfsOpenFile(fs C.hdfsFS, path *C.cchar_t, flags C.int,
	bufferSize C.int, replication C.short, blockSize C.tSize) C.hdfsFile {

	DEBUG("hdfsOpenFile. Path: %s. Replication: %d, Blk Size: %d, Buff Size: %d,"+
		" flag %d", C.GoString(path), replication, blockSize, bufferSize, flags)

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsePath := parsePath(hdfsClientWrapper, C.GoString(path))

	if replication == 0 {
		replication = C.short(hdfsClientWrapper.serverDefaults.Replication)
	}

	if blockSize == 0 {
		blockSize = C.int(hdfsClientWrapper.serverDefaults.BlockSize)
	}

	if bufferSize == 0 {
		bufferSize = C.int(hdfsClientWrapper.serverDefaults.FileBufferSize)
	}

	var overwrite = true

	accmode := int(flags) & syscall.O_ACCMODE

	if accmode == os.O_RDONLY || accmode == os.O_WRONLY {
		// yay
	} else if accmode == os.O_RDWR {
		//errno = ENOTSUP
		ERROR("hdfsOpenFile. Cannot open an hdfs file in O_RDWR mode\n")
		C.setErrno(C.int(syscall.ENOTSUP))
		return nil
	} else {
		// errno = EINVAL
		ERROR("hdfsOpenFile. Cannot open an hdfs file in mode 0x%x\n", flags)
		C.setErrno(C.int(syscall.EINVAL))
		return nil
	}

	if ((int(flags) & os.O_CREATE) == os.O_CREATE) &&
		((int(flags) & os.O_EXCL) == os.O_EXCL) {
		ERROR("ERROR: hdfs does not truly support O_CREATE && O_EXCL\n")
	}

	file := (C.hdfsFile)(C.calloc(1, C.size_t(C.sizeof_hdfsFile_internal)))
	if file == nil {
		C.setErrno(C.int(syscall.ENOMEM))
		ERROR("ENOMEM in hdfsOpenFile")
		return nil
	}

	var method string
	var fileWriter *hdfs.FileWriter
	var fileReader *hdfs.FileReader
	var err error
	if accmode == os.O_RDONLY {
		method = "open"
		fileReader, err = hdfsClientWrapper.client.Open(parsePath)
	} else if (int(flags) & os.O_APPEND) == os.O_APPEND {
		method = "append"
		fileWriter, err = hdfsClientWrapper.client.Append(parsePath)
	} else {
		method = "create"
		fileWriter, err = hdfsClientWrapper.client.CreateFile(parsePath,
			int(replication), int64(blockSize), defaultFilePermissions, true)
	}
	INFO("hdfsOpenFile. Path: %s. method: %s, ", parsePath, method)

	if err != nil {
		setCErrno(err)
		ERROR("hdfsOpenFile failed. Path: %s. Replication: %d,"+
			" Blk Size: %d, Perm: %d, Overwrite %t, Method: %s, "+
			"  Flags %d, Error: %v", parsePath, replication, blockSize,
			defaultFilePermissions, overwrite, method, flags, err)
		return nil
	}

	if fileReader != nil {
		//Store the fileWriter or file reader is a global map to prevent GC
		hdfsReadersWriters[unsafe.Pointer(fileReader)] = &ReaderWriterWithLock{mutex: sync.Mutex{}, readerWriter: fileReader}
		file.file = unsafe.Pointer(fileReader)
		file._type = C.HDFS_STREAM_INPUT
	} else if fileWriter != nil {
		//Store the fileWriter or file reader is a global map to prevent GC
		hdfsReadersWriters[unsafe.Pointer(fileWriter)] = &ReaderWriterWithLock{mutex: sync.Mutex{}, readerWriter: fileWriter}
		file.file = unsafe.Pointer(fileWriter)
		file._type = C.HDFS_STREAM_OUTPUT
	} else {
		ERROR("Programming error in hdfsOpenFile")
		return nil
	}
	file.flags = flags

	return file
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);

/**
 * hdfsRead - Read data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return      On success, a positive number indicating how many bytes
 *              were read.
 *              On end-of-file, 0.
 *              On error, -1.  Errno will be set to the error code.
 *              Just like the POSIX read function, hdfsRead will return -1
 *              and set errno to EINTR if data is temporarily unavailable,
 *              but we are not yet at the end of the file.
 */
//export hdfsRead
func hdfsRead(fs C.hdfsFS, file C.hdfsFile, buffer *C.void, length C.tSize) C.tSize {
	DEBUG("hdfsRead")

	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	var fileReader *hdfs.FileReader
	if file._type == C.HDFS_STREAM_INPUT {
		fileReader = (*hdfs.FileReader)(file.file)
	} else {
		setCErrno(syscall.EBADF)
		ERROR("File read failed. Bad file descriptor")
		return -1
	}

	if length < 0 {
		return -1
	}
	if length == 0 {
		return 0
	}

	goBytes := (*[1 << 30]byte)(unsafe.Pointer(buffer))[:length:length]
	read, err := fileReader.Read(goBytes)
	if err != nil && err != io.EOF {
		setCErrno(err)
		ERROR("File read failed. Error: %v", err)
		return -1
	}

	return C.tSize(read)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos);

/**
 * hdfsSeek - Seek to given offset in file.
 * This works only for files opened in read-only mode.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsSeek
func hdfsSeek(fs C.hdfsFS, file C.hdfsFile, desiredPos C.tOffset) C.int {
	DEBUG("hdfsSeek")
	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	var fileReader *hdfs.FileReader
	if file._type == C.HDFS_STREAM_INPUT {
		fileReader = (*hdfs.FileReader)(file.file)
	} else {
		setCErrno(syscall.EBADF)
		ERROR("File seek failed. Bad file descriptor")
		return -1
	}

	_, err := fileReader.Seek(int64(desiredPos), 0)
	if err != nil {
		setCErrno(err)
		ERROR("File seek failed. Error %v", err)
		return -1
	}

	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// tOffset hdfsTell(hdfsFS fs, hdfsFile file);

/**
 * hdfsTell - Get the current offset in the file, in bytes.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Current offset, -1 on error.
 */
//export hdfsTell
func hdfsTell(fs C.hdfsFS, file C.hdfsFile) C.tOffset {
	DEBUG("hdfsTell. for %d ", file._type)
	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	var fileReader *hdfs.FileReader
	var fileWriter *hdfs.FileWriter
	var pos int64 = -1
	if file._type == C.HDFS_STREAM_INPUT {
		fileReader = (*hdfs.FileReader)(file.file)
		pos = int64(fileReader.GetPos())
	} else if file._type == C.HDFS_STREAM_OUTPUT {
		fileWriter = (*hdfs.FileWriter)(file.file)
		pos = int64(fileWriter.GetPos())
	} else {
		setCErrno(syscall.EBADF)
		ERROR("hdfsTell failed. Bad file descriptor")
		return -1
	}

	return C.tOffset(pos)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer,
// tSize length);

/**
 * hdfsWrite - Write data into an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write.
 * @return Returns the number of bytes written, -1 on error.
 */
//export hdfsWrite
func hdfsWrite(fs C.hdfsFS, file C.hdfsFile, buffer *C.void, length C.tSize) C.tSize {
	DEBUG("hdfsWrite")

	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	var fileWriter *hdfs.FileWriter
	if file._type == C.HDFS_STREAM_OUTPUT {
		fileWriter = (*hdfs.FileWriter)(file.file)
	} else {
		setCErrno(syscall.EBADF)
		ERROR("File write failed. Bad file descriptor")
		return -1
	}

	if length < 0 {
		return -1
	}
	if length == 0 {
		return 0
	}

	goBuffer := (*[1 << 30]byte)(unsafe.Pointer(buffer))[:length:length]
	n, err := fileWriter.Write(goBuffer)
	if err != nil {
		setCErrno(err)
		ERROR("File write failed. Error: %v. Written: %d", err, n)
		return -1
	}

	return C.tSize(n)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath);

/**
 * hdfsRename - Rename file.
 * @param fs The configured filesystem handle.
 * @param oldPath The path of the source file.
 * @param newPath The path of the destination file.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsRename
func hdfsRename(fs C.hdfsFS, oldPath *C.cchar_t, newPath *C.cchar_t) C.int {
	DEBUG("hdfsRename")
	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	oldPathParsed := parsePath(hdfsClientWrapper, C.GoString(oldPath))
	newPathParsed := parsePath(hdfsClientWrapper, C.GoString(newPath))

	err := hdfsClientWrapper.client.Rename(oldPathParsed, newPathParsed)
	if err != nil {
		setCErrno(err)
		ERROR("Rename failed. Error: %v", err)
		return -1
	}

	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsAvailable(hdfsFS fs, hdfsFile file);

/**
 * hdfsAvailable - Number of bytes that can be read from this
 * input stream without blocking.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns available bytes; -1 on error.
 */
//export hdfsAvailable
func hdfsAvailable(fs C.hdfsFS, file C.hdfsFile) C.int {
	DEBUG("hdfsAvailable.")

	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	var fileReader *hdfs.FileReader
	if file._type == C.HDFS_STREAM_INPUT {
		fileReader = (*hdfs.FileReader)(file.file)
	} else {
		setCErrno(syscall.EBADF)
		ERROR("File read failed. Bad file descriptor")
		return -1
	}

	return C.int(fileReader.Available())
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// char*** hdfsGetHosts(hdfsFS fs, const char* path,
// tOffset start, tOffset length);

/**
 * hdfsGetHosts - Get hostnames where a particular block (determined by
 * pos & blocksize) of a file is stored. The last element in the array
 * is NULL. Due to replication, a single block could be present on
 * multiple hosts.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @param start The start of the block.
 * @param length The length of the block.
 * @return Returns a dynamically-allocated 2-d array of blocks-hosts;
 * NULL on error.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsGetHosts
func hdfsGetHosts(fs C.hdfsFS, path *C.cchar_t, start C.tOffset,
	length C.tOffset) ***C.char {
	// hdfsClientWrapper := getHdfsClientWrapper(fs)
	// hdfsClientWrapper.mutex.Lock()
	// defer hdfsClientWrapper.mutex.Unlock()

	// not implemented as it is not used by pyarrow
	ERROR("hdfsGetHosts. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return nil
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hdfsFreeHosts(char ***blockHosts);

/**
 * hdfsFreeHosts - Free up the structure returned by hdfsGetHosts
 * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
 * objects.
 * @param numEntries The size of the array.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFreeHosts
func hdfsFreeHosts(src ***C.char) {
	// not implemented as it is not used by pyarrow
	ERROR("hdfsFreeHosts. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// tOffset hdfsGetDefaultBlockSize(hdfsFS fs);

/**
 * hdfsGetDefaultBlockSize - Get the default blocksize.
 *
 * @param fs            The configured filesystem handle.
 * @deprecated          Use hdfsGetDefaultBlockSizeAtPath instead.
 *
 * @return              Returns the default blocksize, or -1 on error.
 */
//export hdfsGetDefaultBlockSize
func hdfsGetDefaultBlockSize(fs C.hdfsFS) C.tOffset {
	ERROR("hdfsGetDefaultBlockSize")
	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	return C.tOffset(hdfsClientWrapper.serverDefaults.BlockSize)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsSetWorkingDirectory(hdfsFS fs, const char* path);

/**
 * hdfsSetWorkingDirectory - Set the working directory. All relative
 * paths will be resolved relative to it.
 * @param fs The configured filesystem handle.
 * @param path The path of the new 'cwd'.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsSetWorkingDirectory
func hdfsSetWorkingDirectory(fs C.hdfsFS, path *C.cchar_t) C.int {
	DEBUG("hdfsSetWorkingDirectory")
	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))
	hdfsClientWrapper.currentWorkingDir = parsedPath
	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize);

/**
 * hdfsGetWorkingDirectory - Get the current working directory for
 * the given filesystem.
 * @param fs The configured filesystem handle.
 * @param buffer The user-buffer to copy path of cwd into.
 * @param bufferSize The length of user-buffer.
 * @return Returns buffer, NULL on error.
 */
//export hdfsGetWorkingDirectory
func hdfsGetWorkingDirectory(fs C.hdfsFS, buffer *C.char,
	bufferSize C.size_t) *C.char {

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	goCurrDir := hdfsClientWrapper.currentWorkingDir

	if hdfsClientWrapper.useURIPrepend {
		goCurrDir = fmt.Sprintf("%s%s", hdfsClientWrapper.uriPrepend, goCurrDir)
	}

	if len(goCurrDir) >= int(bufferSize) {
		setCErrno(syscall.ENAMETOOLONG)
		return nil
	}

	goBytes := (*[1 << 30]byte)(unsafe.Pointer(buffer))[:bufferSize:bufferSize]
	copied := copy(goBytes[:], goCurrDir)
	goBytes[copied] = 0x00
	DEBUG("hdfsGetWorkingDirectory. %s", goCurrDir)

	return buffer
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);

/**
 * hdfsCopy - Copy file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsCopy
func hdfsCopy(srcFS C.hdfsFS, src *C.cchar_t, dstFS C.hdfsFS, dst *C.cchar_t) C.int {
	DEBUG("hdfsCopy. src: %s, dst: %s", C.GoString(src), C.GoString(dst))
	return hdfsCopyInternal(srcFS, src, dstFS, dst)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);

/**
 * hdfsMove - Move file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsMove
func hdfsMove(srcFS C.hdfsFS, src *C.cchar_t, dstFS C.hdfsFS, dst *C.cchar_t) C.int {
	DEBUG("hdfsMove. src: %s, dst: %s", C.GoString(src), C.GoString(dst))
	ret := hdfsCopyInternal(srcFS, src, dstFS, dst)
	if ret == 0 {
		srcHdfsClientWrapper := getHdfsClientWrapper(srcFS)
		srcHdfsClientWrapper.mutex.Lock()
		defer srcHdfsClientWrapper.mutex.Unlock()

		srcParsedPath := parsePath(srcHdfsClientWrapper, C.GoString(src))
		err := srcHdfsClientWrapper.client.Remove(srcParsedPath)
		if err != nil {
			setCErrno(err)
			ERROR("ERROR: Failed to remove src file. File: %s.  Error: %v",
				srcParsedPath, err)
			return -1
		}
	}
	return ret
}

//------------------------------------------------------------------------------

func hdfsCopyInternal(srcFS C.hdfsFS, src *C.cchar_t, dstFS C.hdfsFS,
	dst *C.cchar_t) C.int {
	srcHdfsClientWrapper := getHdfsClientWrapper(srcFS)
	dstHdfsClientWrapper := getHdfsClientWrapper(dstFS)

	srcHdfsClientWrapper.mutex.Lock()
	defer srcHdfsClientWrapper.mutex.Unlock()

	dstHdfsClientWrapper.mutex.Lock()
	defer dstHdfsClientWrapper.mutex.Unlock()

	srcParsedPath := parsePath(srcHdfsClientWrapper, C.GoString(src))
	dstParsedPath := parsePath(dstHdfsClientWrapper, C.GoString(dst))

	// check if src if file or not
	info, err := srcHdfsClientWrapper.client.Stat(srcParsedPath)
	if err != nil {
		setCErrno(err)
		ERROR("Failed to stat src file. File: %s.  Error: %v", srcParsedPath, err)
		return -1
	}

	if info.IsDir() {
		setCErrno(syscall.EIO)
		ERROR("Unable to copy. Src is a directory. Src: %s.", srcParsedPath)
		return -1
	}

	srcFileReader, err := srcHdfsClientWrapper.client.Open(srcParsedPath)
	if err != nil {
		setCErrno(err)
		ERROR("Failed to open src file. File: %s.  Error: %v", srcParsedPath, err)
		return -1
	}
	defer srcFileReader.Close()

	dstFileWriter, err := dstHdfsClientWrapper.client.CreateFile(dstParsedPath,
		dstHdfsClientWrapper.serverDefaults.Replication,
		dstHdfsClientWrapper.serverDefaults.BlockSize, defaultFilePermissions, true)
	if err != nil {
		setCErrno(err)
		ERROR("Failed to open dst file. File: %s.  Error: %v", srcParsedPath, err)
		return -1
	}
	defer dstFileWriter.Close()

	for {
		buffer := make([]byte, 64*1024)
		read, readErr := srcFileReader.Read(buffer)
		if readErr != nil && readErr != io.EOF {
			setCErrno(readErr)
			ERROR("Failed to read from src file. File: %s.  Error: %v",
				srcParsedPath, readErr)
			return -1
		}

		if read > 0 {
			written, writeErr := dstFileWriter.Write(buffer[0:read])
			if err != nil {
				setCErrno(writeErr)
				ERROR("Failed to write to dst file. File: %s.  Error: %v",
					srcParsedPath, writeErr)
				return -1
			}

			if read != written {
				setCErrno(syscall.EIO)
				ERROR("Failed to write complete data to dst file. File: %s."+
					"  Expected: %d, Written: %d", srcParsedPath, read, written)
				return -1
			}
		}

		if readErr != nil && readErr == io.EOF {
			break
		}
	}

	return 0
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
// void* buffer, tSize length);

/**
 * hdfsPread - Positional read of data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param position Position from which to read
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return      See hdfsRead
 */
//export hdfsPread
func hdfsPread(fs C.hdfsFS, file C.hdfsFile, position C.tOffset,
	buffer *C.void, length C.tSize) C.tSize {

	DEBUG("hdfsPread. offset: %d, length: %d ", position, length)
	if fs == nil || file == nil {
		return -1
	}

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	hdfsReadersWriters[file.file].mutex.Lock()
	defer hdfsReadersWriters[file.file].mutex.Unlock()

	var fileReader *hdfs.FileReader
	if file._type == C.HDFS_STREAM_INPUT {
		fileReader = (*hdfs.FileReader)(file.file)
	} else {
		setCErrno(syscall.EBADF)
		ERROR("File pread failed. Bad file descriptor")
		return -1
	}

	if length < 0 {
		return -1
	}
	if length == 0 {
		return 0
	}

	_, err := fileReader.Seek(int64(position), 0)
	if err != nil {
		setCErrno(err)
		ERROR("File pread failed. Faild to seek to position %d, "+
			" Error: %v", position, err)
		return -1
	}

	goBytes := (*[1 << 30]byte)(unsafe.Pointer(buffer))[:length:length]
	read, err := fileReader.Read(goBytes)
	if err != nil && err != io.EOF {
		setCErrno(err)
		ERROR("File pread failed. Error: %v", err)
		return -1
	}

	return C.tSize(read)

}

//------------------------------------------------------------------------------

/**
 * Get C threadID
 */
func getTId() int {
	return int(C.cGetTId())
}

//------------------------------------------------------------------------------
// LIBHDFS_EXTERNAL
// int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication);

/**
 * hdfsSetReplication - Set the replication of the specified
 * file to the supplied value
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns 0 on success, -1 on error.
 */
//export hdfsSetReplication
func hdfsSetReplication(fs C.hdfsFS, path *C.cchar_t, replication C.int16_t) C.int {
	DEBUG("hdfsSetReplication.")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))
	ret, err := hdfsClientWrapper.client.SetReplication(parsedPath, int16(replication))
	if err != nil {
		setCErrno(err)
		ERROR("Failed to set replication. Path: %s,  Error: %v", parsedPath, err)
		return -1
	}

	if ret {
		return 0
	} else {
		return -1
	}
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime);

/**
 * hdfsUtime
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mtime new modification time or -1 for no change
 * @param atime new access time or -1 for no change
 * @return 0 on success else -1
 */
//export hdfsUtime
func hdfsUtime(fs C.hdfsFS, path *C.cchar_t, mtime C.tTime, atime C.tTime) C.int {
	DEBUG("hdfsUtime.")

	hdfsClientWrapper := getHdfsClientWrapper(fs)
	hdfsClientWrapper.mutex.Lock()
	defer hdfsClientWrapper.mutex.Unlock()

	parsedPath := parsePath(hdfsClientWrapper, C.GoString(path))
	err := hdfsClientWrapper.client.Chtimes(parsedPath, time.Unix(int64(atime), 0),
		time.Unix(int64(mtime), 0))
	if err != nil {
		ERROR("Failed to set time for %s. Error %v", C.GoString(path), err)
		setCErrno(err)
		return -1
	}

	return 0
}

//------------------------------------------------------------------------------

// This function tries to set errno based on "err"
func setCErrno(err error) {

	// life is simple if err is syscall.Errno
	if vale, ok := err.(syscall.Errno); ok {
		DEBUG("syscall.Errno %v", C.int(vale))
		C.setErrno(C.int(vale))
		return
	}

	myErr := err
	// if err is os.PathError
	if val, ok := err.(*os.PathError); ok {
		unwrappedErr := val.Unwrap()
		if vale, ok := unwrappedErr.(syscall.Errno); ok {
			ERROR("Unwrapped Error. syscall.Errno %v", C.int(vale))
			C.setErrno(C.int(vale))
			return
		} else {
			myErr = unwrappedErr
		}
	}

	if myErr.Error() == fs.ErrInvalid.Error() {
		C.setErrno(C.int(syscall.EINVAL))
	} else if myErr.Error() == fs.ErrPermission.Error() {
		C.setErrno(C.int(syscall.EACCES))
	} else if myErr.Error() == fs.ErrExist.Error() {
		C.setErrno(C.int(syscall.EEXIST))
	} else if myErr.Error() == fs.ErrNotExist.Error() {
		C.setErrno(C.int(syscall.ENOENT))
	} else if myErr.Error() == fs.ErrClosed.Error() {
		C.setErrno(C.int(syscall.EBADF))
	} else {
		C.setErrno(C.int(syscall.EIO))
		ERROR("Unable to determine errno for \"%v\". "+
			" Setting errno to EIO", err)
		//ERROR("Stack :\"%s\". Setting errno to EIO", string(debug.Stack()))
	}
}

//------------------------------------------------------------------------------

// this function removes uri scheme, and host information if any from a given path
func parsePath(hdfsClientWrapper *HdfsClientWrapper, cpath string) string {
	parsedURL, err := url.Parse(cpath)
	if err != nil {
		setCErrno(err)
		ERROR("failed to parse path: %s, Error %v ", cpath, err)
		return cpath
	}

	// Get the scheme, host, and port
	scheme := parsedURL.Scheme
	hostname := parsedURL.Hostname()
	port := parsedURL.Port()
	qpath := parsedURL.Path

	if len(qpath) > 1 {
		qpath = path.Clean(qpath)
	}

	DEBUG("parsed path scheme: %s  hostname: %s port: %s path %s ",
		scheme, hostname, port, qpath)
	if len(qpath) != 0 {
		return fixRelativePath(hdfsClientWrapper, qpath)
	} else {
		return fixRelativePath(hdfsClientWrapper, cpath)
	}
}

//------------------------------------------------------------------------------

// it take a path and appends cwd to if the path is relative.
// the path should not contain sheme and hostname:port information
// this function is called from parsePath
func fixRelativePath(hdfsClientWrapper *HdfsClientWrapper, cpath string) string {
	var absPath string
	if path.IsAbs(cpath) {
		absPath = cpath
	} else {
		currDir := hdfsClientWrapper.currentWorkingDir
		absPath = filepath.Join(currDir, cpath)
	}

	DEBUG("Fixed Abs Path: %s", absPath)
	return absPath
}

//------------------------------------------------------------------------------

func getHdfsClientWrapper(fs C.hdfsFS) *HdfsClientWrapper {
	hdfsClientWrapper := hdfsClients[(*hdfs.Client)(unsafe.Pointer(fs))]
	if hdfsClientWrapper == nil {
		ERROR("hdfsclientwrapper is null")
		ERROR("%s", string(debug.Stack()))
		return nil
		// os.Exit(1)
	}
	return hdfsClientWrapper
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsFileIsOpenForRead(hdfsFile file);
/**
 * Determine if a file is open for read.
 *
 * @param file     The HDFS file
 * @return         1 if the file is open for read; 0 otherwise
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFileIsOpenForRead
func hdfsFileIsOpenForRead(file C.hdfsFile) C.int {
	ERROR("hdfsFileIsOpenForRead. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hdfsFileIsOpenForWrite(hdfsFile file);
/**
 * Determine if a file is open for write.
 *
 * @param file     The HDFS file
 * @return         1 if the file is open for write; 0 otherwise
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFileIsOpenForWrite
func hdfsFileIsOpenForWrite(file C.hdfsFile) C.int {
	ERROR("hdfsFileIsOpenForWrite. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hdfsFileGetReadStatistics(hdfsFile file,
//                                  struct hdfsReadStatistics **stats);
/**
 * Get read statistics about a file.  This is only applicable to files
 * opened for reading.
 *
 * @param file     The HDFS file
 * @param stats    (out parameter) on a successful return, the read
 *                 statistics.  Unchanged otherwise.  You must free the
 *                 returned statistics with hdfsFileFreeReadStatistics.
 * @return         0 if the statistics were successfully returned,
 *                 -1 otherwise.  On a failure, please check errno against
 *                 ENOTSUP.  webhdfs, LocalFilesystem, and so forth may
 *                 not support read statistics.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFileGetReadStatistics
func hdfsFileGetReadStatistics(file C.hdfsFile, stats **C.hdfsReadStatistics) C.int {
	ERROR("hdfsFileGetReadStatistics. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int64_t hdfsReadStatisticsGetRemoteBytesRead(
//                            const struct hdfsReadStatistics *stats);
/**
 * @param stats    HDFS read statistics for a file.
 *
 * @return the number of remote bytes read.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsReadStatisticsGetRemoteBytesRead
func hdfsReadStatisticsGetRemoteBytesRead(stats C.chdfsReadStatistics_t) C.int {
	ERROR("hdfsFileGetReadStatistics. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hdfsFileClearReadStatistics(hdfsFile file);
/**
 * Clear the read statistics for a file.
 *
 * @param file      The file to clear the read statistics of.
 *
 * @return          0 on success; the error code otherwise.
 *                  EINVAL: the file is not open for reading.
 *                  ENOTSUP: the file does not support clearing the read
 *                  statistics.
 *                  Errno will also be set to this code on failure.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFileClearReadStatistics
func hdfsFileClearReadStatistics(file C.hdfsFile) C.int {
	ERROR("hdfsFileClearReadStatistics. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats);
/**
 * Free some HDFS read statistics.
 *
 * @param stats    The HDFS read statistics to free.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFileFreeReadStatistics
func hdfsFileFreeReadStatistics(stats *C.hdfsReadStatistics) {
	ERROR("hdfsFileFreeReadStatistics. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------

//	LIBHDFS_EXTERNAL
//	hdfsFS hdfsConnectAsUser(const char* nn, tPort port, const char *user);
/**
 *  - hdfsConnectAsUser - Connect to a hdfs file system as a specific user
 *  - Connect to the hdfs.
 *  - @param nn   The NameNode.  See hdfsBuilderSetNameNode for details.
 *  - @param port The port on which the server is listening.
 *  - @param user the user name (this is hadoop domain user).
 *           Or NULL is equivelant to hhdfsConnect(host, port)
 *  - @return Returns a handle to the filesystem or NULL on error.
 *  - @deprecated Use hdfsBuilderConnect instead.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsConnectAsUser
func hdfsConnectAsUser(nn *C.cchar_t, port C.tPort, user *C.cchar_t) C.hdfsFS {
	ERROR("hdfsConnectAsUser. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return nil
}

//------------------------------------------------------------------------------

//     LIBHDFS_EXTERNAL
//     hdfsFS hdfsConnect(const char* nn, tPort port);
/**
* hdfsConnect - Connect to a hdfs file system.
* Connect to the hdfs.
* @param nn   The NameNode.  See hdfsBuilderSetNameNode for details.
* @param port The port on which the server is listening.
* @return Returns a handle to the filesystem or NULL on error.
* @deprecated Use hdfsBuilderConnect instead.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsConnect
func hdfsConnect(nn *C.cchar_t, port C.tPort, user *C.cchar_t) {
	ERROR("hdfsConnect. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------

//
//LIBHDFS_EXTERNAL
//hdfsFS hdfsConnectAsUserNewInstance(const char* nn, tPort port, const char *user );
/**
 * hdfsConnectAsUserNewInstance - Connect to an hdfs file system.
 *
 * Forces a new instance to be created
 *
 * @param nn     The NameNode.  See hdfsBuilderSetNameNode for details.
 * @param port   The port on which the server is listening.
 * @param user   The user name to use when connecting
 * @return       Returns a handle to the filesystem or NULL on error.
 * @deprecated   Use hdfsBuilderConnect instead.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsConnectAsUserNewInstance
func hdfsConnectAsUserNewInstance(nn *C.cchar_t, port C.tPort,
	user *C.cchar_t) C.hdfsFS {
	ERROR("hdfsConnectAsUserNewInstance. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return nil
}

//------------------------------------------------------------------------------

//     LIBHDFS_EXTERNAL
//     hdfsFS hdfsConnectNewInstance(const char* nn, tPort port);
/**
 * hdfsConnectNewInstance - Connect to an hdfs file system.
 *
 * Forces a new instance to be created
 *
 * @param nn     The NameNode.  See hdfsBuilderSetNameNode for details.
 * @param port   The port on which the server is listening.
 * @return       Returns a handle to the filesystem or NULL on error.
 * @deprecated   Use hdfsBuilderConnect instead.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsConnectNewInstance
func hdfsConnectNewInstance(nn *C.cchar_t, port C.tPort) C.hdfsFS {
	ERROR("hdfsConnectNewInstance. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return nil
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    void hdfsFreeBuilder(struct hdfsBuilder *bld);
/**
 * Free an HDFS builder.
 *
 * It is normally not necessary to call this function since
 * hdfsBuilderConnect frees the builder.
 *
 * @param bld The HDFS builder
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFreeBuilder
func hdfsFreeBuilder(bld *C.hdfsBuilder) {
	ERROR("hdfsFreeBuilder. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hdfsConfGetStr(const char *key, char **val);
/**
 * Get a configuration string.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will be set to NULL if the
 *                 key isn't found.  You must free this string with
 *                 hdfsConfStrFree.
 *
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsConfGetStr
func hdfsConfGetStr(key *C.cchar_t, val **C.char) C.int {
	ERROR("hdfsConfGetStr. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hdfsConfGetInt(const char *key, int32_t *val);
/**
 * Get a configuration integer.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will NOT be changed if the
 *                 key isn't found.
 *
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsConfGetInt
func hdfsConfGetInt(key *C.cchar_t, val *C.int) C.int {
	ERROR("hdfsConfGetInt. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    void hdfsConfStrFree(char *val);
/**
 * Free a configuration string found with hdfsConfGetStr.
 *
 * @param val      A configuration string obtained from hdfsConfGetStr
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsConfStrFree
func hdfsConfStrFree(val C.char) {
	ERROR("hdfsConfStrFree. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------

//    int hdfsTruncateFile(hdfsFS fs, const char* path, tOffset newlength);
/**
 * hdfsTruncateFile - Truncate a hdfs file to given lenght.
 * @param fs The configured filesystem handle.
 * @param path The full path to the file.
 * @param newlength The size the file is to be truncated to
 * @return 1 if the file has been truncated to the desired newlength
 *         and is immediately available to be reused for write operations
 *         such as append.
 *         0 if a background process of adjusting the length of the last
 *         block has been started, and clients should wait for it to
 *         complete before proceeding with further file updates.
 *         -1 on error.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsTruncateFile
func hdfsTruncateFile(fs C.hdfsFS, path *C.cchar_t, newlength C.tOffset) C.int {
	ERROR("hdfsTruncateFile. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hdfsUnbufferFile(hdfsFile file);
/**
 * hdfsUnbufferFile - Reduce the buffering done on a file.
 *
 * @param file  The file to unbuffer.
 * @return      0 on success
 *              ENOTSUP if the file does not support unbuffering
 *              Errno will also be set to this value.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsUnbufferFile
func hdfsUnbufferFile(file C.hdfsFile) C.int {
	ERROR("hdfsUnbufferFile. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hdfsHFlush(hdfsFS fs, hdfsFile file);
/**
 * hdfsHFlush - Flush out the data in client's user buffer. After the
 * return of this call, new readers will see the data.
 * @param fs configured filesystem handle
 * @param file file handle
 * @return 0 on success, -1 on error and sets errno
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsHFlush
func hdfsHFlush(fs C.hdfsFS, file C.hdfsFile) C.int {
	ERROR("hdfsHFlush. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hdfsHSync(hdfsFS fs, hdfsFile file);
/**
  - hdfsHSync - Similar to posix fsync, Flush out the data in client's
  - user buffer. all the way to the disk device (but the disk may have
  - it in its cache).
  - @param fs configured filesystem handle
  - @param file file handle
  - @return 0 on success, -1 on error and sets errno
*/
// ENOSYS. NOT IMLEMENTED
//export hdfsHSync
func hdfsHSync(fs C.hdfsFS, file C.hdfsFile) C.int {
	ERROR("hdfsHSync. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    int hdfsFileIsEncrypted(hdfsFileInfo *hdfsFileInfo);
/**
 * hdfsFileIsEncrypted: determine if a file is encrypted based on its
 * hdfsFileInfo.
 * @return -1 if there was an error (errno will be set), 0 if the file is
 *         not encrypted, 1 if the file is encrypted.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsFileIsEncrypted
func hdfsFileIsEncrypted(hdfsFileInfoParam *C.hdfsFileInfo) C.int {
	ERROR("hdfsFileIsEncrypted. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path);
/**
 * hdfsGetDefaultBlockSizeAtPath - Get the default blocksize at the
 * filesystem indicated by a given path.
 *
 * @param fs            The configured filesystem handle.
 * @param path          The given path will be used to locate the actual
 *                      filesystem.  The full path does not have to exist.
 *
 * @return              Returns the default blocksize, or -1 on error.
 */
// ENOSYS. NOT IMLEMENTED
//export hdfsGetDefaultBlockSizeAtPath
func hdfsGetDefaultBlockSizeAtPath(fs C.hdfsFS, path *C.cchar_t) C.tOffset {
	ERROR("hdfsGetDefaultBlockSizeAtPath. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// struct hadoopRzOptions *hadoopRzOptionsAlloc(void);
/**
  - Allocate a zero-copy options structure.
    *
  - You must free all options structures allocated with this function using
  - hadoopRzOptionsFree.
    *
  - @return            A zero-copy options structure, or NULL if one could
  - not be allocated.  If NULL is returned, errno will
  - contain the error number.
*/
// ENOSYS. NOT IMLEMENTED
//export hadoopRzOptionsAlloc
func hadoopRzOptionsAlloc() *C.hadoopRzOptions {
	ERROR("hadoopRzOptionsAlloc. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return nil
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    int hadoopRzOptionsSetSkipChecksum(
//            struct hadoopRzOptions *opts, int skip);
/**
 * Determine whether we should skip checksums in read0.
 *
 * @param opts        The options structure.
 * @param skip        Nonzero to skip checksums sometimes; zero to always
 *                    check them.
 *
 * @return            0 on success; -1 plus errno on failure.
 */
// ENOSYS. NOT IMLEMENTED
//export hadoopRzOptionsSetSkipChecksum
func hadoopRzOptionsSetSkipChecksum(opts *C.hadoopRzOptions, skip C.int) C.int {
	ERROR("hadoopRzOptionsSetSkipChecksum. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int hadoopRzOptionsSetByteBufferPool(
//	struct hadoopRzOptions *opts, const char *className);

/**
- Set the ByteBufferPool to use with read0.
  *
- @param opts        The options structure.
- @param className   If this is NULL, we will not use any
- ByteBufferPool.  If this is non-NULL, it will be
- treated as the name of the pool class to use.
- For example, you can use
- ELASTIC_BYTE_BUFFER_POOL_CLASS.
  *
- @return            0 if the ByteBufferPool class was found and
- instantiated;
- -1 plus errno otherwise.
*/
// ENOSYS. NOT IMLEMENTED
//export hadoopRzOptionsSetByteBufferPool
func hadoopRzOptionsSetByteBufferPool(opts *C.hadoopRzOptions,
	className *C.cchar_t) C.int {
	ERROR("hadoopRzOptionsSetByteBufferPool. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hadoopRzOptionsFree(struct hadoopRzOptions *opts);
/**
  - Free a hadoopRzOptionsFree structure.
    *
  - @param opts        The options structure to free.
  - Any associated ByteBufferPool will also be freed.
*/
// ENOSYS. NOT IMLEMENTED
//export hadoopRzOptionsFree
func hadoopRzOptionsFree(opts *C.hadoopRzOptions) {
	ERROR("hadoopRzOptionsFree. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    struct hadoopRzBuffer* hadoopReadZero(hdfsFile file,
//            struct hadoopRzOptions *opts, int32_t maxLength);
/**
 * Perform a byte buffer read.
 * If possible, this will be a zero-copy (mmap) read.
 *
 * @param file       The file to read from.
 * @param opts       An options structure created by hadoopRzOptionsAlloc.
 * @param maxLength  The maximum length to read.  We may read fewer bytes
 *                   than this length.
 *
 * @return           On success, we will return a new hadoopRzBuffer.
 *                   This buffer will continue to be valid and readable
 *                   until it is released by readZeroBufferFree.  Failure to
 *                   release a buffer will lead to a memory leak.
 *                   You can access the data within the hadoopRzBuffer with
 *                   hadoopRzBufferGet.  If you have reached EOF, the data
 *                   within the hadoopRzBuffer will be NULL.  You must still
 *                   free hadoopRzBuffer instances containing NULL.
 *
 *                   On failure, we will return NULL plus an errno code.
 *                   errno = EOPNOTSUPP indicates that we could not do a
 *                   zero-copy read, and there was no ByteBufferPool
 *                   supplied.
 */
// ENOSYS. NOT IMLEMENTED
//export hadoopReadZero
func hadoopReadZero(file C.hdfsFile, opts *C.hadoopRzOptions,
	maxLength C.__int32_t) *C.hadoopRzBuffer {
	ERROR("hadoopReadZero. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return nil
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// int32_t hadoopRzBufferLength(const struct hadoopRzBuffer *buffer);
/**
  - Determine the length of the buffer returned from readZero.
    *
  - @param buffer     a buffer returned from readZero.
  - @return           the length of the buffer.
*/
// ENOSYS. NOT IMLEMENTED
//export hadoopRzBufferLength
func hadoopRzBufferLength(buffer C.chadoopRzBuffer_t) C.__int32_t {
	ERROR("hadoopRzBufferLength. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
	return -1
}

//------------------------------------------------------------------------------

//    LIBHDFS_EXTERNAL
//    const void *hadoopRzBufferGet(const struct hadoopRzBuffer *buffer);
/**
 * Get a pointer to the raw buffer returned from readZero.
 *
 * To find out how many bytes this buffer contains, call
 * hadoopRzBufferLength.
 *
 * @param buffer     a buffer returned from readZero.
 * @return           a pointer to the start of the buffer.  This will be
 *                   NULL when end-of-file has been reached.
 */
// ENOSYS. NOT IMLEMENTED
//export hadoopRzBufferGet
func hadoopRzBufferGet(buffer C.chadoopRzBuffer_t) {
	ERROR("hadoopRzBufferGet. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------

// LIBHDFS_EXTERNAL
// void hadoopRzBufferFree(hdfsFile file, struct hadoopRzBuffer *buffer);
/**
  - Release a buffer obtained through readZero.
    *
  - @param file       The hdfs stream that created this buffer.  This must be
  - the same stream you called hadoopReadZero on.
  - @param buffer     The buffer to release.
*/
// ENOSYS. NOT IMLEMENTED
//export hadoopRzBufferFree
func hadoopRzBufferFree(file C.hdfsFile, buffer C.chadoopRzBuffer_t) {
	ERROR("hadoopRzBufferFree. NOT IMPLEMENTED")
	setCErrno(syscall.ENOSYS)
}

//------------------------------------------------------------------------------
