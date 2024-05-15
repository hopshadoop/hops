package main

/*
#include "libhdfs_def.h"
*/
import (
	"C"
)

import (
	"fmt"
	"os"
	"os/user"
	"testing"
	"unsafe"
)

//------------------------------------------------------------------------------

func TestSimpleConnectInternal(t *testing.T) {

	fs := connectToLocalHopsFS(t)

	dirPath := C.CString("/test")
	defer C.free(unsafe.Pointer(dirPath))
	if ret := hdfsDelete(fs, dirPath, 1); ret != 0 {
		t.Fatalf("delete failed")
	}

	if ret := hdfsCreateDirectory(fs, dirPath); ret != 0 {
		t.Fatalf("failed to create directory")
	}

	subPath1 := C.CString("/test/disk-usage-base")
	defer C.free(unsafe.Pointer(subPath1))
	if ret := hdfsCreateDirectory(fs, subPath1); ret != 0 {
		t.Fatalf("failed to create directory")
	}

	subPath2 := C.CString("/test/disk-usage-base/subdir")
	defer C.free(unsafe.Pointer(subPath2))
	if ret := hdfsCreateDirectory(fs, subPath2); ret != 0 {
		t.Fatalf("failed to create directory")
	}

	for i := 0; i < 3; i++ {
		fileNameGo := fmt.Sprintf("/test/disk-usage-base/p%d", i)
		fileNameC := C.CString(fileNameGo)
		defer C.free(unsafe.Pointer(fileNameC))
		createFile(t, fs, fileNameGo, 1024)
	}

	hdfsFileInfoSubPath1 := hdfsGetPathInfo(fs, subPath1)
	if hdfsFileInfoSubPath1 == nil {
		t.Fatal("failed to stat dir")
	}

	fmt.Printf("Info name: %s \n", C.GoString(hdfsFileInfoSubPath1.mName))
	hdfsFreeFileInfo(hdfsFileInfoSubPath1, 1)

	var numEntries C.int
	hdfsFileInfosSubPath1 := hdfsListDirectory(fs, subPath1, &numEntries)
	hdfsFileInfosSubPath1Slice := (*[1 << 30]C.hdfsFileInfo)(unsafe.Pointer(hdfsFileInfosSubPath1))[:numEntries:numEntries]

	if len(hdfsFileInfosSubPath1Slice) != 4 {
		t.Fatalf("stat failed")
	}

	hdfsFreeFileInfo(hdfsFileInfosSubPath1, numEntries)

	if ret := hdfsDelete(fs, dirPath, 1); ret != 0 {
		t.Fatalf("failed to delete directory")
	}

	hdfsDisconnect(fs)
}

//------------------------------------------------------------------------------

func connectToLocalHopsFS(t *testing.T) C.hdfsFS {

	hdfsBuilder := hdfsNewBuilder()
	if hdfsBuilder == nil {
		t.Fatalf("failed to create hdfsNewBuilder")
	}

	address := C.CString("localhost")
	defer C.free(unsafe.Pointer(address))
	hdfsBuilderSetNameNode(hdfsBuilder, address)

	hdfsBuilderSetNameNodePort(hdfsBuilder, 8020)

	username := C.CString("salman")
	defer C.free(unsafe.Pointer(username))
	hdfsBuilderSetUserName(hdfsBuilder, username)

	hdfsBuilderSetForceNewInstance(hdfsBuilder)

	fs := hdfsBuilderConnect(hdfsBuilder)
	if fs == nil {
		t.Fatalf("failed to connect")
	}
	return fs
}

//------------------------------------------------------------------------------

func connectToDefaultFS(t *testing.T) C.hdfsFS {

	hdfsBuilder := hdfsNewBuilder()
	if hdfsBuilder == nil {
		t.Fatalf("failed to create hdfsNewBuilder")
	}

	user, _ := user.Current()

	os.Setenv(LIBHDFS_DEFAULT_FS, "hdfs://localhost:8020")
	os.Setenv(LIBHDFS_DEFAULT_USER, user.Name)

	// read ssl config from core-site.xml
	// os.Setenv("HADOOP_CONF_DIR", "/tmp/hopsfs-conf")

	// read ssl params from environment variables
	// os.Setenv("LIBHDFS_ROOT_CA_BUNDLE", "/tmp/hopsfs-conf/certs/hops_root_ca.pem")
	// os.Setenv("LIBHDFS_CLIENT_CERTIFICATE", "/tmp/hopsfs-conf/certs/salman_certificate_bundle.pem")
	// os.Setenv("LIBHDFS_CLIENT_KEY", "/tmp/hopsfs-conf/certs/salman_priv.pem")

	fs := hdfsBuilderConnect(hdfsBuilder)
	if fs == nil {
		t.Fatalf("failed to connect")
	}
	return fs
}

//------------------------------------------------------------------------------

func TestPathInternal(t *testing.T) {
	fs := connectToLocalHopsFS(t)
	fsWrapper, _ := getHdfsClientWrapper(fs)

	// /home/salman
	input_path := "/home/salman"
	output_path := "/home/salman"
	got := parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// /home////salman///
	input_path = "/home////salman///"
	output_path = "/home/salman"
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// hdfs:///home////salman///
	input_path = "hdfs:///home////salman///"
	output_path = "/home/salman"
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// hdfs://localhost:8020/home/salman
	input_path = "hdfs://localhost:8020/home/salman"
	output_path = "/home/salman"
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// hdfs://localhost:8020///home///salman//
	input_path = "hdfs://localhost:8020///home///salman//"
	output_path = "/home/salman"
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// hdfs://localhost:8020home///salman//
	input_path = "hdfs://localhost:8020home///salman//"
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, input_path, got)

	// hopsfs://localhost:8020///home///salman//
	input_path = "hopsfs://localhost:8020///home///salman//"
	output_path = "/home/salman"
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// test relative paths
	// somedir/file
	u, err := user.Current()
	if err != nil {
		t.Fatalf("Failed to obtain the os user. Err: %v ", err)
	}
	input_path = "somedir/file"
	output_path = fmt.Sprintf("/user/%s/%s", u.Name, input_path)
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// cwd /home/bang/bang
	cwdGo := "/home/boom/boom"
	cwdC := C.CString(cwdGo)
	defer C.free(unsafe.Pointer(cwdC))
	ret := hdfsSetWorkingDirectory(fs, cwdC)
	if ret != 0 {
		t.Fatal("Failed to set working dir")
	}

	// somedir/file
	input_path = "somedir/file"
	output_path = fmt.Sprintf("%s/%s", cwdGo, input_path)
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	// update cwd using relative path
	cwdGo2 := "pow"
	cwdC2 := C.CString(cwdGo2)
	defer C.free(unsafe.Pointer(cwdC2))
	ret = hdfsSetWorkingDirectory(fs, cwdC2)
	if ret != 0 {
		t.Fatal("Failed to set working dir")
	}

	// somedir/file
	input_path = "somedir/file"
	output_path = fmt.Sprintf("%s/%s/%s", cwdGo, cwdGo2, input_path)
	got = parsePath(fsWrapper, input_path)
	compareResponse(t, output_path, got)

	hdfsDisconnect(fs)
}

//------------------------------------------------------------------------------

func compareResponse(t *testing.T, expecting, got string) {
	t.Helper()
	if expecting != got {
		t.Fatalf("failed expecting %s. Got: %s", expecting, got)
	}
}

//------------------------------------------------------------------------------

func TestCopyInternal(t *testing.T) {
	fs := connectToLocalHopsFS(t)
	//fsWrapper := getHdfsClientWrapper(fs)

	file1 := "file1"
	file1C := C.CString(file1)
	defer C.free(unsafe.Pointer(file1C))
	createFile(t, fs, file1, 1024)

	// copy file -> file2
	file2 := "file2"
	file2C := C.CString(file2)
	defer C.free(unsafe.Pointer(file2C))
	ret := hdfsCopy(fs, file1C, fs, file2C)
	if ret != 0 {
		t.Fatalf("Failed to copy file. Expecting: 0, Got: %d", ret)
	}

	infoDst := hdfsGetPathInfo(fs, file2C)
	if infoDst == nil {
		t.Fatal("failed to stat file")
	}
	hdfsFreeFileInfo(infoDst, 1)

	// copy with overwrite
	// copy file -> file2
	ret = hdfsCopy(fs, file1C, fs, file2C)
	if ret != 0 {
		t.Fatalf("Failed to copy file. Expecting: 0, Got: %d", ret)
	}

	// move file -> file3
	file3 := "file3"
	file3C := C.CString(file3)
	defer C.free(unsafe.Pointer(file3C))
	ret = hdfsMove(fs, file1C, fs, file3C)
	if ret != 0 {
		t.Fatalf("Failed to move file. Expecting: 0, Got: %d", ret)
	}

	infoDst = hdfsGetPathInfo(fs, file1C)
	if infoDst != nil {
		t.Fatal("move failed to remove src")
	}

	// move with overwrite
	// move file2 -> file3
	ret = hdfsMove(fs, file2C, fs, file3C)
	if ret != 0 {
		t.Fatalf("Failed to move file. Expecting: 0, Got: %d", ret)
	}
	infoDst = hdfsGetPathInfo(fs, file2C)
	if infoDst != nil {
		t.Fatal("move failed to remove src")
	}

	hdfsDisconnect(fs)
}

// ------------------------------------------------------------------------------
func TestDefaultBlockSizeInternal(t *testing.T) {
	fs := connectToLocalHopsFS(t)
	ret := hdfsGetDefaultBlockSize(fs)
	if ret <= 0 {
		t.Fatalf("Failed to get default block size. Ret: %d", ret)
	}
}

//------------------------------------------------------------------------------

func TestSetTimesInternal(t *testing.T) {
	fs := connectToLocalHopsFS(t)

	file := "file1"
	fileC := C.CString(file)
	defer C.free(unsafe.Pointer(fileC))
	createFile(t, fs, file, 1024)

	info := hdfsGetPathInfo(fs, fileC)
	if info == nil {
		t.Fatal("failed to stat file")
	}
	INFO("aTime %d, mTime %d\n", info.mLastAccess, info.mLastAccess)
	hdfsFreeFileInfo(info, 1)

	time := 946684861 //Sat Jan 01 2000 00:01:01 GMT+0000
	ret := hdfsUtime(fs, fileC, C.long(time), C.long(time))
	if ret != 0 {
		t.Fatal("failed to set times")
	}

	info = hdfsGetPathInfo(fs, fileC)
	if info == nil {
		t.Fatal("failed to stat file")
	}
	INFO("aTime %d, mTime %d\n", info.mLastAccess, info.mLastAccess)
	hdfsFreeFileInfo(info, 1)
}

//------------------------------------------------------------------------------

func createFile(t *testing.T, fs C.hdfsFS, path string, size int) {
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))

	fileHandle := hdfsOpenFile(fs, pathC, C.int(os.O_CREATE|os.O_WRONLY), 1024, 1, 1024*1024)
	if fileHandle == nil {
		t.Fatalf("failed to open file: %s ", path)
	}

	buffer := (*C.void)(C.calloc(1, C.size_t(size)))
	defer C.free(unsafe.Pointer(buffer))
	hdfsWrite(fs, fileHandle, buffer, C.tSize(size))
	hdfsFlush(fs, fileHandle)
	hdfsCloseFile(fs, fileHandle)
}

//------------------------------------------------------------------------------

func TestHdfsAvailableInternal(t *testing.T) {

	fs := connectToLocalHopsFS(t)

	fileSize := 1024
	file := "file1"
	fileC := C.CString(file)
	defer C.free(unsafe.Pointer(fileC))
	createFile(t, fs, file, fileSize)

	fileHandle := hdfsOpenFile(fs, fileC, C.int(os.O_RDONLY), 0, 0, 0)

	ret := hdfsAvailable(fs, fileHandle)
	if ret != C.int(fileSize) {
		t.Fatalf("hdfsAvailable failed. Expecting: %d. Got: %d ", fileSize, ret)
	}

	buffer := (*C.void)(C.calloc(1, C.size_t(fileSize/2)))
	defer C.free(unsafe.Pointer(buffer))
	ret = hdfsRead(fs, fileHandle, buffer, C.tSize(fileSize/2))
	if ret != C.tSize(fileSize/2) {
		t.Fatalf("hdfsRead failed. Expecting: %d. Got: %d ", fileSize/2, ret)
	}

	ret = hdfsAvailable(fs, fileHandle)
	if ret != C.int(fileSize/2) {
		t.Fatalf("hdfsAvailable failed. Expecting: %d. Got: %d ", fileSize/2, ret)
	}

}

//------------------------------------------------------------------------------

func TestHdfsSetReplicationInternal(t *testing.T) {
	fs := connectToLocalHopsFS(t)

	fileSize := 1024
	file := "file1"
	fileC := C.CString(file)
	defer C.free(unsafe.Pointer(fileC))
	createFile(t, fs, file, fileSize)

	ret := hdfsSetReplication(fs, fileC, 1)
	if ret != 0 {
		t.Fatalf("hdfsSetReplication failed.")
	}

	info := hdfsGetPathInfo(fs, fileC)
	if info == nil {
		t.Fatal("failed to stat file")
	}
	if info.mReplication != 1 {
		t.Fatalf("hdfsSetReplication failed. Expecting: 1, Got: %d", info.mReplication)
	}
	hdfsFreeFileInfo(info, 1)

	ret = hdfsSetReplication(fs, fileC, 3)
	if ret != 0 {
		t.Fatalf("hdfsSetReplication failed.")
	}

	info = hdfsGetPathInfo(fs, fileC)
	if info == nil {
		t.Fatal("failed to stat file")
	}
	if info.mReplication != 3 {
		t.Fatalf("hdfsSetReplication failed. Expecting: 3, Got: %d", info.mReplication)
	}
	hdfsFreeFileInfo(info, 1)
}

//------------------------------------------------------------------------------

func TestHdfsConnectToDefaultFSInternal(t *testing.T) {
	connectToDefaultFS(t)
}

//------------------------------------------------------------------------------

func setupLoggerForTesting() {
	os.Setenv(LIBHDFS_ENABLE_LOG, "true")
	// os.Setenv(LIBHDFS_LOG_FILE, "/tmp/libhdfs.log")
	init_logger()
}
