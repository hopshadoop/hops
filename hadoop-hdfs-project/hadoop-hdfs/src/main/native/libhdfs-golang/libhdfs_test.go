package main

import (
	"testing"
)

func TestSimpleConnect(t *testing.T) {
	TestSimpleConnectInternal(t) // tests do not support CGO. That is why the test is in another file
}

func TestPath(t *testing.T) {
	TestPathInternal(t) // tests do not support CGO. That is why the test is in another file
}

func TestCopy(t *testing.T) {
	TestCopyInternal(t)
}

func TestDefaultBlockSize(t *testing.T) {
	TestDefaultBlockSizeInternal(t)
}

func TestSetTimes(t *testing.T) {
	TestSetTimesInternal(t)
}

func TestHdfsAvailable(t *testing.T) {
	TestHdfsAvailableInternal(t)
}

func TestHdfsSetReplication(t *testing.T) {
	TestHdfsSetReplicationInternal(t)
}

func TestHdfsConnectToDefaultFS(t *testing.T) {
	// pass connection param using environment variables
	TestHdfsConnectToDefaultFSInternal(t)
}
