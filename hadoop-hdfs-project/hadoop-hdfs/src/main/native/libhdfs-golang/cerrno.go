package main

/*
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/syscall.h>

void setErrno(int err) {
  errno = err;
}

int cGetTId(){
  return syscall(SYS_gettid);
}
*/
import (
	"C"
)
