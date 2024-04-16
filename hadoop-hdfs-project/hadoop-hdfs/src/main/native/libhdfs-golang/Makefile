# Copyright (c) 2023 Hopsworks AB
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

VERSION:=$(shell xmlstarlet sel -N x=http://maven.apache.org/POM/4.0.0 -t -v "/x:project/x:version" ../../../../../pom.xml)
LIBHDFSGO_OUTPUT_DIR?=libhdfs-golang
LIBHDFSGO_OUTPUT_FILE?=libhdfs-golang-${VERSION}.so

GITCOMMIT=`git rev-parse --short HEAD`
BUILDTIME=`date +%FT%T%z`
HOSTNAME=`hostname`
BRANCH=`git rev-parse --abbrev-ref HEAD`



#Test name. Change it to run specific test
#e.g. make test TEST=TestXYZ 
TEST?=Test

all: shared_lib

shared_lib:
	rm -f ${LIBHDFSGO_OUTPUT_DIR}/${LIBHDFSGO_OUTPUT_FILE}
	go build -ldflags=" \
	-X hopsworks.ai/rdrs/version.GITCOMMIT=${GITCOMMIT} \
	-X hopsworks.ai/rdrs/version.BUILDTIME=${BUILDTIME} \
	-X hopsworks.ai/rdrs/version.HOSTNAME=${HOSTNAME} \
	-X hopsworks.ai/rdrs/version.BRANCH=${BRANCH}" \
	-o ${LIBHDFSGO_OUTPUT_DIR}/${LIBHDFSGO_OUTPUT_FILE} -buildmode=c-shared
	chmod 744  ${LIBHDFSGO_OUTPUT_DIR}/${LIBHDFSGO_OUTPUT_FILE}
	cd ${LIBHDFSGO_OUTPUT_DIR} && ln -sf ${LIBHDFSGO_OUTPUT_FILE} libhdfs.so

clean:
	go clean -cache
	rm -rf libhdfs.so*

executable:
	go build -o bin/libhdfs

test:
	go clean -testcache
	go test -v -p 1 -run $(TEST) 
