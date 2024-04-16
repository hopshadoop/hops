#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
set -x

#----------------------------------------------------------------------
# Change this to whatever makes sense for your system
HOME=/home/salman/code/hops/arrow-dev
MINICONDA=$HOME/miniconda-for-arrow
LIBRARY_INSTALL_DIR=$HOME/local-libs
CPP_BUILD_DIR=$HOME/arrow-cpp-build
ARROW_ROOT=$HOME/arrow
PYTHON=3.10


git config --global --add safe.directory $ARROW_ROOT

#----------------------------------------------------------------------
# Run these only once

function setup_miniconda() {
  MINICONDA_URL="https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh"
  wget -O miniconda.sh $MINICONDA_URL
  bash miniconda.sh -b -p $MINICONDA
  rm -f miniconda.sh
  LOCAL_PATH=$PATH
  export PATH="$MINICONDA/bin:$PATH"

  mamba info -a

  conda config --set show_channel_urls True
  conda config --show channels

  mamba create -y -n pyarrow-$PYTHON \
        --file $ARROW_ROOT/ci/conda_env_unix.txt \
        --file $ARROW_ROOT/ci/conda_env_cpp.txt \
        --file $ARROW_ROOT/ci/conda_env_python.txt \
        compilers \
        python=$PYTHON \
        pandas

  export PATH=$LOCAL_PATH
}

#setup_miniconda

#----------------------------------------------------------------------
# Activate mamba in bash and activate mamba environment

. $MINICONDA/etc/profile.d/conda.sh
conda activate pyarrow-$PYTHON
export ARROW_HOME=$CONDA_PREFIX

#----------------------------------------------------------------------
# Build C++ library

mkdir -p $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=DEBUG \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DCMAKE_UNITY_BUILD=ON \
      -DARROW_COMPUTE=ON \
      -DARROW_CSV=ON \
      -DARROW_FILESYSTEM=ON \
      -DARROW_JSON=ON \
      -DARROW_BUILD_TESTS=OFF \
      -DARROW_HDFS=ON \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_WITH_ZLIB=ON \
      -DARROW_WITH_GZIP=ON \
      -DARROW_WITH_LZ4=ON \
      $ARROW_ROOT/cpp

      # -DARROW_DATASET=ON \
      # -DARROW_PARQUET=ON \
      # -DARROW_WITH_BROTLI=ON \
      # -DARROW_WITH_BZ2=ON \
      # -DARROW_WITH_LZ4=ON \
      # -DARROW_WITH_ZLIB=ON \
      # -DARROW_WITH_ZSTD=ON \
      # -DPARQUET_REQUIRE_ENCRYPTION=ON \

ninja install

popd

#----------------------------------------------------------------------
# Build and test Python library
pushd $ARROW_ROOT/python

rm -rf build/  # remove any pesky pre-existing build directory

export CMAKE_PREFIX_PATH=${ARROW_HOME}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}
export PYARROW_BUILD_TYPE=Debug
export PYARROW_CMAKE_GENERATOR=Ninja
export PYARROW_WITH_SNAPPY=1
export PYARROW_WITH_ZLIB=1
export PYARROW_WITH_GZIP=1
export PYARROW_WITH_LZ4=1

 #You can run either "develop" or "build_ext --inplace". Your pick
# export PYARROW_WITH_PARQUET=1
# export PYARROW_WITH_DATASET=1
# export PYARROW_WITH_COMPUTE=1 
# export PYARROW_WITH_CSV=1 
# export PYARROW_WITH_FILESYSTEM=1 
# export PYARROW_WITH_JSON=1 
# export PYARROW_WITH_BUILD_TESTS=OFF 
# export PYARROW_WITH_DATASET=1 
# export PYARROW_WITH_PARQUET=1 

export PYARROW_WITH_HDFS=1 
export PYARROW_PARALLEL=20


python setup.py build_ext --inplace
#python setup.py develop

py.test  pyarrow/tests/test_hdfs.py


