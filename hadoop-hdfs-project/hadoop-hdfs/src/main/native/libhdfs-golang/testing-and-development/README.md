# Some notes and scripts for testing it with pyarrow

a modified `build_conda.sh` is provided here that was used to test libhdfs.so
arrow/python/examples/minimal_build/build_conda.sh


# modify hdfs.py to that it does not check for class path as we are not dependent on java


```
diff --git a/python/pyarrow/hdfs.py b/python/pyarrow/hdfs.py
index 2e6c387a8..cc32ebc49 100644
--- a/python/pyarrow/hdfs.py
+++ b/python/pyarrow/hdfs.py
@@ -43,8 +43,8 @@ class HadoopFileSystem(_hdfsio.HadoopFileSystem, FileSystem):
             _DEPR_MSG.format(
                 "hdfs.HadoopFileSystem", "2.0.0", "fs.HadoopFileSystem"),
             FutureWarning, stacklevel=2)
-        if driver == 'libhdfs':
-            _maybe_set_hadoop_classpath()
+        # if driver == 'libhdfs':
+            # _maybe_set_hadoop_classpath()

         self._connect(host, port, user, kerb_ticket, extra_conf)
```


# Running pyarrow hdfs tests

```
#!/bin/bash

. ./miniconda-for-arrow/etc/profile.d/conda.sh
conda activate pyarrow-3.10

pushd "/home/salman/code/hops/arrow-dev/arrow/python" || exit
HDFS_LIBRARY_PATH="/home/salman/code/hops/hops/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs-golang/lib"

export PYARROW_HDFS_TEST_LIBHDFS_REQUIRE=ON
export ARROW_HDFS_TEST_HOST=localhost
export ARROW_HDFS_TEST_PORT=8020
export ARROW_HDFS_TEST_USER=salman
export LIBHDFS_ENABLE_LOG=true

py.test  pyarrow/tests/
```

