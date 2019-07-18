/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * Base class for operation context struct passed through codepaths for main
 * S3AFileSystem operations.
 * Anything op-specific should be moved to a subclass of this.
 */
@SuppressWarnings("visibilitymodifier")  // I want a struct of finals, for real.
public class S3AOpContext {

  final boolean isS3GuardEnabled;
  final Invoker invoker;
  @Nullable final FileSystem.Statistics stats;
  final S3AInstrumentation instrumentation;
  @Nullable final Invoker s3guardInvoker;

  /** FileStatus for "destination" path being operated on. */
  protected final FileStatus dstFileStatus;

  /**
   * Alternate constructor that allows passing in two invokers, the common
   * one, and another with the S3Guard Retry Policy.
   * @param isS3GuardEnabled true if s3Guard is active
   * @param invoker invoker, which contains retry policy
   * @param s3guardInvoker s3guard-specific retry policy invoker
   * @param stats optional stats object
   * @param instrumentation instrumentation to use
   * @param dstFileStatus file status from existence check
   */
  public S3AOpContext(boolean isS3GuardEnabled, Invoker invoker,
      Invoker s3guardInvoker, @Nullable FileSystem.Statistics stats,
      S3AInstrumentation instrumentation, FileStatus dstFileStatus) {

    Preconditions.checkNotNull(invoker, "Null invoker arg");
    Preconditions.checkNotNull(instrumentation, "Null instrumentation arg");
    Preconditions.checkNotNull(dstFileStatus, "Null dstFileStatus arg");
    this.isS3GuardEnabled = isS3GuardEnabled;
    Preconditions.checkArgument(!isS3GuardEnabled || s3guardInvoker != null,
        "S3Guard invoker required: S3Guard is enabled.");
    this.invoker = invoker;
    this.s3guardInvoker = s3guardInvoker;
    this.stats = stats;
    this.instrumentation = instrumentation;
    this.dstFileStatus = dstFileStatus;
  }

  /**
   * Constructor using common invoker and retry policy.
   * @param isS3GuardEnabled true if s3Guard is active
   * @param invoker invoker, which contains retry policy
   * @param stats optional stats object
   * @param instrumentation instrumentation to use
   * @param dstFileStatus
   */
  public S3AOpContext(boolean isS3GuardEnabled, Invoker invoker,
      @Nullable FileSystem.Statistics stats, S3AInstrumentation instrumentation,
      FileStatus dstFileStatus) {
    this(isS3GuardEnabled, invoker, null, stats, instrumentation,
        dstFileStatus);
  }

}
