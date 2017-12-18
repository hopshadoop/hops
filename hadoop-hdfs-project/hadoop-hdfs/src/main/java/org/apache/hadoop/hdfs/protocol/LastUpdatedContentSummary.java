/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

public class LastUpdatedContentSummary {

  private final long fileAndDirCount;
  private final long spaceConsumed;
  private final long nsQuota;
  private final long dsQuota;

  public LastUpdatedContentSummary(long fileAndDirCount, long spaceConsumed,
      long nsQuota, long dsQuota) {
    this.fileAndDirCount = fileAndDirCount;
    this.spaceConsumed = spaceConsumed;
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
  }

  public long getFileAndDirCount() {
    return fileAndDirCount;
  }

  public long getSpaceConsumed() {
    return spaceConsumed;
  }

  public long getNsQuota() {
    return nsQuota;
  }

  public long getDsQuota() {
    return dsQuota;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LastUpdatedContentSummary)) {
      return false;
    }

    LastUpdatedContentSummary that = (LastUpdatedContentSummary) o;

    if (fileAndDirCount != that.fileAndDirCount) {
      return false;
    }
    if (spaceConsumed != that.spaceConsumed) {
      return false;
    }
    if (nsQuota != that.nsQuota) {
      return false;
    }
    return dsQuota == that.dsQuota;

  }

  @Override
  public int hashCode() {
    int result = (int) (fileAndDirCount ^ (fileAndDirCount >>> 32));
    result = 31 * result + (int) (spaceConsumed ^ (spaceConsumed >>> 32));
    result = 31 * result + (int) (nsQuota ^ (nsQuota >>> 32));
    result = 31 * result + (int) (dsQuota ^ (dsQuota >>> 32));
    return result;
  }

}

