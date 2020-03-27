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
package io.hops.common;

import io.hops.exception.StorageException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public abstract class IDsGenerator{

  private static final Log LOG =
          LogFactory.getLog(IDsGenerator.class);
  private int batchSize;
  private int threshold;
  private CountersQueue cQ;

  IDsGenerator(int batchSize, float threshold){
    this.batchSize = batchSize;
    this.threshold = (int)(threshold * batchSize);
    cQ = new CountersQueue();
  }

  public synchronized long getUniqueID() throws StorageException {
    if(!cQ.has(1)){
      LOG.warn("ID Generator has run out of cached IDs. Fetching new set of IDs from DB");
      getMoreIdsIfNeeded();
    }
    return cQ.next();
  }

  protected synchronized boolean getMoreIdsIfNeeded()
          throws StorageException {
    try {
      if (!cQ.has(threshold)) {
        cQ.addCounter(incrementCounter(batchSize));
        return true;
      }
      return false;
    } catch (IOException e) {
      if (e instanceof StorageException) {
        throw (StorageException) e;
      } else {
        throw new StorageException(e);
      }
    }
  }

  protected CountersQueue getCQ() {
    return cQ;
  }

  abstract CountersQueue.Counter incrementCounter(int inc) throws IOException ;
}
