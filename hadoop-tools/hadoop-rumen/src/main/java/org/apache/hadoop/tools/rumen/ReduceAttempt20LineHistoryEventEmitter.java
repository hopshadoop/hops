/**
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
package org.apache.hadoop.tools.rumen;

import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinishedEvent;

public class ReduceAttempt20LineHistoryEventEmitter
     extends TaskAttempt20LineEventEmitter {

  static List<SingleEventEmitter> nonFinals =
      new LinkedList<SingleEventEmitter>();
  static List<SingleEventEmitter> finals = new LinkedList<SingleEventEmitter>();

  static {
    nonFinals.addAll(taskEventNonFinalSEEs);

    finals.add(new ReduceAttemptFinishedEventEmitter());
  }

  ReduceAttempt20LineHistoryEventEmitter() {
    super();
  }

  static private class ReduceAttemptFinishedEventEmitter extends
      SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskAttemptIDName,
        HistoryEventEmitter thatg) {
      if (taskAttemptIDName == null) {
        return null;
      }

      TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptIDName);

      String finishTime = line.get("FINISH_TIME");
      String status = line.get("TASK_STATUS");

      if (finishTime != null && status != null
          && status.equalsIgnoreCase("success")) {
        String hostName = line.get("HOSTNAME");
        String counters = line.get("COUNTERS");
        String state = line.get("STATE_STRING");
        String shuffleFinish = line.get("SHUFFLE_FINISHED");
        String sortFinish = line.get("SORT_FINISHED");

        if (shuffleFinish != null && sortFinish != null
            && "success".equalsIgnoreCase(status)) {
          ReduceAttempt20LineHistoryEventEmitter that =
              (ReduceAttempt20LineHistoryEventEmitter) thatg;

          return new ReduceAttemptFinishedEvent
            (taskAttemptID,
             that.originalTaskType, status,
             Long.parseLong(shuffleFinish),
             Long.parseLong(sortFinish),
             Long.parseLong(finishTime),
             hostName, -1, null,
             state, maybeParseCounters(counters),
             null);
        }
      }

      return null;
    }
  }

  @Override
  List<SingleEventEmitter> finalSEEs() {
    return finals;
  }

  @Override
  List<SingleEventEmitter> nonFinalSEEs() {
    return nonFinals;
  }

}
