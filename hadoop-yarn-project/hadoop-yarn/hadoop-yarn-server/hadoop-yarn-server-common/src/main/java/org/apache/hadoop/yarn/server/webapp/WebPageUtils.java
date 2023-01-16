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

package org.apache.hadoop.yarn.server.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;


public class WebPageUtils {

  public static String appsTableInit() {
    return appsTableInit(false, true);
  }

  public static String appsTableInit(boolean isResourceManager) {
    return appsTableInit(false, isResourceManager);
  }

  public static String appsTableInit(
      boolean isFairSchedulerPage, boolean isResourceManager) {
    // id, user, name, queue, starttime, finishtime, state, status, progress, ui
    // FairSchedulerPage's table is a bit different
    return tableInit()
      .append(", 'aaData': appsTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")
      .append("\n, aoColumnDefs: ")
      .append(getAppsTableColumnDefs(isFairSchedulerPage, isResourceManager))
      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  private static String getAppsTableColumnDefs(
      boolean isFairSchedulerPage, boolean isResourceManager) {
    StringBuilder sb = new StringBuilder();
    sb.append("[\n")
      .append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")
      .append("\n, {'sType':'num-ignore-str', 'aTargets': [6, 7, 8]")
      .append(", 'mRender': renderHadoopDate }")
      .append("\n, {'sType':'num-ignore-str', 'aTargets': [11, 12, 13, 14, 15] }")
      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets':");
    if (isFairSchedulerPage) {
      sb.append("[15]");
    } else if (isResourceManager) {
      sb.append("[17]");
    } else {
      sb.append("[9]");
    }
    sb.append(", 'mRender': parseHadoopProgress }]");
    return sb.toString();
  }

  public static String attemptsTableInit() {
    return tableInit().append(", 'aaData': attemptsTableData")
      .append(", bDeferRender: true").append(", bProcessing: true")
      .append("\n, aoColumnDefs: ").append(getAttemptsTableColumnDefs())
      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  private static String getAttemptsTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")
      .append("\n, {'sType':'numeric', 'aTargets': [1]")
      .append(", 'mRender': renderHadoopDate }]").toString();
  }

  public static String containersTableInit() {
    return tableInit().append(", 'aaData': containersTableData")
      .append(", bDeferRender: true").append(", bProcessing: true")
      .append("\n, aoColumnDefs: ").append(getContainersTableColumnDefs())
      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  private static String getContainersTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }]").toString();
  }

  public static String resourceRequestsTableInit() {
    return tableInit().append(", 'aaData': resourceRequestsTableData")
        .append(", bDeferRender: true").append(", bProcessing: true}")
        .toString();
  }

}