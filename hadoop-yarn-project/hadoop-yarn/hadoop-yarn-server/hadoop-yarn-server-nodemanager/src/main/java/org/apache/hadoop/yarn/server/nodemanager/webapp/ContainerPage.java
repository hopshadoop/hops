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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class ContainerPage extends NMView implements YarnWebParams {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);

    setTitle("Container " + $(CONTAINER_ID));
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return ContainerBlock.class;
  }

  public static class ContainerBlock extends HtmlBlock implements YarnWebParams {

    private final Context nmContext;

    @Inject
    public ContainerBlock(Context nmContext) {
      this.nmContext = nmContext;
    }

    @Override
    protected void render(Block html) {
      ContainerId containerID;
      try {
        containerID = ContainerId.fromString($(CONTAINER_ID));
      } catch (IllegalArgumentException e) {
        html.p().__("Invalid containerId " + $(CONTAINER_ID)).__();
        return;
      }

      DIV<Hamlet> div = html.div("#content");
      Container container = this.nmContext.getContainers().get(containerID);
      if (container == null) {
        div.h1("Unknown Container. Container might have completed, "
                + "please go back to the previous page and retry.").__();
        return;
      }
      ContainerInfo info = new ContainerInfo(this.nmContext, container);

      info("Container information")
        .__("ContainerID", info.getId())
        .__("ContainerState", info.getState())
        .__("ExitStatus", info.getExitStatus())
        .__("Diagnostics", info.getDiagnostics())
        .__("User", info.getUser())
        .__("TotalMemoryNeeded", info.getMemoryNeeded())
        .__("TotalVCoresNeeded", info.getVCoresNeeded())
        .__("ExecutionType", info.getExecutionType())
        .__("logs", info.getShortLogLink(), "Link to logs");
      html.__(InfoBlock.class);
    }
  }
}
