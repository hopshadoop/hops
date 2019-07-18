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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.webapp.AppsBlock;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

class DefaultSchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float WIDTH_F = 0.8f;
  static final String Q_END = "left:101%";
  static final String OVER = "font-size:1px;background:#FFA333";
  static final String UNDER = "font-size:1px;background:#5BD75B";
  static final float EPSILON = 1e-8f;

  static class QueueInfoBlock extends HtmlBlock {
    final FifoSchedulerInfo sinfo;

    @Inject
    QueueInfoBlock(ViewContext ctx, ResourceManager rm) {
      super(ctx);
      sinfo = new FifoSchedulerInfo(rm);
    }

    @Override public void render(Block html) {
      info("\'" + sinfo.getQueueName() + "\' Queue Status").
          __("Queue State:" , sinfo.getState()).
          __("Minimum Queue Memory Capacity:" , Long.toString(sinfo.getMinQueueMemoryCapacity())).
          __("Maximum Queue Memory Capacity:" , Long.toString(sinfo.getMaxQueueMemoryCapacity())).
          __("Number of Nodes:" , Integer.toString(sinfo.getNumNodes())).
          __("Used Node Capacity:" , Integer.toString(sinfo.getUsedNodeCapacity())).
          __("Available Node Capacity:" , Integer.toString(sinfo.getAvailNodeCapacity())).
          __("Total Node Capacity:" , Integer.toString(sinfo.getTotalNodeCapacity())).
          __("Number of Node Containers:" , Integer.toString(sinfo.getNumContainers()));

      html.__(InfoBlock.class);
    }
  }

  static class QueuesBlock extends HtmlBlock {
    final FifoSchedulerInfo sinfo;
    final FifoScheduler fs;

    @Inject QueuesBlock(ResourceManager rm) {
      sinfo = new FifoSchedulerInfo(rm);
      fs = (FifoScheduler) rm.getResourceScheduler();
    }

    @Override
    public void render(Block html) {
      html.__(MetricsOverviewTable.class);
      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").
          __("FifoScheduler Queue").__().
          div("#cs.ui-widget-content.ui-corner-bottom").
            ul();

      if (fs == null) {
        ul.
          li().
            a(_Q).$style(width(WIDTH_F)).
              span().$style(Q_END).__("100% ").__().
              span(".q", "default").__().__();
      } else {
        float used = sinfo.getUsedCapacity();
        float set = sinfo.getCapacity();
        float delta = Math.abs(set - used) + 0.001f;
        ul.
          li().
            a(_Q).$style(width(WIDTH_F)).
              $title(join("used:", percent(used))).
              span().$style(Q_END).__("100%").__().
              span().$style(join(width(delta), ';', used > set ? OVER : UNDER,
                ';', used > set ? left(set) : left(used))).__(".").__().
              span(".q", sinfo.getQueueName()).__().
            __(QueueInfoBlock.class).__();
      }

      ul.__().__().
      script().$type("text/javascript").
          __("$('#cs').hide();").__().__().
          __(AppsBlock.class);
    }
  }


  @Override protected void postHead(Page.HTML<__> html) {
    html.
      style().$type("text/css").
        __("#cs { padding: 0.5em 0 1em 0; margin-bottom: 1em; position: relative }",
          "#cs ul { list-style: none }",
          "#cs a { font-weight: normal; margin: 2px; position: relative }",
          "#cs a span { font-weight: normal; font-size: 80% }",
          "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }",
          "table.info tr th {width: 50%}").__(). // to center info table
      script("/static/jt/jquery.jstree.js").
      script().$type("text/javascript").
        __("$(function() {",
          "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');",
          "  $('#cs').bind('loaded.jstree', function (e, data) {",
          "    data.inst.open_all(); }).",
          "    jstree({",
          "    core: { animation: 188, html_titles: true },",
          "    plugins: ['themeroller', 'html_data', 'ui'],",
          "    themeroller: { item_open: 'ui-icon-minus',",
          "      item_clsd: 'ui-icon-plus', item_leaf: 'ui-icon-gear'",
          "    }",
          "  });",
          "  $('#cs').bind('select_node.jstree', function(e, data) {",
          "    var q = $('.q', data.rslt.obj).first().text();",
            "    if (q == 'root') q = '';",
          "    $('#apps').dataTable().fnFilter(q, 4);",
          "  });",
          "  $('#cs').show();",
          "});").__();
  }

  @Override protected Class<? extends SubView> content() {
    return QueuesBlock.class;
  }

  static String percent(float f) {
    return StringUtils.formatPercent(f, 1);
  }

  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}
