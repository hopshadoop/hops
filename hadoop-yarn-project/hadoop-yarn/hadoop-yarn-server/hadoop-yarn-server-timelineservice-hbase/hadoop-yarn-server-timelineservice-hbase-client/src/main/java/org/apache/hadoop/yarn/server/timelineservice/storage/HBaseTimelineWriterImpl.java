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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import  org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.SubApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnName;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTableRW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implements a hbase based backend for storing the timeline entity
 * information.
 * It writes to multiple tables at the backend
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBaseTimelineWriterImpl extends AbstractService implements
    TimelineWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseTimelineWriterImpl.class);

  private Connection conn;
  private TypedBufferedMutator<EntityTable> entityTable;
  private TypedBufferedMutator<AppToFlowTable> appToFlowTable;
  private TypedBufferedMutator<ApplicationTable> applicationTable;
  private TypedBufferedMutator<FlowActivityTable> flowActivityTable;
  private TypedBufferedMutator<FlowRunTable> flowRunTable;
  private TypedBufferedMutator<SubApplicationTable> subApplicationTable;
  private TypedBufferedMutator<DomainTable> domainTable;

  /**
   * Used to convert strings key components to and from storage format.
   */
  private final KeyConverter<String> stringKeyConverter =
      new StringKeyConverter();

  /**
   * Used to convert Long key components to and from storage format.
   */
  private final KeyConverter<Long> longKeyConverter = new LongKeyConverter();

  private enum Tables {
    APPLICATION_TABLE, ENTITY_TABLE, SUBAPPLICATION_TABLE
  };

  public HBaseTimelineWriterImpl() {
    super(HBaseTimelineWriterImpl.class.getName());
  }

  /**
   * initializes the hbase connection to write to the entity table.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    Configuration hbaseConf =
        HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(conf);
    conn = ConnectionFactory.createConnection(hbaseConf);
    entityTable = new EntityTableRW().getTableMutator(hbaseConf, conn);
    appToFlowTable = new AppToFlowTableRW().getTableMutator(hbaseConf, conn);
    applicationTable =
        new ApplicationTableRW().getTableMutator(hbaseConf, conn);
    flowRunTable = new FlowRunTableRW().getTableMutator(hbaseConf, conn);
    flowActivityTable =
        new FlowActivityTableRW().getTableMutator(hbaseConf, conn);
    subApplicationTable =
        new SubApplicationTableRW().getTableMutator(hbaseConf, conn);
    domainTable = new DomainTableRW().getTableMutator(hbaseConf, conn);

    UserGroupInformation ugi = UserGroupInformation.isSecurityEnabled() ?
        UserGroupInformation.getLoginUser() :
        UserGroupInformation.getCurrentUser();
    LOG.info("Initialized HBaseTimelineWriterImpl UGI to " + ugi);
  }

  /**
   * Stores the entire information in TimelineEntities to the timeline store.
   */
  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineEntities data, UserGroupInformation callerUgi)
      throws IOException {

    TimelineWriteResponse putStatus = new TimelineWriteResponse();

    String clusterId = context.getClusterId();
    String userId = context.getUserId();
    String flowName = context.getFlowName();
    String flowVersion = context.getFlowVersion();
    long flowRunId = context.getFlowRunId();
    String appId = context.getAppId();
    String subApplicationUser = callerUgi.getShortUserName();

    // defensive coding to avoid NPE during row key construction
    if ((flowName == null) || (appId == null) || (clusterId == null)
        || (userId == null)) {
      LOG.warn("Found null for one of: flowName=" + flowName + " appId=" + appId
          + " userId=" + userId + " clusterId=" + clusterId
          + " . Not proceeding with writing to hbase");
      return putStatus;
    }

    for (TimelineEntity te : data.getEntities()) {

      // a set can have at most 1 null
      if (te == null) {
        continue;
      }

      // if the entity is the application, the destination is the application
      // table
      boolean isApplication = ApplicationEntity.isApplicationEntity(te);
      byte[] rowKey;
      if (isApplication) {
        ApplicationRowKey applicationRowKey =
            new ApplicationRowKey(clusterId, userId, flowName, flowRunId,
                appId);
        rowKey = applicationRowKey.getRowKey();
        store(rowKey, te, flowVersion, Tables.APPLICATION_TABLE);
      } else {
        EntityRowKey entityRowKey =
            new EntityRowKey(clusterId, userId, flowName, flowRunId, appId,
                te.getType(), te.getIdPrefix(), te.getId());
        rowKey = entityRowKey.getRowKey();
        store(rowKey, te, flowVersion, Tables.ENTITY_TABLE);
      }

      if (!isApplication && SubApplicationEntity.isSubApplicationEntity(te)) {
        SubApplicationRowKey subApplicationRowKey =
            new SubApplicationRowKey(subApplicationUser, clusterId,
                te.getType(), te.getIdPrefix(), te.getId(), userId);
        rowKey = subApplicationRowKey.getRowKey();
        store(rowKey, te, flowVersion, Tables.SUBAPPLICATION_TABLE);
      }

      if (isApplication) {
        TimelineEvent event =
            ApplicationEntity.getApplicationEvent(te,
                ApplicationMetricsConstants.CREATED_EVENT_TYPE);
        FlowRunRowKey flowRunRowKey =
            new FlowRunRowKey(clusterId, userId, flowName, flowRunId);
        if (event != null) {
          onApplicationCreated(flowRunRowKey, clusterId, appId, userId,
              flowVersion, te, event.getTimestamp());
        }
        // if it's an application entity, store metrics
        storeFlowMetricsAppRunning(flowRunRowKey, appId, te);
        // if application has finished, store it's finish time and write final
        // values of all metrics
        event = ApplicationEntity.getApplicationEvent(te,
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
        if (event != null) {
          onApplicationFinished(flowRunRowKey, flowVersion, appId, te,
              event.getTimestamp());
        }
      }
    }
    return putStatus;
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineDomain domain)
      throws IOException {
    TimelineWriteResponse putStatus = new TimelineWriteResponse();

    String clusterId = context.getClusterId();
    String domainId = domain.getId();

    // defensive coding to avoid NPE during row key construction
    if (clusterId == null) {
      LOG.warn(
          "Found null for clusterId. Not proceeding with writing to hbase");
      return putStatus;
    }

    DomainRowKey domainRowKey = new DomainRowKey(clusterId, domainId);
    byte[] rowKey = domainRowKey.getRowKey();

    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.CREATED_TIME, null,
        domain.getCreatedTime());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.DESCRIPTION, null,
        domain.getDescription());
    ColumnRWHelper
        .store(rowKey, domainTable, DomainColumn.MODIFICATION_TIME, null,
            domain.getModifiedTime());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.OWNER, null,
        domain.getOwner());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.READERS, null,
        domain.getReaders());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.WRITERS, null,
        domain.getWriters());
    return putStatus;
  }

  private void onApplicationCreated(FlowRunRowKey flowRunRowKey,
      String clusterId, String appId, String userId, String flowVersion,
      TimelineEntity te, long appCreatedTimeStamp)
      throws IOException {

    String flowName = flowRunRowKey.getFlowName();
    Long flowRunId = flowRunRowKey.getFlowRunId();

    // store in App to flow table
    AppToFlowRowKey appToFlowRowKey = new AppToFlowRowKey(appId);
    byte[] rowKey = appToFlowRowKey.getRowKey();
    ColumnRWHelper.store(rowKey, appToFlowTable,
        AppToFlowColumnPrefix.FLOW_NAME, clusterId, null, flowName);
    ColumnRWHelper.store(rowKey, appToFlowTable,
        AppToFlowColumnPrefix.FLOW_RUN_ID, clusterId, null, flowRunId);
    ColumnRWHelper.store(rowKey, appToFlowTable, AppToFlowColumnPrefix.USER_ID,
        clusterId, null, userId);

    // store in flow run table
    storeAppCreatedInFlowRunTable(flowRunRowKey, appId, te);

    // store in flow activity table
    byte[] flowActivityRowKeyBytes =
        new FlowActivityRowKey(flowRunRowKey.getClusterId(),
            appCreatedTimeStamp, flowRunRowKey.getUserId(), flowName)
            .getRowKey();
    byte[] qualifier = longKeyConverter.encode(flowRunRowKey.getFlowRunId());
    ColumnRWHelper.store(flowActivityRowKeyBytes, flowActivityTable,
        FlowActivityColumnPrefix.RUN_ID, qualifier, null, flowVersion,
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }

  /*
   * updates the {@link FlowRunTable} with Application Created information
   */
  private void storeAppCreatedInFlowRunTable(FlowRunRowKey flowRunRowKey,
      String appId, TimelineEntity te) throws IOException {
    byte[] rowKey = flowRunRowKey.getRowKey();
    ColumnRWHelper.store(rowKey, flowRunTable, FlowRunColumn.MIN_START_TIME,
        null, te.getCreatedTime(),
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }


  /*
   * updates the {@link FlowRunTable} and {@link FlowActivityTable} when an
   * application has finished
   */
  private void onApplicationFinished(FlowRunRowKey flowRunRowKey,
      String flowVersion, String appId, TimelineEntity te,
      long appFinishedTimeStamp) throws IOException {
    // store in flow run table
    storeAppFinishedInFlowRunTable(flowRunRowKey, appId, te,
        appFinishedTimeStamp);

    // indicate in the flow activity table that the app has finished
    byte[] rowKey =
        new FlowActivityRowKey(flowRunRowKey.getClusterId(),
            appFinishedTimeStamp, flowRunRowKey.getUserId(),
            flowRunRowKey.getFlowName()).getRowKey();
    byte[] qualifier = longKeyConverter.encode(flowRunRowKey.getFlowRunId());
    ColumnRWHelper.store(rowKey, flowActivityTable,
        FlowActivityColumnPrefix.RUN_ID, qualifier, null, flowVersion,
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }

  /*
   * Update the {@link FlowRunTable} with Application Finished information
   */
  private void storeAppFinishedInFlowRunTable(FlowRunRowKey flowRunRowKey,
      String appId, TimelineEntity te, long appFinishedTimeStamp)
      throws IOException {
    byte[] rowKey = flowRunRowKey.getRowKey();
    Attribute attributeAppId =
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId);
    ColumnRWHelper.store(rowKey, flowRunTable, FlowRunColumn.MAX_END_TIME,
        null, appFinishedTimeStamp, attributeAppId);

    // store the final value of metrics since application has finished
    Set<TimelineMetric> metrics = te.getMetrics();
    if (metrics != null) {
      storeFlowMetrics(rowKey, metrics, attributeAppId,
          AggregationOperation.SUM_FINAL.getAttribute());
    }
  }

  /*
   * Updates the {@link FlowRunTable} with Application Metrics
   */
  private void storeFlowMetricsAppRunning(FlowRunRowKey flowRunRowKey,
      String appId, TimelineEntity te) throws IOException {
    Set<TimelineMetric> metrics = te.getMetrics();
    if (metrics != null) {
      byte[] rowKey = flowRunRowKey.getRowKey();
      storeFlowMetrics(rowKey, metrics,
          AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId),
          AggregationOperation.SUM.getAttribute());
    }
  }

  private void storeFlowMetrics(byte[] rowKey, Set<TimelineMetric> metrics,
      Attribute... attributes) throws IOException {
    for (TimelineMetric metric : metrics) {
      byte[] metricColumnQualifier = stringKeyConverter.encode(metric.getId());
      Map<Long, Number> timeseries = metric.getValues();
      for (Map.Entry<Long, Number> timeseriesEntry : timeseries.entrySet()) {
        Long timestamp = timeseriesEntry.getKey();
        ColumnRWHelper.store(rowKey, flowRunTable, FlowRunColumnPrefix.METRIC,
            metricColumnQualifier, timestamp, timeseriesEntry.getValue(),
            attributes);
      }
    }
  }

  /**
   * Stores the Relations from the {@linkplain TimelineEntity} object.
   */
  private <T extends BaseTable<T>> void storeRelations(byte[] rowKey,
      Map<String, Set<String>> connectedEntities, ColumnPrefix<T> columnPrefix,
      TypedBufferedMutator<T> table) throws IOException {
    if (connectedEntities != null) {
      for (Map.Entry<String, Set<String>> connectedEntity : connectedEntities
          .entrySet()) {
        // id3?id4?id5
        String compoundValue =
            Separator.VALUES.joinEncoded(connectedEntity.getValue());
        ColumnRWHelper.store(rowKey, table, columnPrefix,
            stringKeyConverter.encode(connectedEntity.getKey()),
            null, compoundValue);
      }
    }
  }

  /**
   * Stores information from the {@linkplain TimelineEntity} object.
   */
  private void store(byte[] rowKey, TimelineEntity te,
      String flowVersion,
      Tables table) throws IOException {
    switch (table) {
    case APPLICATION_TABLE:
      ColumnRWHelper.store(rowKey, applicationTable,
          ApplicationColumn.ID, null, te.getId());
      ColumnRWHelper.store(rowKey, applicationTable,
          ApplicationColumn.CREATED_TIME, null, te.getCreatedTime());
      ColumnRWHelper.store(rowKey, applicationTable,
          ApplicationColumn.FLOW_VERSION, null, flowVersion);
      storeInfo(rowKey, te.getInfo(), flowVersion, ApplicationColumnPrefix.INFO,
          applicationTable);
      storeMetrics(rowKey, te.getMetrics(), ApplicationColumnPrefix.METRIC,
          applicationTable);
      storeEvents(rowKey, te.getEvents(), ApplicationColumnPrefix.EVENT,
          applicationTable);
      storeConfig(rowKey, te.getConfigs(), ApplicationColumnPrefix.CONFIG,
          applicationTable);
      storeRelations(rowKey, te.getIsRelatedToEntities(),
          ApplicationColumnPrefix.IS_RELATED_TO, applicationTable);
      storeRelations(rowKey, te.getRelatesToEntities(),
          ApplicationColumnPrefix.RELATES_TO, applicationTable);
      break;
    case ENTITY_TABLE:
      ColumnRWHelper.store(rowKey, entityTable,
          EntityColumn.ID, null, te.getId());
      ColumnRWHelper.store(rowKey, entityTable,
          EntityColumn.TYPE, null, te.getType());
      ColumnRWHelper.store(rowKey, entityTable,
          EntityColumn.CREATED_TIME, null, te.getCreatedTime());
      ColumnRWHelper.store(rowKey, entityTable,
          EntityColumn.FLOW_VERSION, null, flowVersion);
      storeInfo(rowKey, te.getInfo(), flowVersion, EntityColumnPrefix.INFO,
          entityTable);
      storeMetrics(rowKey, te.getMetrics(), EntityColumnPrefix.METRIC,
          entityTable);
      storeEvents(rowKey, te.getEvents(), EntityColumnPrefix.EVENT,
          entityTable);
      storeConfig(rowKey, te.getConfigs(), EntityColumnPrefix.CONFIG,
          entityTable);
      storeRelations(rowKey, te.getIsRelatedToEntities(),
          EntityColumnPrefix.IS_RELATED_TO, entityTable);
      storeRelations(rowKey, te.getRelatesToEntities(),
          EntityColumnPrefix.RELATES_TO, entityTable);
      break;
    case SUBAPPLICATION_TABLE:
      ColumnRWHelper.store(rowKey, subApplicationTable, SubApplicationColumn.ID,
          null, te.getId());
      ColumnRWHelper.store(rowKey, subApplicationTable,
          SubApplicationColumn.TYPE, null, te.getType());
      ColumnRWHelper.store(rowKey, subApplicationTable,
          SubApplicationColumn.CREATED_TIME, null, te.getCreatedTime());
      ColumnRWHelper.store(rowKey, subApplicationTable,
          SubApplicationColumn.FLOW_VERSION, null, flowVersion);
      storeInfo(rowKey, te.getInfo(), flowVersion,
          SubApplicationColumnPrefix.INFO, subApplicationTable);
      storeMetrics(rowKey, te.getMetrics(), SubApplicationColumnPrefix.METRIC,
          subApplicationTable);
      storeEvents(rowKey, te.getEvents(), SubApplicationColumnPrefix.EVENT,
          subApplicationTable);
      storeConfig(rowKey, te.getConfigs(), SubApplicationColumnPrefix.CONFIG,
          subApplicationTable);
      storeRelations(rowKey, te.getIsRelatedToEntities(),
          SubApplicationColumnPrefix.IS_RELATED_TO, subApplicationTable);
      storeRelations(rowKey, te.getRelatesToEntities(),
          SubApplicationColumnPrefix.RELATES_TO, subApplicationTable);
      break;
    default:
      LOG.info("Invalid table name provided.");
      break;
    }
  }

  /**
   * stores the info information from {@linkplain TimelineEntity}.
   */
  private <T extends BaseTable<T>> void storeInfo(byte[] rowKey,
      Map<String, Object> info, String flowVersion,
      ColumnPrefix<T> columnPrefix, TypedBufferedMutator<T > table)
      throws IOException {
    if (info != null) {
      for (Map.Entry<String, Object> entry : info.entrySet()) {
        ColumnRWHelper.store(rowKey, table, columnPrefix,
            stringKeyConverter.encode(entry.getKey()), null, entry.getValue());
      }
    }
  }

  /**
   * stores the config information from {@linkplain TimelineEntity}.
   */
  private <T extends BaseTable<T>> void storeConfig(
      byte[] rowKey, Map<String, String> config,
      ColumnPrefix<T> columnPrefix, TypedBufferedMutator<T> table)
      throws IOException {
    if (config != null) {
      for (Map.Entry<String, String> entry : config.entrySet()) {
        byte[] configKey = stringKeyConverter.encode(entry.getKey());
        ColumnRWHelper.store(rowKey, table, columnPrefix, configKey,
            null, entry.getValue());
      }
    }
  }

  /**
   * stores the {@linkplain TimelineMetric} information from the
   * {@linkplain TimelineEvent} object.
   */
  private <T extends BaseTable<T>> void storeMetrics(
      byte[] rowKey, Set<TimelineMetric> metrics,
      ColumnPrefix<T> columnPrefix, TypedBufferedMutator<T> table)
      throws IOException {
    if (metrics != null) {
      for (TimelineMetric metric : metrics) {
        byte[] metricColumnQualifier =
            stringKeyConverter.encode(metric.getId());
        Map<Long, Number> timeseries = metric.getValues();
        for (Map.Entry<Long, Number> timeseriesEntry : timeseries.entrySet()) {
          Long timestamp = timeseriesEntry.getKey();
          ColumnRWHelper.store(rowKey, table, columnPrefix,
              metricColumnQualifier, timestamp, timeseriesEntry.getValue());
        }
      }
    }
  }

  /**
   * Stores the events from the {@linkplain TimelineEvent} object.
   */
  private <T extends BaseTable<T>> void storeEvents(
      byte[] rowKey, Set<TimelineEvent> events,
      ColumnPrefix<T> columnPrefix, TypedBufferedMutator<T> table)
      throws IOException {
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event != null) {
          String eventId = event.getId();
          if (eventId != null) {
            long eventTimestamp = event.getTimestamp();
            // if the timestamp is not set, use the current timestamp
            if (eventTimestamp == TimelineEvent.INVALID_TIMESTAMP) {
              LOG.warn("timestamp is not set for event " + eventId +
                  "! Using the current timestamp");
              eventTimestamp = System.currentTimeMillis();
            }
            Map<String, Object> eventInfo = event.getInfo();
            if ((eventInfo == null) || (eventInfo.size() == 0)) {
              byte[] columnQualifierBytes =
                  new EventColumnName(eventId, eventTimestamp, null)
                      .getColumnQualifier();
              ColumnRWHelper.store(rowKey, table, columnPrefix,
                  columnQualifierBytes, null, Separator.EMPTY_BYTES);
            } else {
              for (Map.Entry<String, Object> info : eventInfo.entrySet()) {
                // eventId=infoKey
                byte[] columnQualifierBytes =
                    new EventColumnName(eventId, eventTimestamp, info.getKey())
                        .getColumnQualifier();
                ColumnRWHelper.store(rowKey, table, columnPrefix,
                    columnQualifierBytes, null, info.getValue());
              } // for info: eventInfo
            }
          }
        }
      } // event : events
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage
   * .TimelineWriter#aggregate
   * (org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity,
   * org.apache
   * .hadoop.yarn.server.timelineservice.storage.TimelineAggregationTrack)
   */
  @Override
  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter#flush
   * ()
   */
  @Override
  public void flush() throws IOException {
    // flush all buffered mutators
    entityTable.flush();
    appToFlowTable.flush();
    applicationTable.flush();
    flowRunTable.flush();
    flowActivityTable.flush();
    subApplicationTable.flush();
    domainTable.flush();
  }

  /**
   * close the hbase connections The close APIs perform flushing and release any
   * resources held.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (entityTable != null) {
      LOG.info("closing the entity table");
      // The close API performs flushing and releases any resources held
      entityTable.close();
    }
    if (appToFlowTable != null) {
      LOG.info("closing the app_flow table");
      // The close API performs flushing and releases any resources held
      appToFlowTable.close();
    }
    if (applicationTable != null) {
      LOG.info("closing the application table");
      applicationTable.close();
    }
    if (flowRunTable != null) {
      LOG.info("closing the flow run table");
      // The close API performs flushing and releases any resources held
      flowRunTable.close();
    }
    if (flowActivityTable != null) {
      LOG.info("closing the flowActivityTable table");
      // The close API performs flushing and releases any resources held
      flowActivityTable.close();
    }
    if (subApplicationTable != null) {
      subApplicationTable.close();
    }
    if (domainTable != null) {
      domainTable.close();
    }
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      conn.close();
    }
    super.serviceStop();
  }
}
