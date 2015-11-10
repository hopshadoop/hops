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
package io.hops.metadata.util;

import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.YarnVariables;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HopYarnAPIUtilities {

  private static final Log LOG = LogFactory.getLog(HopYarnAPIUtilities.class);
  public static final int RMCONTEXT_ID = 0;
  //NDB ids thar correspond to each field we want to update.
  //In our NDB table each variable is assigned to a specific row.
  public static final int YARN_VARIABLES_UPDATE_THRESHOLD = 5;
  public static final int UPDATED_CONTAINER_INFO = 0;
  public static final int NODEID = 1;
  public static final int NODE = 2;
  public static final int RESOURCE = 3;
  public static final int LIST = 4;
  public static final int NODEHBRESPONSE = 5;
  public static final int RMCONTEXT = 6;
  public static final int CONTAINERSTATUS = 7;
  public static final int CONTAINERID = 8;
  public static final int APPATTEMPTID = 9;
  public static final int APPLICATIONID = 10;
  public static final int INVOKEREQUEST = 11;
  public static final int FICASCHEDULERNODE = 12;
  public static final int RPC = 13;
  public static final int SCHEDULERAPPLICATION = 14;
  public static final int FICASCHEDULERAPP = 15;
  public static final int APPSCHEDULINGINFO = 16;
  public static final int RMCONTAINERID = 17;
  public static final int PRIORITYID = 18;
  public static final int QUEMETRICSID = 1;
  public final static Map<Integer, Integer> availableIDs =
      new HashMap<Integer, Integer>();
  public static final int AVAILABLE_IDS_INIT_VALUE = 0;
  //UpdatedContainerInfo Variables
  public static final int CONTAINER_NEWLYLAUNCHED = 0;
  public static final int CONTAINER_COMPLETED = 1;

  static {
    availableIDs.put(UPDATED_CONTAINER_INFO, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(NODEID, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(NODE, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(RESOURCE, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(LIST, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(NODEHBRESPONSE, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(RMCONTEXT, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(CONTAINERSTATUS, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(CONTAINERID, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(APPATTEMPTID, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(APPLICATIONID, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(INVOKEREQUEST, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(FICASCHEDULERNODE, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(RPC, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(SCHEDULERAPPLICATION, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(FICASCHEDULERAPP, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(APPSCHEDULINGINFO, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(RMCONTAINERID, AVAILABLE_IDS_INIT_VALUE);
    availableIDs.put(PRIORITYID, AVAILABLE_IDS_INIT_VALUE);
  }


  /**
   * Sets new value for yarn variable and returns old one to be used by
   * application.
   *
   * @param type
   * @return
   */
  public synchronized static int incrementYarnVariables(final int type,
          final int increment) {
    LightWeightRequestHandler setYarnVariablesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            LOG.debug(
                "HOP :: setYarnVariables Start-" + System.currentTimeMillis());
            YarnVariablesDataAccess yDA =
                (YarnVariablesDataAccess) YarnAPIStorageFactory
                    .getDataAccess(YarnVariablesDataAccess.class);
            YarnVariables found = (YarnVariables) yDA.findById(type);

            if (found != null) {
              int toReturn = found.getValue();
              found.setValue(found.getValue() + increment);
              yDA.add(found);
              connector.commit();
              LOG.debug("HOP :: setYarnVariables Finish-" +
                  System.currentTimeMillis());
              return toReturn;
            }
            return null;
          }
        };
    try {
      return (Integer) setYarnVariablesHandler.handle();
    } catch (IOException ex) {
      LOG.error("Error setting YarnVariables", ex);
    }
    return Integer.MIN_VALUE;
  }

  
  private static int currentRPCID = -1;
  private static int maxRPCID = -1;
  private static final int idsInterval = 1000;

  /**
   * Sets new value for yarn variable and returns old one to be used by
   * application.
   *
   * @param type
   * @return
   */
  public synchronized static int getRPCID() {
    if (currentRPCID == maxRPCID) {
        currentRPCID = incrementYarnVariables(RPC, idsInterval);
        maxRPCID = currentRPCID + idsInterval;
    }
    return currentRPCID++;
  }

  /**
   * Sets new value for yarn variable and returns old one to be used by
   * application. Fetches multiple PKs from db to memory. It first tries to
   * return a PK from memory and if no PK is available, it fetces another
   * batch from NDB. This is done to avoid going to the DB every time a PK is
   * needed.
   *
   * @param type
   * @return
   */
  public synchronized static int setYarnVariablesWithThreshold(final int type) {
    //If we have available PK in availableIDs, fetch it from memory
    //Else allocate more ids from database
    int id = availableIDs.get(type);
    if (id % YARN_VARIABLES_UPDATE_THRESHOLD != 0) {
      availableIDs.put(type, id + 1);
      LOG.debug("HOP :: return yarnVariable of type:" + type + ", val:" + id);
      return id;
    }
    LightWeightRequestHandler setYarnVariablesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            LOG.debug(
                "HOP :: setYarnVariables Start-" + System.currentTimeMillis());
            YarnVariablesDataAccess yDA =
                (YarnVariablesDataAccess) YarnAPIStorageFactory
                    .getDataAccess(YarnVariablesDataAccess.class);
            YarnVariables found = (YarnVariables) yDA.findById(type);
            //If found was null just try to retrieve it again
            int counter = 1;
            while (found == null && counter < 5) {
              try {
                LOG.info("HOP :: Retrying setYarnVariables=" + counter);
                found = (YarnVariables) yDA.findById(type);
                counter++;
                Thread.sleep(20);
              } catch (Exception ex) {
                LOG.error("HOP :: yarnVariable was null 2", ex);
              }
            }
            if (found != null) {
              availableIDs.put(type, found.getValue() + 1);
              int toReturn = found.getValue();
              found
                  .setValue(found.getValue() + YARN_VARIABLES_UPDATE_THRESHOLD);
              yDA.add(found);
              connector.commit();
              LOG.debug("HOP :: setYarnVariables Finish-" +
                  System.currentTimeMillis());
              return toReturn;
            }
            return null;

          }
        };
    try {
      return (Integer) setYarnVariablesHandler.handle();
    } catch (IOException ex) {
      LOG.error("Error setting YarnVariables", ex);
    }
    return Integer.MIN_VALUE;
  }


  //TODO remove code repetition with RMUtilities
  public static Resource getResourceLightweight(final String id, final int type,
      final int parent) {
    LightWeightRequestHandler getResourceHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.readCommitted();
            LOG.debug("START");
            ResourceDataAccess resDA =
                (ResourceDataAccess) YarnAPIStorageFactory
                    .getDataAccess(ResourceDataAccess.class);
            Resource found = (Resource) resDA.findEntry(id, type, parent);
            LOG.debug("HOP :: getResourceLightweight - Found resource:" + id);
            LOG.debug("FINISH");
            return found;
          }
        };
    try {
      return (Resource) getResourceHandler.handle();
    } catch (IOException ex) {
      LOG.error("Error getResourceLightweight:" + id, ex);
    }
    return null;
  }
}
