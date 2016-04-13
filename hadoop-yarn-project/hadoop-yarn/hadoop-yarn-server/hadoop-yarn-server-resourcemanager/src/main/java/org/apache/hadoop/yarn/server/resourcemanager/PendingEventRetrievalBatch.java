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
package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.common.GlobalThreadPool;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.entity.PendingEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically retrieves and processes pending events created by the
 * ResourceManagers.
 * <p/>
 */
public class PendingEventRetrievalBatch extends PendingEventRetrieval {

  private static final Log LOG =
      LogFactory.getLog(PendingEventRetrievalBatch.class);//recovered
  //When the scheduler starts for the first time, it must also retrieve
  //the events with status 'pending'. After that, it retrieves only 'new' events
  private boolean firstRetrieval;
  private final int period;
  private final Map<String, ConcurrentSkipListSet<PendingEvent>> pendingEvents;
  private final ConcurrentLinkedQueue<String> pendingNMs =
      new ConcurrentLinkedQueue<String>();
  private final WriteLock writeLock;

  /**
   * @param rmContext
   * @param period
   */
  public PendingEventRetrievalBatch(RMContext rmContext, Configuration conf) {
    super(rmContext, conf);
    this.firstRetrieval = true;
    this.period =
        conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD);
    LOG.debug("PendingEventRetrieval period=" + period);
    this.pendingEvents =
        new HashMap<String, ConcurrentSkipListSet<PendingEvent>>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.writeLock = lock.writeLock();
  }

  public void start() {
    if (!active) {
      active = true;
      retrivingThread = new Thread(new RetrivingThread());
      retrivingThread.setName("pending events retriver thread");
      retrivingThread.start();
    } else {
      LOG.error("ndb event retriver is already active");
    }
  }
  
  private class RetrivingThread implements Runnable {

    @Override
    public void run() {
      while (active) {
        try {
          long startTime = System.currentTimeMillis();
          try {
            writeLock.lock();
            //If scheduler just started, retrieve events with status pending 
            if (firstRetrieval) {
              pendingEvents.putAll(
                      RMUtilities.getPendingEvents(0,
                              TablesDef.PendingEventTableDef.PENDING));
              firstRetrieval = false;
            }
          //Retrieve all pending events, update their status
            //to pending and create scheduler events.
            pendingEvents.putAll(RMUtilities.getAndUpdatePendingEvents(
                    conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_BATCH,
                            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_BATCH),
                    TablesDef.PendingEventTableDef.NEW));
          LOG.debug("HOP :: pending events are:" + pendingEvents.size() + ", " +
              pendingEvents);
            //Parse all events grouped by RMNode
            for (String id : pendingEvents.keySet()) {
              //If this RMNode has not been processed yet
              if (!pendingNMs.contains(id)) {
                pendingNMs.add(id);
                GlobalThreadPool.getExecutorService().
                        execute(new RMNodeWorker(id));
              }
            }
          } finally {
            writeLock.unlock();
          }
          Thread.sleep(
                  Math.max(0, period - (System.currentTimeMillis() - startTime)));
        } catch (IOException ex) {
          LOG.error("HOP :: Error while retrieving PendingEvents", ex);
        } catch (InterruptedException ex) {
          LOG.error("HOP :: Error while retrieving PendingEvents", ex);
        }
      }
    }
  }

  public void setFirstRetrieval(boolean firstRetrieval) {
    this.firstRetrieval = firstRetrieval;
  }

  public class RMNodeWorker implements Runnable {

    private final String id;
    private RMNode rmNode;

    public RMNodeWorker(String id) {
      this.id = id;
    }

    @Override
    public void run() {
      LOG.debug("HOP :: RMNodeWorker:" + id + " - START");
      try {
        //Retrieve RMNode
        rmNode = RMUtilities.processHopRMNodeCompsForScheduler(RMUtilities.
                getRMNodeBatch(id), rmContext);
        LOG.debug("HOP :: RMNodeWorker rmNode:" + rmNode);
      } catch (IOException ex) {
        LOG.error("HOP :: Error retrieving rmNode:" + id, ex);
      }
      if (rmNode != null) {
        //Update Scheduler's rmContext objects

        updateRMContext(rmNode);

        //Parse and trigger events
        while (pendingEvents.containsKey(id)) {
          ConcurrentSkipListSet<PendingEvent> eventsToRemove = pendingEvents.
              get(id);
          for (PendingEvent pendingEvent : eventsToRemove) {
            try {
              triggerEvent(rmNode, pendingEvent, false);
            } catch (InterruptedException ex) {
              LOG.error(ex,ex);
              return;
            }
          }
          try {
            writeLock.lock();
            //Remove processed events
            pendingEvents.get(id).removeAll(eventsToRemove);
            if (pendingEvents.get(id).isEmpty()) {
              pendingNMs.remove(id);
              pendingEvents.remove(id);
            }
          } finally {
            writeLock.unlock();
          }
          LOG.debug("HOP :: RMNodeWorker:" + rmNode.getNodeID() + " - FINISH");
        }
      }
      try {
        writeLock.lock();
        
      } finally {
        writeLock.unlock();
      }
    }
  }

}
