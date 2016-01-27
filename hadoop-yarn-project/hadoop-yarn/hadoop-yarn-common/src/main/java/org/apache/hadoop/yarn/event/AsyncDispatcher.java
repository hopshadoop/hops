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

package org.apache.hadoop.yarn.event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 */
@SuppressWarnings("rawtypes")
@Public
@Evolving
public class AsyncDispatcher extends AbstractService implements Dispatcher {

  private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);

  private final BlockingQueue<Event> eventQueue;
  private volatile boolean stopped = false;

  // Configuration flag for enabling/disabling draining dispatcher's events on
  // stop functionality.
  private volatile boolean drainEventsOnStop = false;

  // Indicates all the remaining dispatcher's events on stop have been drained
  // and processed.
  private volatile boolean drained = true;
  private Object waitForDrained = new Object();

  // For drainEventsOnStop enabled only, block newly coming events into the
  // queue while stopping.
  private volatile boolean blockNewEvents = false;
  private EventHandler handlerInstance = null;

  private Thread eventHandlingThread;
  protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
  private boolean exitOnDispatchException;

  public AsyncDispatcher() {
    this(new LinkedBlockingQueue<Event>());
  }

  public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
    super("Dispatcher");
    this.eventQueue = eventQueue;
    this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
  }

  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          drained = eventQueue.isEmpty();
          // blockNewEvents is only set when dispatcher is draining to stop,
          // adding this check is to avoid the overhead of acquiring the lock
          // and calling notify every time in the normal run of the loop.
          if (blockNewEvents) {
            synchronized (waitForDrained) {
              if (drained) {
                waitForDrained.notify();
              }
            }
          }
          Event event;
          try {
            event = eventQueue.take();
          } catch (InterruptedException ie) {
            if (!stopped) {
              LOG.warn("AsyncDispatcher thread interrupted", ie);
            }
            return;
          }
          if (event != null) {
            dispatch(event);
          }
        }
      }
    };
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.exitOnDispatchException =
        conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
            Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    //start all the components
    super.serviceStart();
    eventHandlingThread = new Thread(createThread());
    eventHandlingThread.setName("AsyncDispatcher event handler");
    eventHandlingThread.start();
  }

  public void setDrainEventsOnStop() {
    drainEventsOnStop = true;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (drainEventsOnStop) {
      blockNewEvents = true;
      LOG.info("AsyncDispatcher is draining to stop, igonring any new events.");
      synchronized (waitForDrained) {
        while (!drained && eventHandlingThread.isAlive()) {
          waitForDrained.wait(1000);
          LOG.info("Waiting for AsyncDispatcher to drain.");
        }
      }
    }
    stopped = true;
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      try {
        eventHandlingThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }

    // stop all the components
    super.serviceStop();
  }

  @SuppressWarnings("unchecked")
  protected void dispatch(Event event) {
    //all events go thru this loop
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatching the event " + event.getClass().getName() + "." +
          event.toString());
    }

    Class<? extends Enum> type = event.getType().getDeclaringClass();

    try {
      EventHandler handler = eventDispatchers.get(type);
      if (handler != null) {
        handler.handle(event);
        if (event instanceof AbstractEventTransaction) {
          AbstractEventTransaction eventT = (AbstractEventTransaction) event;
          if (eventT.getTransactionState() != null) {
            eventT.getTransactionState().decCounter(event.getType());
          }
        }
      } else {
        throw new Exception("No handler for registered for " + type);
      }
    } catch (Throwable t) {
      //TODO Maybe log the state of the queue
      LOG.fatal("Error in dispatcher thread", t);
      // If serviceStop is called, we should exit this thread gracefully.
      if (exitOnDispatchException &&
          (ShutdownHookManager.get().isShutdownInProgress()) == false &&
          stopped == false) {
        LOG.info("Exiting, bbye..");
        ExitUtil.terminate(-1);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void register(Class<? extends Enum> eventType, EventHandler handler) {
    /* check to see if we have a listener registered */
    EventHandler<Event> registeredHandler =
        (EventHandler<Event>) eventDispatchers.get(eventType);
    LOG.info("Registering " + eventType + " for " + handler.getClass());
    if (registeredHandler == null) {
      eventDispatchers.put(eventType, handler);
    } else if (!(registeredHandler instanceof MultiListenerHandler)) {
      /* for multiple listeners of an event add the multiple listener handler */
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      eventDispatchers.put(eventType, multiHandler);
    } else {
      /* already a multilistener, just add to it */
      MultiListenerHandler multiHandler =
          (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
  }

  @Override
  public EventHandler getEventHandler() {
    if (handlerInstance == null) {
      handlerInstance = new GenericEventHandler();
    }
    return handlerInstance;
  }

  class GenericEventHandler implements EventHandler<Event> {
    public void handle(Event event) {
      if (blockNewEvents) {
        return;
      }
      drained = false;

      /* all this method does is enqueue all the events onto the queue */
      int qSize = eventQueue.size();
      if (qSize != 0 && qSize % 1000 == 0) {
        LOG.info("Size of event-queue is " + qSize);
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn(
            "Very low remaining capacity in the event-queue: " + remCapacity);
      }
      try {
        if (event instanceof AbstractEventTransaction) {
          AbstractEventTransaction eventT = (AbstractEventTransaction) event;
          if (eventT.getTransactionState() != null) {
            eventT.getTransactionState().incCounter(event.getType());
          }
        }
        eventQueue.put(event);
      } catch (InterruptedException e) {
        if (!stopped) {
          LOG.warn("AsyncDispatcher thread interrupted", e);
        }
        throw new YarnRuntimeException(e);
      }
    }

    ;
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   *
   * @param <T>
   *     the type of event these multiple handlers are interested in.
   */
  static class MultiListenerHandler implements EventHandler<Event> {
    List<EventHandler<Event>> listofHandlers;

    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<Event>>();
    }

    @Override
    public void handle(Event event) {
      for (EventHandler<Event> handler : listofHandlers) {
        handler.handle(event);
      }
    }

    void addHandler(EventHandler<Event> handler) {
      listofHandlers.add(handler);
      }

  }
}
