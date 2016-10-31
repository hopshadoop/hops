package io.hops.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

/**
 * Created by antonis on 8/26/16.
 */
public abstract class StreamingReceiver {

    protected final Log LOG = LogFactory.getLog(StreamingReceiver.class);

    Thread retrievingThread = null;

    protected final RMContext rmContext;
    private Runnable retrievingRunnable;
    private final String threadName;
    protected boolean running = false;

    public StreamingReceiver(RMContext rmContext, String threadName) {
        this.rmContext = rmContext;
        this.threadName = threadName;
    }

    protected void setRetrievingRunnable(Runnable retrievingRunnable) {
        this.retrievingRunnable = retrievingRunnable;
    }

    public void start() {
        if (retrievingThread == null) {
          running = true;
            LOG.debug("HOP :: Creating " + threadName);
            retrievingThread = new Thread(retrievingRunnable);
            retrievingThread.setName(threadName);
            retrievingThread.start();
        } else {
            LOG.error("HOP :: " + threadName + " has already started");
        }
    }

    public void stop() {
      running =false;
        if (retrievingThread != null) {
            retrievingThread.interrupt();
        }
    }
}
