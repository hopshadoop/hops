package io.hops.services;

/**
 * A service provides an abstraction used by many internal components of hops.
 * These services can generally be started and stopped as well as queries on their status.
 */
public interface Service {
  /**
   * Starts the service.
   */
  void start();

  /**
   * Stops the service
   */
  void stop();

  /**
   * Waits until the service has started.
   */
  void waitStarted() throws InterruptedException;

  /**
   * Returns whether the service is running.
   */
  boolean isRunning();
}
