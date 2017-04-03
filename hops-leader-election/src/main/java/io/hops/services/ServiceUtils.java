package io.hops.services;

import java.util.Arrays;
import java.util.HashSet;

public class ServiceUtils {

  private static class Services implements Service {
    private HashSet<Service> members;

    public Services(Service ...ss) {
      members = new HashSet<>();
      members.addAll(Arrays.asList(ss));
    }

    @Override
    public void start() {
      for(Service s: members) {
        s.start();
      }
    }

    @Override
    public void stop() {
      for(Service s: members) {
        s.stop();
      }
    }

    @Override
    public void waitStarted() throws InterruptedException {
      for(Service s: members) {
        s.waitStarted();
      }
    }

    @Override
    public boolean isRunning() {
      return false;
    }
  }


  /**
   * All operations applied to the {@link Service} returned by this method
   * will be run on all the services passed as parameters.
   * If some of the parameters refer to the same object, methods will be called
   * only once.
   * Note that {@link Service#isRunning()} on the resulting instance will always return false.
   * @param services services to execute the operation on.
   * @return a new service
   */
  public static Service doAll(Service ...services) {
    return new Services(services);
  }
}
