package io.hops.services;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ServiceUtilsTest {
  static class MockService implements Service {
    static int called = 0;
    @Override
    public void start() {
      called++;
    }

    @Override
    public void stop() {
    }

    @Override
    public void waitStarted() throws InterruptedException {
    }

    @Override
    public boolean isRunning() {
      return false;
    }
  }

  @Before
  public void setUp() throws Exception {
    MockService.called = 0;
  }

  @Test
  public void doAll() throws Exception {
    Service s1 = new MockService();
    Service s2 = new MockService();
    ServiceUtils.doAll(s1, s2, s1, s1, s1, s2, s2).start();
    assertEquals(2, MockService.called);
  }
}