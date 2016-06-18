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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestLocalResourcesTrackerImpl {

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void test() {
    String user = "testuser";
    DrainDispatcher dispatcher = null;
    try {
      Configuration conf = new Configuration();
      dispatcher = createDispatcher(conf);
      EventHandler<LocalizerEvent> localizerEventHandler =
          mock(EventHandler.class);
      EventHandler<LocalizerEvent> containerEventHandler =
          mock(EventHandler.class);
      dispatcher.register(LocalizerEventType.class, localizerEventHandler);
      dispatcher.register(ContainerEventType.class, containerEventHandler);

      DeletionService mockDelService = mock(DeletionService.class);

      ContainerId cId1 = BuilderUtils.newContainerId(1, 1, 1, 1);
      LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
      ContainerId cId2 = BuilderUtils.newContainerId(1, 1, 1, 2);
      LocalizerContext lc2 = new LocalizerContext(user, cId2, null);

      LocalResourceRequest req1 = createLocalResourceRequest(user, 1, 1,
          LocalResourceVisibility.PUBLIC);
      LocalResourceRequest req2 = createLocalResourceRequest(user, 2, 1,
          LocalResourceVisibility.PUBLIC);
      LocalizedResource lr1 = createLocalizedResource(req1, dispatcher);
      LocalizedResource lr2 = createLocalizedResource(req2, dispatcher);
      ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc =
          new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
      localrsrc.put(req1, lr1);
      localrsrc.put(req2, lr2);
      LocalResourcesTracker tracker =
          new LocalResourcesTrackerImpl(user, dispatcher, localrsrc, false,
              conf);

      ResourceEvent req11Event =
          new ResourceRequestEvent(req1, LocalResourceVisibility.PUBLIC, lc1);
      ResourceEvent req12Event =
          new ResourceRequestEvent(req1, LocalResourceVisibility.PUBLIC, lc2);
      ResourceEvent req21Event =
          new ResourceRequestEvent(req2, LocalResourceVisibility.PUBLIC, lc1);

      ResourceEvent rel11Event = new ResourceReleaseEvent(req1, cId1);
      ResourceEvent rel12Event = new ResourceReleaseEvent(req1, cId2);
      ResourceEvent rel21Event = new ResourceReleaseEvent(req2, cId1);

      // Localize R1 for C1
      tracker.handle(req11Event);

      // Localize R1 for C2
      tracker.handle(req12Event);

      // Localize R2 for C1
      tracker.handle(req21Event);

      dispatcher.await();
      verify(localizerEventHandler, times(3))
          .handle(any(LocalizerResourceRequestEvent.class));
      // Verify refCount for R1 is 2
      Assert.assertEquals(2, lr1.getRefCount());
      // Verify refCount for R2 is 1
      Assert.assertEquals(1, lr2.getRefCount());

      // Release R2 for C1
      tracker.handle(rel21Event);

      dispatcher.await();
      verifyTrackedResourceCount(tracker, 2);

      // Verify resource with non zero ref count is not removed.
      Assert.assertEquals(2, lr1.getRefCount());
      Assert.assertFalse(tracker.remove(lr1, mockDelService));
      verifyTrackedResourceCount(tracker, 2);

      // Localize resource1
      ResourceLocalizedEvent rle =
          new ResourceLocalizedEvent(req1, new Path("file:///tmp/r1"), 1);
      lr1.handle(rle);
      Assert.assertTrue(lr1.getState().equals(ResourceState.LOCALIZED));

      // Release resource1
      tracker.handle(rel11Event);
      tracker.handle(rel12Event);
      Assert.assertEquals(0, lr1.getRefCount());

      // Verify resources in state LOCALIZED with ref-count=0 is removed.
      Assert.assertTrue(tracker.remove(lr1, mockDelService));
      verifyTrackedResourceCount(tracker, 1);
    } finally {
      if (dispatcher != null) {
        dispatcher.stop();
      }
    }
  }

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void testConsistency() {
    String user = "testuser";
    DrainDispatcher dispatcher = null;
    try {
      Configuration conf = new Configuration();
      dispatcher = createDispatcher(conf);
      EventHandler<LocalizerEvent> localizerEventHandler =
          mock(EventHandler.class);
      EventHandler<LocalizerEvent> containerEventHandler =
          mock(EventHandler.class);
      dispatcher.register(LocalizerEventType.class, localizerEventHandler);
      dispatcher.register(ContainerEventType.class, containerEventHandler);

      ContainerId cId1 = BuilderUtils.newContainerId(1, 1, 1, 1);
      LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
      LocalResourceRequest req1 = createLocalResourceRequest(user, 1, 1,
          LocalResourceVisibility.PUBLIC);
      LocalizedResource lr1 = createLocalizedResource(req1, dispatcher);
      ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc =
          new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
      localrsrc.put(req1, lr1);
      LocalResourcesTracker tracker =
          new LocalResourcesTrackerImpl(user, dispatcher, localrsrc, false,
              conf);

      ResourceEvent req11Event =
          new ResourceRequestEvent(req1, LocalResourceVisibility.PUBLIC, lc1);

      ResourceEvent rel11Event = new ResourceReleaseEvent(req1, cId1);

      // Localize R1 for C1
      tracker.handle(req11Event);

      dispatcher.await();

      // Verify refCount for R1 is 1
      Assert.assertEquals(1, lr1.getRefCount());

      dispatcher.await();
      verifyTrackedResourceCount(tracker, 1);

      // Localize resource1
      ResourceLocalizedEvent rle =
          new ResourceLocalizedEvent(req1, new Path("file:///tmp/r1"), 1);
      lr1.handle(rle);
      Assert.assertTrue(lr1.getState().equals(ResourceState.LOCALIZED));
      Assert.assertTrue(createdummylocalizefile(new Path("file:///tmp/r1")));
      LocalizedResource rsrcbefore = tracker.iterator().next();
      File resFile =
          new File(lr1.getLocalPath().toUri().getRawPath().toString());
      Assert.assertTrue(resFile.exists());
      Assert.assertTrue(resFile.delete());

      // Localize R1 for C1
      tracker.handle(req11Event);

      dispatcher.await();
      lr1.handle(rle);
      Assert.assertTrue(lr1.getState().equals(ResourceState.LOCALIZED));
      LocalizedResource rsrcafter = tracker.iterator().next();
      if (rsrcbefore == rsrcafter) {
        Assert.fail("Localized resource should not be equal");
      }
      // Release resource1
      tracker.handle(rel11Event);
    } finally {
      if (dispatcher != null) {
        dispatcher.stop();
      }
    }
  }

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void testLocalResourceCache() throws InterruptedException {
    String user = "testuser";
    DrainDispatcher dispatcher = null;
    try {
      Configuration conf = new Configuration();
      dispatcher = createDispatcher(conf);

      EventHandler<LocalizerEvent> localizerEventHandler =
          mock(EventHandler.class);
      EventHandler<ContainerEvent> containerEventHandler =
          mock(EventHandler.class);

      // Registering event handlers.
      dispatcher.register(LocalizerEventType.class, localizerEventHandler);
      dispatcher.register(ContainerEventType.class, containerEventHandler);

      ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc =
          new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
      LocalResourcesTracker tracker =
          new LocalResourcesTrackerImpl(user, dispatcher, localrsrc, true,
              conf);

      LocalResourceRequest lr = createLocalResourceRequest(user, 1, 1,
          LocalResourceVisibility.PUBLIC);

      // Creating 2 containers for same application which will be requesting
      // same local resource.
      // Container 1 requesting local resource.
      ContainerId cId1 = BuilderUtils.newContainerId(1, 1, 1, 1);
      LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
      ResourceEvent reqEvent1 =
          new ResourceRequestEvent(lr, LocalResourceVisibility.PRIVATE, lc1);

      // No resource request is initially present in local cache
      Assert.assertEquals(0, localrsrc.size());

      // Container-1 requesting local resource.
      tracker.handle(reqEvent1);
      //leting the time to all the event triggered by the handler to be handle
      Thread.sleep(1000);
      
      // New localized Resource should have been added to local resource map
      // and the requesting container will be added to its waiting queue.
      Assert.assertEquals(1, localrsrc.size());
      Assert.assertTrue(localrsrc.containsKey(lr));
      Assert.assertEquals(1, localrsrc.get(lr).getRefCount());
      Assert.assertTrue(localrsrc.get(lr).ref.contains(cId1));
      Assert.assertEquals(ResourceState.DOWNLOADING,
          localrsrc.get(lr).getState());

      // Container 2 requesting the resource
      ContainerId cId2 = BuilderUtils.newContainerId(1, 1, 1, 2);
      LocalizerContext lc2 = new LocalizerContext(user, cId2, null);
      ResourceEvent reqEvent2 =
          new ResourceRequestEvent(lr, LocalResourceVisibility.PRIVATE, lc2);
      tracker.handle(reqEvent2);
      //leting the time to all the event trigered by the handler to be handle
      Thread.sleep(1000);
      
      // Container 2 should have been added to the waiting queue of the local
      // resource
      Assert.assertEquals(2, localrsrc.get(lr).getRefCount());
      Assert.assertTrue(localrsrc.get(lr).ref.contains(cId2));

      // Failing resource localization
      ResourceEvent resourceFailedEvent =
          new ResourceFailedLocalizationEvent(lr,
              (new Exception("test").getMessage()));
      
      // Backing up the resource to track its state change as it will be
      // removed after the failed event.
      LocalizedResource localizedResource = localrsrc.get(lr);
      
      tracker.handle(resourceFailedEvent);
      //leting the time to all the event trigered by the handler to be handle
      Thread.sleep(1000);
      
      // After receiving failed resource event; all waiting containers will be
      // notified with Container Resource Failed Event.
      Assert.assertEquals(0, localrsrc.size());
      verify(containerEventHandler, times(2))
          .handle(isA(ContainerResourceFailedEvent.class));
      Assert.assertEquals(ResourceState.FAILED, localizedResource.getState());

      // Container 1 trying to release the resource (This resource is already
      // deleted from the cache. This call should return silently without
      // exception.
      ResourceReleaseEvent relEvent1 = new ResourceReleaseEvent(lr, cId1);
      tracker.handle(relEvent1);
      //leting the time to all the event trigered by the handler to be handle
      Thread.sleep(1000);
      
      // Container-3 now requests for the same resource. This request call
      // is coming prior to Container-2's release call.
      ContainerId cId3 = BuilderUtils.newContainerId(1, 1, 1, 3);
      LocalizerContext lc3 = new LocalizerContext(user, cId3, null);
      ResourceEvent reqEvent3 =
          new ResourceRequestEvent(lr, LocalResourceVisibility.PRIVATE, lc3);
      tracker.handle(reqEvent3);
      //leting the time to all the event trigered by the handler to be handle
      Thread.sleep(1000);
      
      // Local resource cache now should have the requested resource and the
      // number of waiting containers should be 1.
      Assert.assertEquals(1, localrsrc.size());
      Assert.assertTrue(localrsrc.containsKey(lr));
      Assert.assertEquals(1, localrsrc.get(lr).getRefCount());
      Assert.assertTrue(localrsrc.get(lr).ref.contains(cId3));

      // Container-2 Releases the resource
      ResourceReleaseEvent relEvent2 = new ResourceReleaseEvent(lr, cId2);
      tracker.handle(relEvent2);
      //leting the time to all the event trigered by the handler to be handle
      Thread.sleep(1000);
      
      // Making sure that there is no change in the cache after the release.
      Assert.assertEquals(1, localrsrc.size());
      Assert.assertTrue(localrsrc.containsKey(lr));
      Assert.assertEquals(1, localrsrc.get(lr).getRefCount());
      Assert.assertTrue(localrsrc.get(lr).ref.contains(cId3));
      
      // Sending ResourceLocalizedEvent to tracker. In turn resource should
      // send Container Resource Localized Event to waiting containers.
      Path localizedPath = new Path("/tmp/file1");
      ResourceLocalizedEvent localizedEvent =
          new ResourceLocalizedEvent(lr, localizedPath, 123L);
      tracker.handle(localizedEvent);
      //leting the time to all the event trigered by the handler to be handle
      Thread.sleep(1000);
      
      // Verifying ContainerResourceLocalizedEvent .
      verify(containerEventHandler, times(1))
          .handle(isA(ContainerResourceLocalizedEvent.class));
      Assert
          .assertEquals(ResourceState.LOCALIZED, localrsrc.get(lr).getState());
      Assert.assertEquals(1, localrsrc.get(lr).getRefCount());
      
      // Container-3 releasing the resource.
      ResourceReleaseEvent relEvent3 = new ResourceReleaseEvent(lr, cId3);
      tracker.handle(relEvent3);
      //leting the time to all the event trigered by the handler to be handle
      Thread.sleep(1000);
      
      Assert.assertEquals(0, localrsrc.get(lr).getRefCount());
      
    } finally {
      if (dispatcher != null) {
        dispatcher.stop();
      }
    }
  }

  @Test(timeout = 100000)
  @SuppressWarnings("unchecked")
  public void testHierarchicalLocalCacheDirectories() {
    String user = "testuser";
    DrainDispatcher dispatcher = null;
    try {
      Configuration conf = new Configuration();
      // setting per directory file limit to 1.
      conf.set(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY, "37");
      dispatcher = createDispatcher(conf);

      EventHandler<LocalizerEvent> localizerEventHandler =
          mock(EventHandler.class);
      EventHandler<LocalizerEvent> containerEventHandler =
          mock(EventHandler.class);
      dispatcher.register(LocalizerEventType.class, localizerEventHandler);
      dispatcher.register(ContainerEventType.class, containerEventHandler);

      DeletionService mockDelService = mock(DeletionService.class);

      ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc =
          new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
      LocalResourcesTracker tracker =
          new LocalResourcesTrackerImpl(user, dispatcher, localrsrc, true,
              conf);

      // This is a random path. NO File creation will take place at this place.
      Path localDir = new Path("/tmp");

      // Container 1 needs lr1 resource
      ContainerId cId1 = BuilderUtils.newContainerId(1, 1, 1, 1);
      LocalResourceRequest lr1 = createLocalResourceRequest(user, 1, 1,
          LocalResourceVisibility.PUBLIC);
      LocalizerContext lc1 = new LocalizerContext(user, cId1, null);

      // Container 1 requests lr1 to be localized
      ResourceEvent reqEvent1 =
          new ResourceRequestEvent(lr1, LocalResourceVisibility.PUBLIC, lc1);
      tracker.handle(reqEvent1);

      // Simulate the process of localization of lr1
      Path hierarchicalPath1 = tracker.getPathForLocalization(lr1, localDir);
      // Simulate lr1 getting localized
      ResourceLocalizedEvent rle1 = new ResourceLocalizedEvent(lr1,
          new Path(hierarchicalPath1.toUri().toString() +
              Path.SEPARATOR + "file1"), 120);
      tracker.handle(rle1);
      // Localization successful.

      LocalResourceRequest lr2 = createLocalResourceRequest(user, 3, 3,
          LocalResourceVisibility.PUBLIC);
      // Container 1 requests lr2 to be localized.
      ResourceEvent reqEvent2 =
          new ResourceRequestEvent(lr2, LocalResourceVisibility.PUBLIC, lc1);
      tracker.handle(reqEvent2);

      Path hierarchicalPath2 = tracker.getPathForLocalization(lr2, localDir);
      // localization failed.
      ResourceFailedLocalizationEvent rfe2 =
          new ResourceFailedLocalizationEvent(lr2,
              new Exception("Test").toString());
      tracker.handle(rfe2);

      /*
       * The path returned for two localization should be different because we
       * are limiting one file per sub-directory.
       */
      Assert.assertNotSame(hierarchicalPath1, hierarchicalPath2);

      LocalResourceRequest lr3 = createLocalResourceRequest(user, 2, 2,
          LocalResourceVisibility.PUBLIC);
      ResourceEvent reqEvent3 =
          new ResourceRequestEvent(lr3, LocalResourceVisibility.PUBLIC, lc1);
      tracker.handle(reqEvent3);
      Path hierarchicalPath3 = tracker.getPathForLocalization(lr3, localDir);
      // localization successful
      ResourceLocalizedEvent rle3 = new ResourceLocalizedEvent(lr3, new Path(
          hierarchicalPath3.toUri().toString() + Path.SEPARATOR + "file3"),
          120);
      tracker.handle(rle3);

      // Verifying that path created is inside the subdirectory
      Assert.assertEquals(hierarchicalPath3.toUri().toString(),
          hierarchicalPath1.toUri().toString() + Path.SEPARATOR + "0");

      // Container 1 releases resource lr1
      ResourceEvent relEvent1 = new ResourceReleaseEvent(lr1, cId1);
      tracker.handle(relEvent1);

      // Validate the file counts now
      int resources = 0;
      Iterator<LocalizedResource> iter = tracker.iterator();
      while (iter.hasNext()) {
        iter.next();
        resources++;
      }
      // There should be only two resources lr1 and lr3 now.
      Assert.assertEquals(2, resources);

      // Now simulate cache cleanup - removes unused resources.
      iter = tracker.iterator();
      while (iter.hasNext()) {
        LocalizedResource rsrc = iter.next();
        if (rsrc.getRefCount() == 0) {
          Assert.assertTrue(tracker.remove(rsrc, mockDelService));
          resources--;
        }
      }
      // lr1 is not used by anyone and will be removed, only lr3 will hang
      // around
      Assert.assertEquals(1, resources);
    } finally {
      if (dispatcher != null) {
        dispatcher.stop();
      }
    }
  }

  private boolean createdummylocalizefile(Path path) {
    boolean ret = false;
    File file = new File(path.toUri().getRawPath().toString());
    try {
      ret = file.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ret;
  }
  
  private void verifyTrackedResourceCount(LocalResourcesTracker tracker,
      int expected) {
    int count = 0;
    Iterator<LocalizedResource> iter = tracker.iterator();
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    Assert
        .assertEquals("Tracker resource count does not match", expected, count);
  }

  private LocalResourceRequest createLocalResourceRequest(String user, int i,
      long ts, LocalResourceVisibility vis) {
    final LocalResourceRequest req =
        new LocalResourceRequest(new Path("file:///tmp/" + user + "/rsrc" + i),
            ts + i * 2000, LocalResourceType.FILE, vis, null);
    return req;
  }

  private LocalizedResource createLocalizedResource(LocalResourceRequest req,
      Dispatcher dispatcher) {
    LocalizedResource lr = new LocalizedResource(req, dispatcher);
    return lr;
  }

  private DrainDispatcher createDispatcher(Configuration conf) {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    return dispatcher;
  }
}
