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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestUtils {
  private static final Log LOG = LogFactory.getLog(TestUtils.class);

  /**
   * Get a mock {@link RMContext} for use in test cases.
   * @return a mock {@link RMContext} for use in test cases
   */
  @SuppressWarnings({ "rawtypes", "unchecked" }) 
  public static RMContext getMockRMContext() {
    // Null dispatcher
    Dispatcher nullDispatcher = new Dispatcher() {
      private final EventHandler handler =
          new EventHandler() {
            @Override
            public void handle(Event event) {
            }
          };
      @Override
      public void register(Class<? extends Enum> eventType, 
          EventHandler handler) {
      }
      @Override
      public EventHandler<Event> getEventHandler() {
        return handler; 
      }
    };
    
    // No op
    ContainerAllocationExpirer cae =
        new ContainerAllocationExpirer(nullDispatcher);

    Configuration conf = new Configuration();
    RMApplicationHistoryWriter writer =  mock(RMApplicationHistoryWriter.class);
    RMContextImpl rmContext =
        new RMContextImpl(nullDispatcher, cae, null, null, null,
          new AMRMTokenSecretManager(conf, null),
          new RMContainerTokenSecretManager(conf),
          new NMTokenSecretManagerInRM(conf),
          new ClientToAMTokenSecretManagerInRM());
    RMNodeLabelsManager nlm = mock(RMNodeLabelsManager.class);
    when(
        nlm.getQueueResource(any(String.class), any(Set.class),
            any(Resource.class))).thenAnswer(new Answer<Resource>() {
      @Override
      public Resource answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return (Resource) args[2];
      }
    });
    
    when(nlm.getResourceByLabel(any(String.class), any(Resource.class)))
        .thenAnswer(new Answer<Resource>() {
          @Override public Resource answer(InvocationOnMock invocation)
              throws Throwable {
            Object[] args = invocation.getArguments();
            return (Resource) args[1];
          }
        });

    rmContext.setNodeLabelManager(nlm);
    rmContext.setSystemMetricsPublisher(mock(SystemMetricsPublisher.class));
    rmContext.setRMApplicationHistoryWriter(mock(RMApplicationHistoryWriter.class));
    ResourceScheduler mockScheduler = mock(ResourceScheduler.class);
    when(mockScheduler.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    rmContext.setScheduler(mockScheduler);

    AllocationTagsManager ptm = mock(AllocationTagsManager.class);
    rmContext.setAllocationTagsManager(ptm);

    return rmContext;
  }
  
  /**
   * Hook to spy on queues.
   */
  static class SpyHook extends CapacitySchedulerQueueManager.QueueHook {
    @Override
    public CSQueue hook(CSQueue queue) {
      return spy(queue);
    }
  }
  public static SpyHook spyHook = new SpyHook();
  
  private static final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  public static Priority createMockPriority( int priority) {
//    Priority p = mock(Priority.class);
//    when(p.getPriority()).thenReturn(priority);
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(priority);
    return p;
  }
  
  public static ResourceRequest createResourceRequest(
      String resourceName, int memory, int numContainers, boolean relaxLocality,
      Priority priority, RecordFactory recordFactory, String labelExpression) {
    return createResourceRequest(resourceName, memory, 1, numContainers,
        relaxLocality, priority, recordFactory, labelExpression);
  }

  public static ResourceRequest createResourceRequest(String resourceName,
      int memory, int vcores, int numContainers, boolean relaxLocality,
      Priority priority, RecordFactory recordFactory, String labelExpression) {
    ResourceRequest request =
        recordFactory.newRecordInstance(ResourceRequest.class);
    Resource capability = Resources.createResource(memory, vcores);

    request.setNumContainers(numContainers);
    request.setResourceName(resourceName);
    request.setCapability(capability);
    request.setRelaxLocality(relaxLocality);
    request.setPriority(priority);
    request.setNodeLabelExpression(labelExpression);
    return request;
  }
  
  public static ResourceRequest createResourceRequest(
      String resourceName, int memory, int numContainers, boolean relaxLocality,
 Priority priority,
      RecordFactory recordFactory) {
    return createResourceRequest(resourceName, memory, numContainers,
        relaxLocality, priority, recordFactory, RMNodeLabelsManager.NO_LABEL);
  }

  public static ApplicationId getMockApplicationId(int appId) {
    return ApplicationId.newInstance(0L, appId);
  }
  
  public static ApplicationAttemptId 
  getMockApplicationAttemptId(int appId, int attemptId) {
    ApplicationId applicationId = BuilderUtils.newApplicationId(0l, appId);
    return ApplicationAttemptId.newInstance(applicationId, attemptId);
  }

  public static FiCaSchedulerNode getMockNodeWithAttributes(String host,
      String rack, int port, int memory, Set<NodeAttribute> attributes) {
    FiCaSchedulerNode node = getMockNode(host, rack, port, memory, 1);
    when(node.getNodeAttributes()).thenReturn(attributes);
    return node;
  }

  public static FiCaSchedulerNode getMockNode(String host, String rack,
      int port, int memory) {
    return getMockNode(host, rack, port, memory, 1);
  }

  public static FiCaSchedulerNode getMockNode(String host, String rack,
      int port, int memory, int vcores) {
    NodeId nodeId = NodeId.newInstance(host, port);
    RMNode rmNode = mock(RMNode.class);
    when(rmNode.getNodeID()).thenReturn(nodeId);
    when(rmNode.getTotalCapability()).thenReturn(
        Resources.createResource(memory, vcores));
    when(rmNode.getNodeAddress()).thenReturn(host+":"+port);
    when(rmNode.getHostName()).thenReturn(host);
    when(rmNode.getRackName()).thenReturn(rack);
    when(rmNode.getState()).thenReturn(NodeState.RUNNING);
    
    FiCaSchedulerNode node = spy(new FiCaSchedulerNode(rmNode, false));
    LOG.info("node = " + host + " avail=" + node.getUnallocatedResource());
    
    when(node.getNodeID()).thenReturn(nodeId);
    return node;
  }

  @SuppressWarnings("deprecation")
  public static ContainerId getMockContainerId(FiCaSchedulerApp application) {
    ContainerId containerId = mock(ContainerId.class);
    doReturn(application.getApplicationAttemptId()).
    when(containerId).getApplicationAttemptId();
    long id = application.getNewContainerId();
    doReturn((int)id).when(containerId).getId();
    doReturn(id).when(containerId).getContainerId();
    return containerId;
  }

  public static ContainerId getMockContainerId(int appId, int containerId) {
    ApplicationAttemptId attemptId = getMockApplicationAttemptId(appId, 1);
    return ContainerId.newContainerId(attemptId, containerId);
  }
  
  public static Container getMockContainer(
      ContainerId containerId, NodeId nodeId, 
      Resource resource, Priority priority) {
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(containerId);
    when(container.getNodeId()).thenReturn(nodeId);
    when(container.getResource()).thenReturn(resource);
    when(container.getPriority()).thenReturn(priority);
    return container;
  }

  @SuppressWarnings("unchecked")
  public static <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }

  /**
   * Get a queue structure:
   * <pre>
   *             Root
   *            /  |  \
   *           a   b   c
   *           |   |   |
   *           a1  b1  c1
   *          (x)  (y)
   * </pre>  
   */
  public static Configuration getConfigurationWithQueueLabels(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setAccessibleNodeLabels(A, toSet("x"));
    conf.setCapacityByLabel(A, "x", 100);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setAccessibleNodeLabels(B, toSet("y"));
    conf.setCapacityByLabel(B, "y", 100);
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
    conf.setAccessibleNodeLabels(C, RMNodeLabelsManager.EMPTY_STRING_SET);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setCapacityByLabel(A1, "x", 100);
    
    final String B1 = B + ".b1";
    conf.setQueues(B, new String[] {"b1"});
    conf.setCapacity(B1, 100);
    conf.setMaximumCapacity(B1, 100);
    conf.setCapacityByLabel(B1, "y", 100);
    conf.setMaximumApplicationMasterResourcePerQueuePercent(B1, 1f);

    final String C1 = C + ".c1";
    conf.setQueues(C, new String[] {"c1"});
    conf.setCapacity(C1, 100);
    conf.setMaximumCapacity(C1, 100);
    
    return conf;
  }

  public static Configuration getComplexConfigurationWithQueueLabels(
      Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 10);
    conf.setAccessibleNodeLabels(A, toSet("x", "y"));
    conf.setCapacityByLabel(A, "x", 100);
    conf.setCapacityByLabel(A, "y", 50);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);
    conf.setMaximumCapacity(B, 100);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setCapacityByLabel(B, "y", 50);
    conf.setCapacityByLabel(B, "z", 100);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setAccessibleNodeLabels(A1, toSet("x", "y"));
    conf.setDefaultNodeLabelExpression(A1, "x");
    conf.setCapacityByLabel(A1, "x", 100);
    conf.setCapacityByLabel(A1, "y", 100);
    
    conf.setQueues(B, new String[] {"b1", "b2"});
    final String B1 = B + ".b1";
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 50);
    conf.setAccessibleNodeLabels(B1, RMNodeLabelsManager.EMPTY_STRING_SET);

    final String B2 = B + ".b2";
    conf.setCapacity(B2, 50);
    conf.setMaximumCapacity(B2, 50);
    conf.setAccessibleNodeLabels(B2, toSet("y", "z"));
    conf.setCapacityByLabel(B2, "y", 100);
    conf.setCapacityByLabel(B2, "z", 100);

    return conf;
  }
  
  public static Configuration getConfigurationWithDefaultQueueLabels(
      Configuration config) {
    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    
    CapacitySchedulerConfiguration conf =
        (CapacitySchedulerConfiguration) getConfigurationWithQueueLabels(config);
        new CapacitySchedulerConfiguration(config);
    conf.setDefaultNodeLabelExpression(A, "x");
    conf.setDefaultNodeLabelExpression(B, "y");
    return conf;
  }

  public static FiCaSchedulerApp getFiCaSchedulerApp(MockRM rm,
      ApplicationId appId) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    return cs.getSchedulerApplications().get(appId).getCurrentAppAttempt();
  }

  /**
   * Get a queue structure:
   * <pre>
   *             Root
   *            /  |  \
   *           a   b   c
   *          10   20  70
   * </pre>
   */
  public static Configuration
  getConfigurationWithMultipleQueues(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b", "c" });

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 100);
    conf.setUserLimitFactor(A, 100);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setMaximumCapacity(B, 100);
    conf.setUserLimitFactor(B, 100);

    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 100);
    conf.setUserLimitFactor(C, 100);

    return conf;
  }

  public static SchedulerRequestKey toSchedulerKey(Priority pri) {
    return SchedulerRequestKey.create(
        ResourceRequest.newInstance(pri, null, null, 0));
  }

  public static SchedulerRequestKey toSchedulerKey(int pri) {
    return SchedulerRequestKey.create(ResourceRequest.newInstance(
        Priority.newInstance(pri), null, null, 0));
  }

  public static SchedulerRequestKey toSchedulerKey(Priority pri,
      long allocationRequestId) {
    ResourceRequest req = ResourceRequest.newInstance(pri, null, null, 0);
    req.setAllocationRequestId(allocationRequestId);
    return SchedulerRequestKey.create(req);
  }

  public static void applyResourceCommitRequest(Resource clusterResource,
      CSAssignment csAssignment,
      final Map<NodeId, FiCaSchedulerNode> nodes,
      final Map<ApplicationAttemptId, FiCaSchedulerApp> apps)
      throws IOException {
    CapacityScheduler cs = new CapacityScheduler() {
      @Override
      public FiCaSchedulerNode getNode(NodeId nodeId) {
        return nodes.get(nodeId);
      }

      @Override
      public FiCaSchedulerApp getApplicationAttempt(
          ApplicationAttemptId applicationAttemptId) {
        return apps.get(applicationAttemptId);
      }
    };

    cs.setResourceCalculator(new DefaultResourceCalculator());

    cs.submitResourceCommitRequest(clusterResource,
        csAssignment);
  }

  /**
   * An easy way to create resources other than memory and vcores for tests.
   * @param memory memory
   * @param vcores vcores
   * @param nameToValues resource types other than memory and vcores.
   * @return created resource
   */
  public static Resource createResource(long memory, int vcores,
      Map<String, Integer> nameToValues) {
    Resource res = Resource.newInstance(memory, vcores);
    if (nameToValues != null) {
      for (Map.Entry<String, Integer> entry : nameToValues.entrySet()) {
        res.setResourceInformation(entry.getKey(), ResourceInformation
            .newInstance(entry.getKey(), "", entry.getValue()));
      }
    }
    return res;
  }
}
