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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMWebAppFairScheduler {

    @Test
    public void testFairSchedulerWeAppPage() {
        List<RMAppState> appStates = Arrays.asList(RMAppState.NEW,
                RMAppState.SUBMITTED);
        final RMContext rmContext = mockRMContext(appStates);
        Injector injector = WebAppTests.createMockInjector(RMContext.class,
                rmContext,
                new Module() {
                    @Override
                    public void configure(Binder binder) {
                        try {
                            ResourceManager mockRmWithFairScheduler =
                                    mockRm(rmContext);
                            binder.bind(ResourceManager.class).toInstance
                                    (mockRmWithFairScheduler);

                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                });
        FairSchedulerPage fsViewInstance = injector.getInstance(FairSchedulerPage
                .class);
        fsViewInstance.render();
        WebAppTests.flushOutput(injector);
    }

    private static RMContext mockRMContext(List<RMAppState> states) {
        final ConcurrentMap<ApplicationId, RMApp> applicationsMaps = Maps.
                newConcurrentMap();
        int i = 0;
        for (RMAppState state : states) {
            MockRMApp app = new MockRMApp(i, i, state);
            applicationsMaps.put(app.getApplicationId(), app);
            i++;
        }

        return new RMContextImpl(null, null, null, null,
                null, null, null, null, null, null) {
            @Override
            public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
                return applicationsMaps;
            }
        };
    }

    private static ResourceManager mockRm(RMContext rmContext) throws
            IOException {
        ResourceManager rm = mock(ResourceManager.class);
        ResourceScheduler rs = mockFairScheduler();
        when(rm.getResourceScheduler()).thenReturn(rs);
        when(rm.getRMContext()).thenReturn(rmContext);
        return rm;
    }

    private static FairScheduler mockFairScheduler() throws IOException {
        FairScheduler fs = new FairScheduler();
        FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
        RMContext rmContext = new RMContextImpl(null, null, null, null, null,
                null, new RMContainerTokenSecretManager(conf, null), null,
                new ClientToAMTokenSecretManagerInRM(), null, conf);
        fs.reinitialize(conf, rmContext, null);
        return fs;
    }
}
