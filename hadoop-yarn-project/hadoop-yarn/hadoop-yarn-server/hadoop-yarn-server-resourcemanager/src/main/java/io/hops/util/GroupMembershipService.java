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
package io.hops.util;

import io.hops.leaderElection.LeaderElection;
import io.hops.leaderElection.YarnLeDescriptorFactory;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.yarn.entity.Load;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import io.hops.util.impl.ActiveRMPBImpl;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;

public class GroupMembershipService extends CompositeService
    implements GroupMembership, HAServiceProtocol {

  private static final Log LOG =
      LogFactory.getLog(GroupMembershipService.class);
  private final RMContext rmContext;
  private final ResourceManager rm;
  private Server server;
  private AccessControlList adminAcl;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private LeaderElection groupMembership;
  private boolean autoFailoverEnabled;
  private InetSocketAddress groupMembershipServiceAddress;
  boolean running = true;
  private String hostname = "";
  private Thread lEnGmMonitor;
  private YarnAuthorizationProvider authorizer;
  private UserGroupInformation daemonUser;
  
  public GroupMembershipService(ResourceManager rm, RMContext rmContext) {
    super(GroupMembershipService.class.getName());
    this.rm = rm;
    this.rmContext = rmContext;
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {

    groupMembershipServiceAddress =
        conf.getSocketAddr(YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_GROUP_MEMBERSHIP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_GROUP_MEMBERSHIP_PORT);
    adminAcl = new AccessControlList(conf.get(YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    this.hostname = groupMembershipServiceAddress.toString();
    daemonUser = UserGroupInformation.getCurrentUser();
    authorizer = YarnAuthorizationProvider.getInstance(conf);
    authorizer.setAdmins(getAdminAclList(conf), UserGroupInformation
        .getCurrentUser());
    
    LOG.info("init groupMembershipService " + this.hostname);

    if (rmContext.isHAEnabled() || rmContext.isDistributed()) {
      initLEandGM(conf);
    }
  }

  private AccessControlList getAdminAclList(Configuration conf) {
    AccessControlList aclList =
        new AccessControlList(conf.get(YarnConfiguration.YARN_ADMIN_ACL,
          YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    aclList.addUser(daemonUser.getShortUserName());
    return aclList;
  }
  
  @Override
  protected synchronized void serviceStart() throws Exception {
    startGroupMembership();
    startServer();
    /*getConfig().updateConnectAddr(YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS,
            server.getListenerAddress());*/

    /*getConfig().updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
            YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_GROUP_MEMBERSHIP_ADDRESS,
            server.getListenerAddress());*/

    // TODO: For the moment this is needed, otherwise tests fail, I should check it later
    LOG.info("Started GMS on " + this.server.getListenerAddress().getHostName()
            + ":" + this.server.getPort());
    /*getConfig().set(YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS,
            this.server.getListenerAddress().getHostName() + ":" + this.server.getPort());
    getConfig().setInt(YarnConfiguration.RM_GROUP_MEMBERSHIP_PORT, this.server.getPort());*/

    getConfig().updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
            YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_GROUP_MEMBERSHIP_ADDRESS,
            server.getListenerAddress());
    super.serviceStart();
  }

  protected synchronized void startGroupMembership() {
    if (groupMembership != null) {
      groupMembership.start();

      try {
        groupMembership.waitActive();
      } catch (InterruptedException e) {
        LOG.warn("Group membership service was interrupted");
      }

      lEnGmMonitor = new Thread(new LEnGmMonitor());
      lEnGmMonitor.setName("group membership monitor");
      lEnGmMonitor.start();
    }
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    stopServer();
    stopGroupMembership();
    super.serviceStop();
  }

  protected synchronized void stopGroupMembership() throws Exception {
    if (groupMembership != null && groupMembership.isRunning()) {
      groupMembership.stopElectionThread();
    }
  }

  protected void startServer() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server = rpc.getServer(GroupMembership.class, this,
        groupMembershipServiceAddress, conf, null,
        conf.getInt(YarnConfiguration.RM_GROUP_MEMBERSHIP_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_GROUP_MEMBERSHIP_CLIENT_THREAD_COUNT));
    this.server.start();
  }

  protected void stopServer() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
  }

  private synchronized Configuration getConfiguration(Configuration conf,
      String confFileName) throws YarnException, IOException {
    InputStream confFileInputStream = this.rmContext.getConfigurationProvider()
        .getConfigurationInputStream(conf, confFileName);
    if (confFileInputStream != null) {
      conf.addResource(confFileInputStream);
    }
    return conf;
  }

  public String getHostname() {
    return hostname;
  }

  public boolean isLeader() {
    if (groupMembership != null && groupMembership.isRunning()) {
      return groupMembership.isLeader();
    } else {
      return false;
    }
  }
  
  public boolean isLeadingRT(){
    if(groupMembership!=null && groupMembership.isRunning()){
      return groupMembership.isSecond();
    }else{
      return false;
    }
  }

  public boolean isAlone(){
    if(groupMembership.getActiveNamenodes().size()==1){
      return true;
    }else{
      return false;
    }
  }
  
  @Override
  public synchronized void monitorHealth() throws IOException {
    checkAccess("monitorHealth");
    if (isRMActive() && !rm.areSchedulerServicesRunning()) {
      throw new HealthCheckFailedException(
          "Active ResourceManager services are not running!");
    }
  }

  private UserGroupInformation checkAccess(String method) throws IOException {
    return RMServerUtils.verifyAdminAccess(authorizer, method, LOG);
  }

  private UserGroupInformation checkAcls(String method) throws YarnException {
    try {
      return checkAccess(method);
    } catch (IOException ioe) {
      throw RPCUtil.getRemoteException(ioe);
    }
  }

  private synchronized boolean isRMActive() {
    return HAServiceState.ACTIVE == rmContext.getHAServiceState();
  }

  private void throwStandbyException() throws StandbyException {
    throw new StandbyException(
        "ResourceManager " + hostname + " is not Active!");
  }

  @Override
  public synchronized void transitionToActive(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    // call refreshAdminAcls before HA state transition
    // for the case that adminAcls have been updated in previous active RM
    try {
      refreshAdminAcls(false);
    } catch (YarnException ex) {
      throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
    }
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public synchronized void transitionToStandby(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    // call refreshAdminAcls before HA state transition
    // for the case that adminAcls have been updated in previous active RM
    try {
      refreshAdminAcls(false);
    } catch (YarnException ex) {
      throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
    }
    throw new UnsupportedOperationException("not implemented yet");
  }

  private RefreshAdminAclsResponse refreshAdminAcls(boolean checkRMHAState)
      throws YarnException, IOException {
    String argName = "refreshAdminAcls";
    UserGroupInformation user = checkAcls(argName);

    if (checkRMHAState) {
      checkRMStatus(user.getShortUserName(), argName, "refresh Admin ACLs.");
    }
    Configuration conf =
        getConfiguration(new Configuration(false),
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    authorizer.setAdmins(getAdminAclList(conf), UserGroupInformation
        .getCurrentUser());
    RMAuditLogger.logSuccess(user.getShortUserName(), argName,
        "AdminService");

    return recordFactory.newRecordInstance(RefreshAdminAclsResponse.class);
  }
  
  private void checkRMStatus(String user, String argName, String msg)
      throws StandbyException {
    if (!isRMActive()) {
      RMAuditLogger.logFailure(user, argName, "", 
          "AdminService", "ResourceManager is not active. Can not " + msg);
      throwStandbyException();
    }
  }

  @Override
  public synchronized HAServiceStatus getServiceStatus() throws IOException {
    checkAccess("getServiceState");
    HAServiceState haState = rmContext.getHAServiceState();
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (isRMActive() || haState == HAServiceProtocol.HAServiceState.STANDBY) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  }

  @Override
  public LiveRMsResponse getLiveRMList() {

    List<ActiveNode> rmList = new ArrayList<ActiveNode>();
    Map<String, Load> loads;
    try {
      loads = DBUtility.getAllLoads();
    } catch (IOException ex) {
      LOG.error(ex);
      loads = new HashMap<String, Load>();
    }
    SortedActiveNodeList nnList = groupMembership.getActiveNamenodes();
    for (ActiveNode node : nnList.getSortedActiveNodes()) {
      if (loads.get(node.getHostname()) == null) {
        rmList.add(new ActiveRMPBImpl(node.getId(), node.getHostname(), node.
            getIpAddress(), node.getPort(), node.getHttpAddress(), 0));
      } else {
        rmList.add(new ActiveRMPBImpl(node.getId(), node.getHostname(), node.
            getIpAddress(), node.getPort(), node.getHttpAddress(), loads.
            get(node.getHostname()).getLoad()));
      }
    }
    SortedActiveRMList sortedRmList = new SortedActiveRMList(rmList);
    return YarnServerBuilderUtils.newLiveRMsResponse(sortedRmList);

  }

  private void initLEandGM(Configuration conf) throws IOException {
    // Initialize the leader election algorithm (only once rpc server is
    // created and httpserver is started)
    long leadercheckInterval =
        conf.getInt(CommonConfigurationKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
            CommonConfigurationKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
    int missedHeartBeatThreshold =
        conf.getInt(CommonConfigurationKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
            CommonConfigurationKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
    int leIncrement =
        conf.getInt(CommonConfigurationKeys.DFS_LEADER_TP_INCREMENT_KEY,
            CommonConfigurationKeys.DFS_LEADER_TP_INCREMENT_DEFAULT);

    groupMembership =
        new LeaderElection(new YarnLeDescriptorFactory(), leadercheckInterval,
            missedHeartBeatThreshold, leIncrement, "",
            groupMembershipServiceAddress.getAddress().getHostAddress() + ":"
                + groupMembershipServiceAddress.getPort());
  }

  private class LEnGmMonitor implements Runnable {

    Boolean previousLeaderRole = null;
    Boolean previousLeadingRTRole = null;
    
    @Override
    public void run() {
      try {
        while (true && groupMembership.isRunning()) {
          boolean currentLeaderRole = isLeader();
          if (previousLeaderRole == null ||
              currentLeaderRole != previousLeaderRole) {
            previousLeaderRole = currentLeaderRole;
            switchLeaderRole(previousLeaderRole);
          }
          boolean currentLeadingRTRole = isLeadingRT();
//          if(previousLeadingRTRole ==null || 
//                  currentLeadingRTRole != previousLeadingRTRole){
//            previousLeadingRTRole = currentLeadingRTRole;
//            switchLeadintRTRole(currentLeadingRTRole);
//          }
          Thread.sleep(100L);
        }
      } catch (Exception ex) {
        LOG.error(ex, ex);
      }
    }

    private void switchLeaderRole(boolean role) throws Exception {
      if (role) {
        LOG.info(groupMembership.getCurrentId() + " switching to active ");
        rm.transitionToActive();
      } else {
        LOG.info(groupMembership.getCurrentId() + " switching to standby ");
        rm.transitionToStandby(true);
      }
    }
    
//    private void switchLeadintRTRole(boolean isLeadingRT) throws Exception {
//      if (isLeadingRT) {
//        LOG.info(groupMembership.getCurrentId() + " switching to leading RT");
//        rm.transitionToLeadingRT();
//      } else {
//        LOG.info(groupMembership.getCurrentId() + " switching to nonleading RT ");
//        rm.transitionToNonLeadingRT();
//      }
//    }
  }

  public void relinquishId() throws InterruptedException {
    if(groupMembership!=null){
      groupMembership.relinquishCurrentIdInNextRound();
    }
  }

}
