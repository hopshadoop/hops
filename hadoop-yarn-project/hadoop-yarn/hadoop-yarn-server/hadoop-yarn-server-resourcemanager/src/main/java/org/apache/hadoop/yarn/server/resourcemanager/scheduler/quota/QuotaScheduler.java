/*
 * Copyright 2015 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota;

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.YarnContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.YarnContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsDailyId;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 * @author rizvi
 */
public class QuotaScheduler implements Runnable {

    private static final Log LOG = LogFactory.getLog(QuotaScheduler.class);
    private StorageConnector connector = null;
    ApplicationStateDataAccess AppStatDS = null;
    Map<String , ApplicationState>  HopApplicationState = null;
    
    
    public void setupConfiguration() throws IOException {
        Configuration conf = new YarnConfiguration();
        LOG.info("DFS_STORAGE_DRIVER_JAR_FILE : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE);
        LOG.info("DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT);
        LOG.info("DFS_STORAGE_DRIVER_CLASS : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS);
        LOG.info("DFS_STORAGE_DRIVER_CLASS_DEFAULT : " + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS_DEFAULT);

        //YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);

        //RMUtilities.InitializeDB();
        this.connector = RMStorageFactory.getConnector();
        RequestHandler.setStorageConnector(this.connector);
    }

    @Override
    public void run() {
        LOG.info("Schedular is running in a thread!");

//        try {
//            setupConfiguration();
//        } catch (IOException ex) {
//            LOG.info("Schedular ran in exception while ndb setup configuration. " + ex.getMessage());
//        }
        //Get DataAccess and Map for ** ApplicationState **
        this.AppStatDS = (ApplicationStateDataAccess)RMStorageFactory.getDataAccess(ApplicationStateDataAccess.class);
        this.HopApplicationState = new HashMap<String , ApplicationState>();
            

        while ( !Thread.currentThread().isInterrupted()) {
            try {

                LightWeightRequestHandler bomb = new LightWeightRequestHandler(
                        YARNOperationType.TEST) {
                            @Override
                            public Object performTask() throws IOException {
                                connector.beginTransaction();
                                connector.writeLock();

                                //Get Data  ** ApplicationState **
                                ApplicationStateDataAccess _appStatDS = (ApplicationStateDataAccess)RMStorageFactory.getDataAccess(ApplicationStateDataAccess.class);
                                List<ApplicationState> hopApplicationStateList = _appStatDS.getAll();
                                Map<String , ApplicationState> hopApplicationState = new HashMap<String , ApplicationState>();
                                for ( ApplicationState _as :hopApplicationStateList)
                                  hopApplicationState.put(_as.getApplicationId(), _as);


                                //Get Data  ** YarnContainersLogs **
                                YarnContainersLogsDataAccess _csDA = (YarnContainersLogsDataAccess)RMStorageFactory.getDataAccess(YarnContainersLogsDataAccess.class);
                                Map<String , YarnContainersLogs> hopYarnContainersLogs = _csDA.getAll();

                                //Get Data  ** YarnProjectsQuota **
                                YarnProjectsQuotaDataAccess _pqDA = (YarnProjectsQuotaDataAccess)RMStorageFactory.getDataAccess(YarnProjectsQuotaDataAccess.class);
                                Map<String , YarnProjectsQuota> hopYarnProjectsQuotaList = _pqDA.getAll();
                                //YarnProjectsQuota _pq = (YarnProjectsQuota)hopYarnProjectsQuotaList.get("Project07");

                                //Get Data  ** YarnProjectsDailyCost **
                                YarnProjectsDailyCostDataAccess _pdcDA = (YarnProjectsDailyCostDataAccess)RMStorageFactory.getDataAccess(YarnProjectsDailyCostDataAccess.class);
                                Map<YarnProjectsDailyId , YarnProjectsDailyCost> hopYarnProjectsDailyCostList = _pdcDA.getAll();

                                long _miliSec = System.currentTimeMillis();            
                                final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);
                                Map<String, YarnProjectsQuota> chargedYarnProjectsQuota = new HashMap<String,YarnProjectsQuota>();
                                Map<YarnProjectsDailyId, YarnProjectsDailyCost> chargedYarnProjectsDailyCost = new HashMap<YarnProjectsDailyId, YarnProjectsDailyCost>();

                                List<YarnContainersLogs> toBeRemovedYarnContainersLogs = new ArrayList<YarnContainersLogs>();
                                List<YarnContainersLogs> toBeModifiedYarnContainersLogs = new ArrayList<YarnContainersLogs>();

                                // Calculate the quota 
                                LOG.info("RIZ:: YarnContainersLogs count : " + hopYarnContainersLogs.size());                    
                                for(  Map.Entry<String , YarnContainersLogs> _ycl : hopYarnContainersLogs.entrySet()){

                                    // Get ApplicationId from ContainerId
                                    LOG.info("RIZ:: YarnContainersLogs entry : " + _ycl.getValue().toString());
                                    ContainerId _cId = ConverterUtils.toContainerId(_ycl.getValue().getContainerid());
                                    ApplicationId _appId = _cId.getApplicationAttemptId().getApplicationId();                        

                                    //Get ProjectId from ApplicationId in ** ApplicationState Table ** 
                                    //ApplicationStateDataAccess _appStatDS = (ApplicationStateDataAccess)RMStorageFactory.getDataAccess(ApplicationStateDataAccess.class);
                                    //ApplicationState _appStat =(ApplicationState)_appStatDS.findByApplicationId(_appId.toString());
                                    //ApplicationState _appStat = hopApplicationState.get(_appId.toString());
                                    ApplicationState _appStat = HopApplicationState.get(_appId.toString());
                                    if (_appStat == null){
                                      _appStat =(ApplicationState)AppStatDS.findByApplicationId(_appId.toString());
                                      if (_appStat == null){
                                          LOG.error("Application not found: " + _appId.toString());
                                      }else{
                                          HopApplicationState.put(_appId.toString(),_appStat);                                                        
                                      }
                                    }

                                    String _projectid = _appStat.getUser().split("__")[0];
                                    String _user = _appStat.getUser().split("__")[1];
                                    LOG.info("RIZ:: App : " + _appId.toString() + " User : " + _appStat.getUser());

                                    // Calculate the charge
                                    long _charge = _ycl.getValue().getStop() -_ycl.getValue().getStart();

                                    // Decide what to do with the charge
                                    if (_charge > 0){
                                        if (_ycl.getValue().getState() == ContainerExitStatus.CONTAINER_RUNNING_STATE){
                                            //>> Edit log entry + Increase Quota
                                            toBeModifiedYarnContainersLogs.add(new YarnContainersLogs(_ycl.getValue().getContainerid(),
                                                                                                      _ycl.getValue().getState(), 
                                                                                                      _ycl.getValue().getStop(), 
                                                                                                      _ycl.getValue().getStop()));
                                            //** YarnProjectsQuota charging**
                                            chargeYarnProjectsQuota(chargedYarnProjectsQuota,hopYarnProjectsQuotaList ,_projectid, _charge);

                                            //** YarnProjectsDailyCost charging**
                                            chargeYarnProjectsDailyCost(chargedYarnProjectsDailyCost,hopYarnProjectsDailyCostList,_projectid, _user, _day, _charge);

                                        }
                                        else if(_ycl.getValue().getState() == ContainerExitStatus.ABORTED ||
                                                _ycl.getValue().getState() == ContainerExitStatus.DISKS_FAILED ||
                                                _ycl.getValue().getState() == ContainerExitStatus.PREEMPTED){
                                            //>> Delete log entry
                                            toBeRemovedYarnContainersLogs.add((YarnContainersLogs)_ycl.getValue());                                
                                        }
                                        else{
                                            //>> Delete log entry + Increase Quota                                   
                                            toBeRemovedYarnContainersLogs.add((YarnContainersLogs)_ycl.getValue());
                                            //** YarnProjectsQuota charging**
                                            chargeYarnProjectsQuota(chargedYarnProjectsQuota,hopYarnProjectsQuotaList ,_projectid, _charge);

                                            //** YarnProjectsDailyCost charging**
                                            chargeYarnProjectsDailyCost(chargedYarnProjectsDailyCost,hopYarnProjectsDailyCostList,_projectid, _user, _day, _charge);

                                        }
                                    }                
                                }

                                // Deleta/Modify the ** YarnContainersLogs **
                                _csDA.removeAll(toBeRemovedYarnContainersLogs);
                                _csDA.addAll(toBeModifiedYarnContainersLogs);

                                // Show all charged project
                                for(YarnProjectsQuota _cpq : chargedYarnProjectsQuota.values()){                        
                                    LOG.debug("RIZ:: Charged projects: " + _cpq.toString() + " charge amount:" + _cpq.getTotalUsedQuota());
                                }
                                
                                
                                // Add all the changed project quotato NDB
                                _pqDA.addAll(chargedYarnProjectsQuota.values());
                                _pdcDA.addAll(chargedYarnProjectsDailyCost.values());
                    
                                connector.commit();
                                return null;
                            }
                            private void chargeYarnProjectsQuota(Map<String, YarnProjectsQuota> chargedYarnProjectsQuota,
                                                      Map<String, YarnProjectsQuota> hopYarnProjectsQuotaList,
                                                      String _projectid, long _charge) {
                        
                                YarnProjectsQuota _tempPq = (YarnProjectsQuota)hopYarnProjectsQuotaList.get(_projectid);
                                if(_tempPq != null){
                                    YarnProjectsQuota _modifiedPq = new YarnProjectsQuota(_projectid,
                                                                                              _tempPq.getRemainingQuota() - (int)_charge ,
                                                                                              _tempPq.getTotalUsedQuota() + (int)_charge);

                                    chargedYarnProjectsQuota.put(_projectid, _modifiedPq);
                                    hopYarnProjectsQuotaList.put(_projectid, _modifiedPq);                  
                                }else{
                                  LOG.error("Project not found: " + _projectid);
                                }

                            }



                            private void chargeYarnProjectsDailyCost(
                                  Map<YarnProjectsDailyId, YarnProjectsDailyCost> chargedYarnProjectsDailyCost,
                                  Map<YarnProjectsDailyId, YarnProjectsDailyCost> hopYarnProjectsDailyCostList,
                                  String _projectid, String _user, long _day, long _charge) {

                                YarnProjectsDailyId _key = new YarnProjectsDailyId(_projectid, _user, _day);                      
                                YarnProjectsDailyCost _tempPdc =(YarnProjectsDailyCost)hopYarnProjectsDailyCostList.get(_key); // "TestProject#rizvi#16794"
                                if (_tempPdc != null){
                                    YarnProjectsDailyCost _incrementedPdc = new  YarnProjectsDailyCost(_projectid, _user, _day, _tempPdc.getCreditsUsed() + (int)_charge);
                                    chargedYarnProjectsDailyCost.put(_key,_incrementedPdc);
                                    hopYarnProjectsDailyCostList.put(_key,_incrementedPdc);
                                }else{
                                    YarnProjectsDailyCost _newPdc = new  YarnProjectsDailyCost(_projectid, _user, _day, (int)_charge);
                                    chargedYarnProjectsDailyCost.put(_key, _newPdc);
                                    hopYarnProjectsDailyCostList.put(_key, _newPdc);
                                }                                
                            }
                        };
                bomb.handle();

                Thread.currentThread().sleep(1000);
            } catch (InterruptedException ex) {
                //Logger.getLogger(QuotaScheduler.class.getName()).log(Level.SEVERE, null, ex);
                LOG.info("Schedular thread is exiting with exception" + ex.getMessage());
            } catch (StorageInitializtionException ex) {
                //LOG.error(ex);
            } catch (StorageException ex) {
                //LOG.error(ex);
            } catch (IOException ex) {
                Logger.getLogger(QuotaScheduler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        LOG.info("Schedular thread is exiting gracefully");
    }
}
