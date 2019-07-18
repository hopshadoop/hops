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

import Ember from 'ember';

export default Ember.Controller.extend({
  appId: '',
  serviceName: undefined,

  breadcrumbs: [{
    text: "Home",
    routeName: 'application'
  }, {
    text: "Applications",
    routeName: 'yarn-apps.apps'
  }, {
    text: 'App'
  }],

  actions: {
    showStopServiceConfirm() {
      this.set('actionResponse', null);
      Ember.$("#stopServiceConfirmDialog").modal('show');
    },

    stopService() {
      var self = this;
      Ember.$("#stopServiceConfirmDialog").modal('hide');
      var adapter = this.store.adapterFor('yarn-servicedef');
      self.set('isLoading', true);
      adapter.stopService(this.model.serviceName, this.get('model.app.user')).then(function () {
        self.set('actionResponse', { msg: 'Service stopped successfully. Auto refreshing in 5 seconds.', type: 'success' });
        Ember.run.later(self, function () {
          this.set('actionResponse', null);
          this.send("refresh");
        }, 5000);
      }, function (errr) {
        let messg = 'Error: Stop service failed!';
        if (errr.errors && errr.errors[0] && errr.errors[0].diagnostics) {
          messg = 'Error: ' + errr.errors[0].diagnostics;
        }
        self.set('actionResponse', { msg: messg, type: 'error' });
      }).finally(function () {
        self.set('isLoading', false);
      });
    },

    showDeleteServiceConfirm() {
      this.set('actionResponse', null);
      Ember.$("#deleteServiceConfirmDialog").modal('show');
    },

    deleteService() {
      var self = this;
      Ember.$("#deleteServiceConfirmDialog").modal('hide');
      var adapter = this.store.adapterFor('yarn-servicedef');
      self.set('isLoading', true);
      adapter.deleteService(this.model.serviceName, this.get('model.app.user')).then(function () {
        self.set('actionResponse', { msg: 'Service deleted successfully. Redirecting to services in 5 seconds.', type: 'success' });
        Ember.run.later(self, function () {
          this.set('actionResponse', null);
          this.transitionToRoute("yarn-services");
        }, 5000);
      }, function (errr) {
        let messg = 'Error: Delete service failed!';
        if (errr.errors && errr.errors[0] && errr.errors[0].diagnostics) {
          messg = 'Error: ' + errr.errors[0].diagnostics;
        }
        self.set('actionResponse', { msg: messg, type: 'error' });
      }).finally(function () {
        self.set('isLoading', false);
      });
    },

    showKillApplicationConfirm() {
      this.set('actionResponse', null);
      Ember.$("#killApplicationConfirmDialog").modal('show');
    },

    killApplication() {
      var self = this;
      Ember.$("#killApplicationConfirmDialog").modal('hide');
      const adapter = this.store.adapterFor('yarn-app');
      self.set('isLoading', true);
      adapter.sendKillApplication(this.model.app.id).then(function () {
        self.set('actionResponse', {
          msg: 'Application killed successfully. Auto refreshing in 5 seconds.',
          type: 'success'
        });
        Ember.run.later(self, function () {
          this.set('actionResponse', null);
          this.send("refresh");
        }, 5000);
      }, function (err) {
        let message = err.diagnostics || 'Error: Kill application failed!';
        self.set('actionResponse', { msg: message, type: 'error' });
      }).finally(function () {
        self.set('isLoading', false);
      });
    },

    resetActionResponse() {
      this.set('actionResponse', null);
    }
  },

  isRunningService: Ember.computed('model.serviceName', 'model.app.state', function () {
    return this.model.serviceName && this.model.app.get('state') === 'RUNNING';
  }),


  updateBreadcrumbs(appId, serviceName, tailCrumbs) {
    var breadcrumbs = [{
      text: "Home",
      routeName: 'application'
    }];
    if (appId && serviceName) {
      breadcrumbs.push({
        text: "Services",
        routeName: 'yarn-services'
      }, {
        text: `${serviceName} [${appId}]`,
        href: `#/yarn-app/${appId}/components?service=${serviceName}`
      });
    } else {
      breadcrumbs.push({
        text: "Applications",
        routeName: 'yarn-apps.apps'
      }, {
        text: `App [${appId}]`,
        href: `#/yarn-app/${appId}/attempts`
      });
    }
    if (tailCrumbs) {
      breadcrumbs.pushObjects(tailCrumbs);
    }
    this.set('breadcrumbs', breadcrumbs);
  },

  amHostHttpAddressFormatted: Ember.computed('model.app.amHostHttpAddress', function () {
    var amHostAddress = this.get('model.app.amHostHttpAddress');
    if (amHostAddress && amHostAddress.indexOf('://') < 0) {
      amHostAddress = 'http://' + amHostAddress;
    }
    return amHostAddress;
  }),

  isAppKillable: Ember.computed("model.app.state", function () {
    if (this.get("model.app.applicationType") === 'yarn-service') {
      return false;
    }
    const killableStates = ['NEW', 'NEW_SAVING', 'SUBMITTED', 'ACCEPTED', 'RUNNING'];
    return killableStates.indexOf(this.get("model.app.state")) > -1;
  }),

  isServiceDeployedOrRunning: Ember.computed('model.serviceInfo', function() {
    const serviceInfo = this.get('model.serviceInfo');
    const stoppedStates = ['STOPPED', 'SUCCEEDED', 'FAILED'];
    if (serviceInfo) {
      return stoppedStates.indexOf(serviceInfo.get('state')) === -1;
    }
    return false;
  }),

  isServiceStoppped: Ember.computed('model.serviceInfo', function() {
    const serviceInfo = this.get('model.serviceInfo');
    const stoppedStates = ['STOPPED', 'SUCCEEDED'];
    if (serviceInfo) {
      return stoppedStates.indexOf(serviceInfo.get('state')) > -1;
    }
    return false;
  })
});
