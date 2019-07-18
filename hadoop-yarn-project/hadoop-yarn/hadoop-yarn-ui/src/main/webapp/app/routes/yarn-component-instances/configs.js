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
import AbstractRoute from '../abstract';

export default AbstractRoute.extend({
  model(params, transition) {
    var componentName = params.component_name;
    transition.send('updateBreadcrumbs', params.appid, params.service, componentName, [{text: 'Configurations'}]);
    return Ember.RSVP.hash({
      appId: params.appid,
      serviceName: params.service,
      componentName: componentName,
      configs: this.store.query('yarn-service-component', {appId: params.appid}).then(function(components) {
        if (components && components.findBy('name', componentName)) {
          return components.findBy('name', componentName).get('configs');
        }
        return [];
      }, function() {
        return [];
      })
    });
  },

  unloadAll() {
    this.store.unloadAll('yarn-service-component');
  }
});
