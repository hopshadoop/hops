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

import { moduleForModel, test } from 'ember-qunit';
import Ember from 'ember';

moduleForModel('yarn-node-app', 'Unit | Model | NodeApp', {
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(model);
  assert.ok(model._notifyProperties);
  assert.ok(model.didLoad);
  assert.ok(model.appId);
  assert.ok(model.state);
  assert.ok(model.user);
  assert.ok(model.containers);
});

test('test fields', function(assert) {
  let model = this.subject();

  assert.expect(5);
  Ember.run(function () {
    model.set("id", "application_1456251210105_0002");
    model.set("state", "RUNNING");
    assert.equal(model.get("appStateStyle"), "label label-primary");
    assert.equal(model.get("isDummyApp"), false);
    model.set("id", "dummy");
    assert.equal(model.get("isDummyApp"), true);
    model.set("state", "FINISHED");
    assert.equal(model.get("appStateStyle"), "label label-success");
    model.set("state", "NEW");
    assert.equal(model.get("appStateStyle"), "label label-default");
  });
});
