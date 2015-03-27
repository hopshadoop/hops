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
package io.hops.transaction.handler;

public enum EncodingStatusOperationType
    implements RequestHandler.OperationType {
  ADD,
  DELETE,
  UPDATE,
  FIND_BY_INODE_ID,
  FIND_ACTIVE_ENCODINGS,
  FIND_REQUESTED_ENCODINGS,
  FIND_ENCODED,
  FIND_ACTIVE_REPAIRS,
  COUNT_REQUESTED_ENCODINGS,
  COUNT_ACTIVE_ENCODINGS,
  COUNT_ENCODED,
  COUNT_ACTIVE_REPAIRS,
  FIND_REQUESTED_REPAIRS,
  FIND_POTENTIALLY_FIXED,
  FIND_REQUESTED_PARITY_REPAIRS,
  FIND_POTENTIALLY_FIXED_PARITIES,
  FIND_DELETED,
  FIND_REVOKED
}
