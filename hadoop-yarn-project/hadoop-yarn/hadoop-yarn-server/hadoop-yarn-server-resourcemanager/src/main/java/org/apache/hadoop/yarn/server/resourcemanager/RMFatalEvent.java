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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Event that indicates a non-recoverable error for the resource manager.
 */
public class RMFatalEvent extends AbstractEvent<RMFatalEventType> {
  private final Exception cause;
  private final String message;

  /**
   * Create a new event of the given type with the given cause.
   * @param rmFatalEventType The {@link RMFatalEventType} of the event
   * @param message a text description of the reason for the event
   */
  public RMFatalEvent(RMFatalEventType rmFatalEventType, String message) {
    this(rmFatalEventType, null, message);
  }

  /**
   * Create a new event of the given type around the given source
   * {@link Exception}.
   * @param rmFatalEventType The {@link RMFatalEventType} of the event
   * @param cause the source exception
   */
  public RMFatalEvent(RMFatalEventType rmFatalEventType, Exception cause) {
    this(rmFatalEventType, cause, null);
  }

  /**
   * Create a new event of the given type around the given source
   * {@link Exception} with the given cause.
   * @param rmFatalEventType The {@link RMFatalEventType} of the event
   * @param cause the source exception
   * @param message a text description of the reason for the event
   */
  public RMFatalEvent(RMFatalEventType rmFatalEventType, Exception cause,
      String message) {
    super(rmFatalEventType);
    this.cause = cause;
    this.message = message;
  }

  /**
   * Get a text description of the reason for the event.  If a cause was, that
   * {@link Exception} will be converted to a {@link String} and included in
   * the result.
   * @return a text description of the reason for the event
   */
  public String getExplanation() {
    StringBuilder sb = new StringBuilder();

    if (message != null) {
      sb.append(message);

      if (cause != null) {
        sb.append(": ");
      }
    }

    if (cause != null) {
      sb.append(StringUtils.stringifyException(cause));
    }

    return sb.toString();
  }

  @Override
  public String toString() {
    return String.format("RMFatalEvent of type %s, caused by %s",
        getType().name(), getExplanation());
  }
}
