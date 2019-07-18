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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@link ReservationDefinition} captures the set of resource and time
 * constraints the user cares about regarding a reservation.
 * 
 * @see ResourceRequest
 * 
 */
@Public
@Unstable
public abstract class ReservationDefinition {

  @Public
  @Unstable
  public static ReservationDefinition newInstance(long arrival, long deadline,
      ReservationRequests reservationRequests, String name,
      String recurrenceExpression, Priority priority) {
    ReservationDefinition rDefinition =
        Records.newRecord(ReservationDefinition.class);
    rDefinition.setArrival(arrival);
    rDefinition.setDeadline(deadline);
    rDefinition.setReservationRequests(reservationRequests);
    rDefinition.setReservationName(name);
    rDefinition.setRecurrenceExpression(recurrenceExpression);
    rDefinition.setPriority(priority);
    return rDefinition;
  }

  @Public
  @Unstable
  public static ReservationDefinition newInstance(long arrival, long deadline,
      ReservationRequests reservationRequests, String name) {
    ReservationDefinition rDefinition = newInstance(arrival, deadline,
        reservationRequests, name, "0", Priority.UNDEFINED);
    return rDefinition;
  }

  /**
   * Get the arrival time or the earliest time from which the resource(s) can be
   * allocated. Time expressed as UTC.
   * 
   * @return the earliest valid time for this reservation
   */
  @Public
  @Unstable
  public abstract long getArrival();

  /**
   * Set the arrival time or the earliest time from which the resource(s) can be
   * allocated. Time expressed as UTC.
   * 
   * @param earliestStartTime the earliest valid time for this reservation
   */
  @Public
  @Unstable
  public abstract void setArrival(long earliestStartTime);

  /**
   * Get the deadline or the latest time by when the resource(s) must be
   * allocated. Time expressed as UTC.
   * 
   * @return the deadline or the latest time by when the resource(s) must be
   *         allocated
   */
  @Public
  @Unstable
  public abstract long getDeadline();

  /**
   * Set the deadline or the latest time by when the resource(s) must be
   * allocated. Time expressed as UTC.
   * 
   * @param latestEndTime the deadline or the latest time by when the
   *          resource(s) should be allocated
   */
  @Public
  @Unstable
  public abstract void setDeadline(long latestEndTime);

  /**
   * Get the list of {@link ReservationRequests} representing the resources
   * required by the application
   * 
   * @return the list of {@link ReservationRequests}
   */
  @Public
  @Unstable
  public abstract ReservationRequests getReservationRequests();

  /**
   * Set the list of {@link ReservationRequests} representing the resources
   * required by the application
   * 
   * @param reservationRequests the list of {@link ReservationRequests}
   */
  @Public
  @Unstable
  public abstract void setReservationRequests(
      ReservationRequests reservationRequests);

  /**
   * Get the name for this reservation. The name need-not be unique, and it is
   * just a mnemonic for the user (akin to job names). Accepted reservations are
   * uniquely identified by a system-generated ReservationId.
   * 
   * @return string representing the name of the corresponding reserved resource
   *         allocation in the scheduler
   */
  @Public
  @Unstable
  public abstract String getReservationName();

  /**
   * Set the name for this reservation. The name need-not be unique, and it is
   * just a mnemonic for the user (akin to job names). Accepted reservations are
   * uniquely identified by a system-generated ReservationId.
   * 
   * @param name representing the name of the corresponding reserved resource
   *          allocation in the scheduler
   */
  @Public
  @Unstable
  public abstract void setReservationName(String name);

  /**
   * Get the recurrence of this reservation representing the time period of
   * the periodic job. Currently, only long values are supported. Later,
   * support for regular expressions denoting arbitrary recurrence patterns
   * (e.g., every Tuesday and Thursday) will be added.
   * Recurrence is represented in milliseconds for periodic jobs.
   * Recurrence is 0 for non-periodic jobs. Periodic jobs are valid until they
   * are explicitly cancelled and have higher priority than non-periodic jobs
   * (during initial placement and replanning). Periodic job allocations are
   * consistent across runs (flexibility in allocation is leveraged only during
   * initial placement, allocations remain consistent thereafter). Note that
   * as a long, the recurrence expression must be greater than the duration of
   * the reservation (deadline - arrival). Also note that the configured max
   * period must be divisible by the recurrence expression if expressed as a
   * long.
   *
   * @return recurrence of this reservation
   */
  @Public
  @Unstable
  public abstract String getRecurrenceExpression();

  /**
   * Set the recurrence of this reservation representing the time period of
   * the periodic job. Currently, only long values are supported. Later,
   * support for regular expressions denoting arbitrary recurrence patterns
   * (e.g., every Tuesday and Thursday) will be added.
   * Recurrence is represented in milliseconds for periodic jobs.
   * Recurrence is 0 for non-periodic jobs. Periodic jobs are valid until they
   * are explicitly cancelled and have higher priority than non-periodic jobs
   * (during initial placement and replanning). Periodic job allocations are
   * consistent across runs (flexibility in allocation is leveraged only during
   * initial placement, allocations remain consistent thereafter). Note that
   * as a long, the recurrence expression must be greater than the duration of
   * the reservation (deadline - arrival). Also note that the configured max
   * period must be divisible by the recurrence expression if expressed as a
   * long.
   *
   * @param recurrenceExpression recurrence interval of this reservation
   */
  @Public
  @Unstable
  public abstract void setRecurrenceExpression(String recurrenceExpression);

  /**
   * Get the priority for this reservation. A lower number for priority
   * indicates a higher priority reservation. Recurring reservations are
   * always higher priority than non-recurring reservations. Priority for
   * non-recurring reservations are only compared with non-recurring
   * reservations. Likewise for recurring reservations.
   *
   * @return int representing the priority of the reserved resource
   *         allocation in the scheduler
   */
  @Public
  @Unstable
  public abstract Priority getPriority();

  /**
   * Set the priority for this reservation. A lower number for priority
   * indicates a higher priority reservation. Recurring reservations are
   * always higher priority than non-recurring reservations. Priority for
   * non-recurring reservations are only compared with non-recurring
   * reservations. Likewise for recurring reservations.
   *
   * @param priority representing the priority of the reserved resource
   *          allocation in the scheduler
   */
  @Public
  @Unstable
  public abstract void setPriority(Priority priority);

}
