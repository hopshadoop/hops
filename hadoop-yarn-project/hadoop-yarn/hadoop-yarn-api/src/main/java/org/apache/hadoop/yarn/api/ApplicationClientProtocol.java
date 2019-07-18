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

package org.apache.hadoop.yarn.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YARNFeatureNotEnabledException;

/**
 * <p>The protocol between clients and the <code>ResourceManager</code>
 * to submit/abort jobs and to get information on applications, cluster metrics,
 * nodes, queues and ACLs.</p> 
 */
@Public
@Stable
public interface ApplicationClientProtocol extends ApplicationBaseProtocol {
  /**
   * <p>The interface used by clients to obtain a new {@link ApplicationId} for 
   * submitting new applications.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a new, monotonically
   * increasing, {@link ApplicationId} which is used by the client to submit
   * a new application.</p>
   *
   * <p>The <code>ResourceManager</code> also responds with details such 
   * as maximum resource capabilities in the cluster as specified in
   * {@link GetNewApplicationResponse}.</p>
   *
   * @param request request to get a new <code>ApplicationId</code>
   * @return response containing the new <code>ApplicationId</code> to be used
   * to submit an application
   * @throws YarnException
   * @throws IOException
   * @see #submitApplication(SubmitApplicationRequest)
   */
  @Public
  @Stable
  @Idempotent
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request)
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by clients to submit a new application to the
   * <code>ResourceManager.</code></p>
   * 
   * <p>The client is required to provide details such as queue, 
   * {@link Resource} required to run the <code>ApplicationMaster</code>, 
   * the equivalent of {@link ContainerLaunchContext} for launching
   * the <code>ApplicationMaster</code> etc. via the 
   * {@link SubmitApplicationRequest}.</p>
   * 
   * <p>Currently the <code>ResourceManager</code> sends an immediate (empty) 
   * {@link SubmitApplicationResponse} on accepting the submission and throws 
   * an exception if it rejects the submission. However, this call needs to be
   * followed by {@link #getApplicationReport(GetApplicationReportRequest)}
   * to make sure that the application gets properly submitted - obtaining a
   * {@link SubmitApplicationResponse} from ResourceManager doesn't guarantee
   * that RM 'remembers' this application beyond failover or restart. If RM
   * failover or RM restart happens before ResourceManager saves the
   * application's state successfully, the subsequent
   * {@link #getApplicationReport(GetApplicationReportRequest)} will throw
   * a {@link ApplicationNotFoundException}. The Clients need to re-submit
   * the application with the same {@link ApplicationSubmissionContext} when
   * it encounters the {@link ApplicationNotFoundException} on the
   * {@link #getApplicationReport(GetApplicationReportRequest)} call.</p>
   * 
   * <p>During the submission process, it checks whether the application
   * already exists. If the application exists, it will simply return
   * SubmitApplicationResponse</p>
   *
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to
   * queues etc. before accepting the application submission.</p>
   * 
   * @param request request to submit a new application
   * @return (empty) response on accepting the submission
   * @throws YarnException
   * @throws IOException
   * @see #getNewApplication(GetNewApplicationRequest)
   */
  @Public
  @Stable
  @Idempotent
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) 
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by clients to request the 
   * <code>ResourceManager</code> to fail an application attempt.</p>
   *
   * <p>The client, via {@link FailApplicationAttemptRequest} provides the
   * {@link ApplicationAttemptId} of the attempt to be failed.</p>
   *
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to the
   * application, queue etc. before failing the attempt.</p>
   *
   * <p>Currently, the <code>ResourceManager</code> returns an empty response
   * on success and throws an exception on rejecting the request.</p>
   *
   * @param request request to fail an attempt
   * @return <code>ResourceManager</code> returns an empty response
   *         on success and throws an exception on rejecting the request
   * @throws YarnException
   * @throws IOException
   * @see #getQueueUserAcls(GetQueueUserAclsInfoRequest)
   */
  @Public
  @Unstable
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request)
  throws YarnException, IOException;

  /**
   * <p>The interface used by clients to request the
   * <code>ResourceManager</code> to abort submitted application.</p>
   * 
   * <p>The client, via {@link KillApplicationRequest} provides the
   * {@link ApplicationId} of the application to be aborted.</p>
   * 
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to the
   * application, queue etc. before terminating the application.</p> 
   * 
   * <p>Currently, the <code>ResourceManager</code> returns an empty response
   * on success and throws an exception on rejecting the request.</p>
   * 
   * @param request request to abort a submitted application
   * @return <code>ResourceManager</code> returns an empty response
   *         on success and throws an exception on rejecting the request
   * @throws YarnException
   * @throws IOException
   * @see #getQueueUserAcls(GetQueueUserAclsInfoRequest) 
   */
  @Public
  @Stable
  @Idempotent
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) 
  throws YarnException, IOException;

  /**
   * <p>The interface used by clients to get metrics about the cluster from
   * the <code>ResourceManager</code>.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a
   * {@link GetClusterMetricsResponse} which includes the 
   * {@link YarnClusterMetrics} with details such as number of current
   * nodes in the cluster.</p>
   * 
   * @param request request for cluster metrics
   * @return cluster metrics
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) 
  throws YarnException, IOException;

  /**
   * <p>The interface used by clients to get a report of all nodes
   * in the cluster from the <code>ResourceManager</code>.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with a 
   * {@link GetClusterNodesResponse} which includes the 
   * {@link NodeReport} for all the nodes in the cluster.</p>
   * 
   * @param request request for report on all nodes
   * @return report on all nodes
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetClusterNodesResponse getClusterNodes(
      GetClusterNodesRequest request) 
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by clients to get information about <em>queues</em>
   * from the <code>ResourceManager</code>.</p>
   * 
   * <p>The client, via {@link GetQueueInfoRequest}, can ask for details such
   * as used/total resources, child queues, running applications etc.</p>
   *
   * <p> In secure mode,the <code>ResourceManager</code> verifies access before
   * providing the information.</p> 
   * 
   * @param request request to get queue information
   * @return queue information
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
  @Idempotent
  public GetQueueInfoResponse getQueueInfo(
      GetQueueInfoRequest request) 
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by clients to get information about <em>queue 
   * acls</em> for <em>current user</em> from the <code>ResourceManager</code>.
   * </p>
   * 
   * <p>The <code>ResourceManager</code> responds with queue acls for all
   * existing queues.</p>
   * 
   * @param request request to get queue acls for <em>current user</em>
   * @return queue acls for <em>current user</em>
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Stable
 @Idempotent
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) 
  throws YarnException, IOException;

  /**
   * Move an application to a new queue.
   * 
   * @param request the application ID and the target queue
   * @return an empty response
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  @Idempotent
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException, IOException;

  /**
   * <p>The interface used by clients to obtain a new {@link ReservationId} for
   * submitting new reservations.</p>
   *
   * <p>The <code>ResourceManager</code> responds with a new, unique,
   * {@link ReservationId} which is used by the client to submit
   * a new reservation.</p>
   *
   * @param request to get a new <code>ReservationId</code>
   * @return response containing the new <code>ReservationId</code> to be used
   * to submit a new reservation
   * @throws YarnException if the reservation system is not enabled.
   * @throws IOException on IO failures.
   * @see #submitReservation(ReservationSubmissionRequest)
   */
  @Public
  @Unstable
  @Idempotent
  GetNewReservationResponse getNewReservation(
          GetNewReservationRequest request)
          throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to submit a new reservation to the
   * {@code ResourceManager}.
   * </p>
   * 
   * <p>
   * The client packages all details of its request in a
   * {@link ReservationSubmissionRequest} object. This contains information
   * about the amount of capacity, temporal constraints, and concurrency needs.
   * Furthermore, the reservation might be composed of multiple stages, with
   * ordering dependencies among them.
   * </p>
   * 
   * <p>
   * In order to respond, a new admission control component in the
   * {@code ResourceManager} performs an analysis of the resources that have
   * been committed over the period of time the user is requesting, verify that
   * the user requests can be fulfilled, and that it respect a sharing policy
   * (e.g., {@code CapacityOverTimePolicy}). Once it has positively determined
   * that the ReservationSubmissionRequest is satisfiable the
   * {@code ResourceManager} answers with a
   * {@link ReservationSubmissionResponse} that include a non-null
   * {@link ReservationId}. Upon failure to find a valid allocation the response
   * is an exception with the reason.
   * 
   * On application submission the client can use this {@link ReservationId} to
   * obtain access to the reserved resources.
   * </p>
   * 
   * <p>
   * The system guarantees that during the time-range specified by the user, the
   * reservationID will be corresponding to a valid reservation. The amount of
   * capacity dedicated to such queue can vary overtime, depending of the
   * allocation that has been determined. But it is guaranteed to satisfy all
   * the constraint expressed by the user in the
   * {@link ReservationSubmissionRequest}.
   * </p>
   * 
   * @param request the request to submit a new Reservation
   * @return response the {@link ReservationId} on accepting the submission
   * @throws YarnException if the request is invalid or reservation cannot be
   *           created successfully
   * @throws IOException
   * 
   */
  @Public
  @Unstable
  @Idempotent
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to update an existing Reservation. This is
   * referred to as a re-negotiation process, in which a user that has
   * previously submitted a Reservation.
   * </p>
   * 
   * <p>
   * The allocation is attempted by virtually substituting all previous
   * allocations related to this Reservation with new ones, that satisfy the new
   * {@link ReservationUpdateRequest}. Upon success the previous allocation is
   * substituted by the new one, and on failure (i.e., if the system cannot find
   * a valid allocation for the updated request), the previous allocation
   * remains valid.
   * 
   * The {@link ReservationId} is not changed, and applications currently
   * running within this reservation will automatically receive the resources
   * based on the new allocation.
   * </p>
   * 
   * @param request to update an existing Reservation (the ReservationRequest
   *          should refer to an existing valid {@link ReservationId})
   * @return response empty on successfully updating the existing reservation
   * @throws YarnException if the request is invalid or reservation cannot be
   *           updated successfully
   * @throws IOException
   * 
   */
  @Public
  @Unstable
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to remove an existing Reservation.
   * 
   * Upon deletion of a reservation applications running with this reservation,
   * are automatically downgraded to normal jobs running without any dedicated
   * reservation.
   * </p>
   * 
   * @param request to remove an existing Reservation (the ReservationRequest
   *          should refer to an existing valid {@link ReservationId})
   * @return response empty on successfully deleting the existing reservation
   * @throws YarnException if the request is invalid or reservation cannot be
   *           deleted successfully
   * @throws IOException
   *
   */
  @Public
  @Unstable
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to get the list of reservations in a plan.
   * The reservationId will be used to search for reservations to list if it is
   * provided. Otherwise, it will select active reservations within the
   * startTime and endTime (inclusive).
   * </p>
   *
   * @param request to list reservations in a plan. Contains fields to select
   *                String queue, ReservationId reservationId, long startTime,
   *                long endTime, and a bool includeReservationAllocations.
   *
   *                queue: Required. Cannot be null or empty. Refers to the
   *                reservable queue in the scheduler that was selected when
   *                creating a reservation submission
   *                {@link ReservationSubmissionRequest}.
   *
   *                reservationId: Optional. If provided, other fields will
   *                be ignored.
   *
   *                startTime: Optional. If provided, only reservations that
   *                end after the startTime will be selected. This defaults
   *                to 0 if an invalid number is used.
   *
   *                endTime: Optional. If provided, only reservations that
   *                start on or before endTime will be selected. This defaults
   *                to Long.MAX_VALUE if an invalid number is used.
   *
   *                includeReservationAllocations: Optional. Flag that
   *                determines whether the entire reservation allocations are
   *                to be returned. Reservation allocations are subject to
   *                change in the event of re-planning as described by
   *                {@code ReservationDefinition}.
   *
   * @return response that contains information about reservations that are
   *                being searched for.
   * @throws YarnException if the request is invalid
   * @throws IOException on IO failures
   *
   */
  @Public
  @Unstable
  ReservationListResponse listReservations(
            ReservationListRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by client to get node to labels mappings in existing cluster
   * </p>
   *
   * @param request
   * @return node to labels mappings
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by client to get labels to nodes mappings
   * in existing cluster
   * </p>
   *
   * @param request
   * @return labels to nodes mappings
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by client to get node labels in the cluster
   * </p>
   *
   * @param request to get node labels collection of this cluster
   * @return node labels collection of this cluster
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by client to set priority of an application.
   * </p>
   * @param request to set priority of an application
   * @return an empty response
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  @Idempotent
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request) throws YarnException,
      IOException;

  /**
   * <p>The interface used by clients to request the
   * <code>ResourceManager</code> to signal a container. For example,
   * the client can send command OUTPUT_THREAD_DUMP to dump threads of the
   * container.</p>
   *
   * <p>The client, via {@link SignalContainerRequest} provides the
   * id of the container and the signal command. </p>
   *
   * <p> In secure mode,the <code>ResourceManager</code> verifies access to the
   * application before signaling the container.
   * The user needs to have <code>MODIFY_APP</code> permission.</p>
   *
   * <p>Currently, the <code>ResourceManager</code> returns an empty response
   * on success and throws an exception on rejecting the request.</p>
   *
   * @param request request to signal a container
   * @return <code>ResourceManager</code> returns an empty response
   *         on success and throws an exception on rejecting the request
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException,
      IOException;

  /**
   * <p>
   * The interface used by client to set ApplicationTimeouts of an application.
   * The UpdateApplicationTimeoutsRequest should have timeout value with
   * absolute time with ISO8601 format <b>yyyy-MM-dd'T'HH:mm:ss.SSSZ</b>.
   * </p>
   * <b>Note:</b> If application timeout value is less than or equal to current
   * time then update application throws YarnException.
   * @param request to set ApplicationTimeouts of an application
   * @return a response with updated timeouts.
   * @throws YarnException if update request has empty values or application is
   *           in completing states.
   * @throws IOException on IO failures
   */
  @Public
  @Unstable
  @Idempotent
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to get all the resource profiles that are
   * available on the ResourceManager.
   * </p>
   * @param request request to get all the resource profiles
   * @return Response containing a map of the profile name to Resource
   *         capabilities
   * @throws YARNFeatureNotEnabledException if resource-profile is disabled
   * @throws YarnException if any error happens inside YARN
   * @throws IOException in case of other errors
   */
  @Public
  @Unstable
  GetAllResourceProfilesResponse getResourceProfiles(
      GetAllResourceProfilesRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface to get the details for a specific resource profile.
   * </p>
   * @param request request to get the details of a resource profile
   * @return Response containing the details for a particular resource profile
   * @throws YARNFeatureNotEnabledException if resource-profile is disabled
   * @throws YarnException if any error happens inside YARN
   * @throws IOException in case of other errors
   */
  @Public
  @Unstable
  GetResourceProfileResponse getResourceProfile(
      GetResourceProfileRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface to get the details for a specific resource profile.
   * </p>
   * @param request request to get the details of a resource profile
   * @return Response containing the details for a particular resource profile
   * @throws YarnException if any error happens inside YARN
   * @throws IOException in case of other errors
   */
  @Public
  @Unstable
  GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by client to get attributes to nodes mappings
   * available in ResourceManager.
   * </p>
   *
   * @param request request to get details of attributes to nodes mapping.
   * @return Response containing the details of attributes to nodes mappings.
   * @throws YarnException if any error happens inside YARN
   * @throws IOException   incase of other errors
   */
  @Public
  @Unstable
  GetAttributesToNodesResponse getAttributesToNodes(
      GetAttributesToNodesRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by client to get node attributes available in
   * ResourceManager.
   * </p>
   *
   * @param request request to get node attributes collection of this cluster.
   * @return Response containing node attributes collection.
   * @throws YarnException if any error happens inside YARN.
   * @throws IOException   incase of other errors.
   */
  @Public
  @Unstable
  GetClusterNodeAttributesResponse getClusterNodeAttributes(
      GetClusterNodeAttributesRequest request)
      throws YarnException, IOException;

  /**
   * <p>
   * The interface used by client to get node to attributes mappings.
   * in existing cluster.
   * </p>
   *
   * @param request request to get nodes to attributes mapping.
   * @return nodes to attributes mappings.
   * @throws YarnException if any error happens inside YARN.
   * @throws IOException
   */
  @Public
  @Unstable
  GetNodesToAttributesResponse getNodesToAttributes(
      GetNodesToAttributesRequest request) throws YarnException, IOException;
}
