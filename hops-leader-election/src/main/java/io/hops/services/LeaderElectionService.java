package io.hops.services;

import io.hops.leaderElection.NDBLeaderElection;
import io.hops.leader_election.node.SortedActiveNodeList;

/**
 * This interface defines a leader election service.
 * In a (possibly distributed) set of nodes, this call should
 * only return true on at most one node at any given time.
 */
public interface LeaderElectionService extends Service {

  /**
   * @return whether the current node is leader
   */
  boolean isLeader();

  /**
   * Detects whether the node is next in line for leader election.
   * So far it is only used in YARN to decide where to place services.
   * As such it does not matter whether the node returned is "second in line"
   * so long as this method selects a stable node different from the master consistently.
   * If the master changes, this method may return true on a new node as a result.
   *
   * @return whether the node is "second in line" for election.
   */
  boolean isSecond();

  /**
   * Gets the ID of the current node in the leader election.
   * @return the ID
   */
  long getCurrentID();

  /**
   * Relinquishes the current id in the next round of elections.
   * Blocks until it is done.
   */
  void relinquishCurrentIdInNextRound() throws InterruptedException;


  /**
   * Returns a {@link SortedActiveNodeList} of namenodes.
   * The nodes in this list are active.
   */
  SortedActiveNodeList getActiveNamenodes();
}
