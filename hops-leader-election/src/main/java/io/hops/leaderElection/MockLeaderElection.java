package io.hops.leaderElection;

import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.services.LeaderElectionService;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

public class MockLeaderElection implements LeaderElectionService {
  private static class MockActiveNode implements ActiveNode {
    String ipAddr;
    int port;
    int id;

    @Override
    public String getHostname() {
      return ipAddr;
    }

    @Override
    public long getId() {
      return id;
    }

    @Override
    public String getIpAddress() {
      return ipAddr;
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public InetSocketAddress getInetSocketAddress() {
      return null;
    }

    @Override
    public String getHttpAddress() {
      return String.format("%s:%d", getHostname(), getPort());
    }


    @Override
    public int compareTo(ActiveNode o) {
      return getHttpAddress().compareTo(o.getHttpAddress());
    }
  }


  private static LinkedList<MockActiveNode> nodes = new LinkedList<>();

  private final MockActiveNode mockNode;
  private boolean running = false;

  public MockLeaderElection(final String hostName, final int port) {
    mockNode = new MockActiveNode();
    mockNode.ipAddr = hostName;
    mockNode.port = port;
  }

  @Override
  public void start() {
    synchronized (MockLeaderElection.class) {
      addNodeToLE(mockNode);
      running = true;
    }
  }

  private void addNodeToLE(MockActiveNode n) {
    synchronized (MockLeaderElection.class) {
      nodes.addLast(n);

      n.id = this.nodes.size() == 0 ? 0 : this.nodes.getLast().id + 1;
    }
  }

  private int getPos() {
    synchronized (MockLeaderElection.class) {
      return nodes.indexOf(mockNode);
    }
  }

  @Override
  public void stop() {
    synchronized (MockLeaderElection.class) {
      nodes.remove(mockNode);
      running = false;
    }
  }

  @Override
  public void waitStarted() throws InterruptedException {
    return;
  }

  @Override
  public boolean isLeader() {
    return getPos() == 0;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isSecond() {
    return getPos() == 1;
  }

  @Override
  public long getCurrentID() {
    return getPos();
  }

  @Override
  public void relinquishCurrentIdInNextRound() throws InterruptedException {
    synchronized (MockLeaderElection.class) {
      nodes.remove(mockNode);
      addNodeToLE(mockNode);
    }
  }

  @Override
  public SortedActiveNodeList getActiveNamenodes() {
    final List<ActiveNode> l = new LinkedList<>();
    synchronized (MockLeaderElection.class) {
      l.addAll(nodes);
    }
    return new SortedActiveNodeList() {
      @Override
      public boolean isEmpty() {
        return l.isEmpty();
      }

      @Override
      public int size() {
        return l.size();
      }

      @Override
      public List<ActiveNode> getActiveNodes() {
        return l;
      }

      @Override
      public List<ActiveNode> getSortedActiveNodes() {
        return l;
      }

      @Override
      public ActiveNode getActiveNode(InetSocketAddress address) {
        address.getPort();
        for(ActiveNode n: l){
          if(n.getPort() == address.getPort()) {
            return n;
          }
        }
        return null;
      }

      @Override
      public ActiveNode getLeader() {
        return l.get(0);
      }
    };
  }
}
