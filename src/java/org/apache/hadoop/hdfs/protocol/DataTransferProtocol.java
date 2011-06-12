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
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PacketHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.util.ByteBufferOutputStream;
import org.apache.hadoop.security.token.Token;

import static org.apache.hadoop.hdfs.protocol.DataTransferProtoUtil.fromProto;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtoUtil.toProto;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.fromProto;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.fromProtos;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.toProto;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.toProtos;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.vintPrefixed;

import com.google.protobuf.Message;

/**
 * Transfer data to/from datanode using a streaming protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DataTransferProtocol {
  public static final Log LOG = LogFactory.getLog(DataTransferProtocol.class);
  
  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious. 
   */
  /*
   * Version 26:
   *    Use protobuf.
   */
  public static final int DATA_TRANSFER_VERSION = 26;

  /** Operation */
  public enum Op {
    WRITE_BLOCK((byte)80),
    READ_BLOCK((byte)81),
    READ_METADATA((byte)82),
    REPLACE_BLOCK((byte)83),
    COPY_BLOCK((byte)84),
    BLOCK_CHECKSUM((byte)85),
    TRANSFER_BLOCK((byte)86);

    /** The code for this operation. */
    public final byte code;
    
    private Op(byte code) {
      this.code = code;
    }
    
    private static final int FIRST_CODE = values()[0].code;
    /** Return the object represented by the code. */
    private static Op valueOf(byte code) {
      final int i = (code & 0xff) - FIRST_CODE;
      return i < 0 || i >= values().length? null: values()[i];
    }

    /** Read from in */
    public static Op read(DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.write(code);
    }
  }
    
  public enum BlockConstructionStage {
    /** The enumerates are always listed as regular stage followed by the
     * recovery stage. 
     * Changing this order will make getRecoveryStage not working.
     */
    // pipeline set up for block append
    PIPELINE_SETUP_APPEND,
    // pipeline set up for failed PIPELINE_SETUP_APPEND recovery
    PIPELINE_SETUP_APPEND_RECOVERY,
    // data streaming
    DATA_STREAMING,
    // pipeline setup for failed data streaming recovery
    PIPELINE_SETUP_STREAMING_RECOVERY,
    // close the block and pipeline
    PIPELINE_CLOSE,
    // Recover a failed PIPELINE_CLOSE
    PIPELINE_CLOSE_RECOVERY,
    // pipeline set up for block creation
    PIPELINE_SETUP_CREATE,
    // transfer RBW for adding datanodes
    TRANSFER_RBW,
    // transfer Finalized for adding datanodes
    TRANSFER_FINALIZED;
    
    final static private byte RECOVERY_BIT = (byte)1;
    
    /**
     * get the recovery stage of this stage
     */
    public BlockConstructionStage getRecoveryStage() {
      if (this == PIPELINE_SETUP_CREATE) {
        throw new IllegalArgumentException( "Unexpected blockStage " + this);
      } else {
        return values()[ordinal()|RECOVERY_BIT];
      }
    }
  }    

  
  /** Sender */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class Sender {
    /** Initialize a operation. */
    private static void op(final DataOutput out, final Op op
        ) throws IOException {
      out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
      op.write(out);
    }

    private static void send(final DataOutputStream out, final Op opcode,
        final Message proto) throws IOException {
      op(out, opcode);
      proto.writeDelimitedTo(out);
      out.flush();
    }

    /** Send OP_READ_BLOCK */
    public static void opReadBlock(DataOutputStream out, ExtendedBlock blk,
        long blockOffset, long blockLen, String clientName,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException {

      OpReadBlockProto proto = OpReadBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildClientHeader(blk, clientName, blockToken))
        .setOffset(blockOffset)
        .setLen(blockLen)
        .build();

      send(out, Op.READ_BLOCK, proto);
    }
    

    /** Send OP_WRITE_BLOCK */
    public static void opWriteBlock(DataOutputStream out, ExtendedBlock blk,
        int pipelineSize, BlockConstructionStage stage, long newGs,
        long minBytesRcvd, long maxBytesRcvd, String client, DatanodeInfo src,
        DatanodeInfo[] targets, Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      ClientOperationHeaderProto header = DataTransferProtoUtil.buildClientHeader(blk, client,
          blockToken);
      
      OpWriteBlockProto.Builder proto = OpWriteBlockProto.newBuilder()
        .setHeader(header)
        .addAllTargets(
            toProtos(targets, 1))
        .setStage(toProto(stage))
        .setPipelineSize(pipelineSize)
        .setMinBytesRcvd(minBytesRcvd)
        .setMaxBytesRcvd(maxBytesRcvd)
        .setLatestGenerationStamp(newGs);
      
      if (src != null) {
        proto.setSource(toProto(src));
      }

      send(out, Op.WRITE_BLOCK, proto.build());
    }

    /** Send {@link Op#TRANSFER_BLOCK} */
    public static void opTransferBlock(DataOutputStream out, ExtendedBlock blk,
        String client, DatanodeInfo[] targets,
        Token<BlockTokenIdentifier> blockToken) throws IOException {
      
      OpTransferBlockProto proto = OpTransferBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildClientHeader(
            blk, client, blockToken))
        .addAllTargets(toProtos(targets, 0))
        .build();

      send(out, Op.TRANSFER_BLOCK, proto);
    }

    /** Send OP_REPLACE_BLOCK */
    public static void opReplaceBlock(DataOutputStream out,
        ExtendedBlock blk, String delHint, DatanodeInfo src,
        Token<BlockTokenIdentifier> blockToken) throws IOException {
      OpReplaceBlockProto proto = OpReplaceBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .setDelHint(delHint)
        .setSource(toProto(src))
        .build();
      
      send(out, Op.REPLACE_BLOCK, proto);
    }

    /** Send OP_COPY_BLOCK */
    public static void opCopyBlock(DataOutputStream out, ExtendedBlock blk,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      OpCopyBlockProto proto = OpCopyBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .build();
      
      send(out, Op.COPY_BLOCK, proto);
    }

    /** Send OP_BLOCK_CHECKSUM */
    public static void opBlockChecksum(DataOutputStream out, ExtendedBlock blk,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      OpBlockChecksumProto proto = OpBlockChecksumProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .build();
      
      send(out, Op.BLOCK_CHECKSUM, proto);
    }
  }

  /** Receiver */
  public static abstract class Receiver {
    /** Read an Op.  It also checks protocol version. */
    protected final Op readOp(DataInputStream in) throws IOException {
      final short version = in.readShort();
      if (version != DATA_TRANSFER_VERSION) {
        throw new IOException( "Version Mismatch (Expected: " +
            DataTransferProtocol.DATA_TRANSFER_VERSION  +
            ", Received: " +  version + " )");
      }
      return Op.read(in);
    }

    /** Process op by the corresponding method. */
    protected final void processOp(Op op, DataInputStream in
        ) throws IOException {
      switch(op) {
      case READ_BLOCK:
        opReadBlock(in);
        break;
      case WRITE_BLOCK:
        opWriteBlock(in);
        break;
      case REPLACE_BLOCK:
        opReplaceBlock(in);
        break;
      case COPY_BLOCK:
        opCopyBlock(in);
        break;
      case BLOCK_CHECKSUM:
        opBlockChecksum(in);
        break;
      case TRANSFER_BLOCK:
        opTransferBlock(in);
        break;
      default:
        throw new IOException("Unknown op " + op + " in data stream");
      }
    }

    /** Receive OP_READ_BLOCK */
    private void opReadBlock(DataInputStream in) throws IOException {
      OpReadBlockProto proto = OpReadBlockProto.parseFrom(vintPrefixed(in));
      
      ExtendedBlock b = fromProto(
          proto.getHeader().getBaseHeader().getBlock());
      Token<BlockTokenIdentifier> token = fromProto(
          proto.getHeader().getBaseHeader().getToken());

      opReadBlock(in, b, proto.getOffset(), proto.getLen(),
          proto.getHeader().getClientName(), token);
    }
    /**
     * Abstract OP_READ_BLOCK method. Read a block.
     */
    protected abstract void opReadBlock(DataInputStream in, ExtendedBlock blk,
        long offset, long length, String client,
        Token<BlockTokenIdentifier> blockToken) throws IOException;
    
    /** Receive OP_WRITE_BLOCK */
    private void opWriteBlock(DataInputStream in) throws IOException {
      final OpWriteBlockProto proto = OpWriteBlockProto.parseFrom(vintPrefixed(in));
      opWriteBlock(in,
          fromProto(proto.getHeader().getBaseHeader().getBlock()),
          proto.getPipelineSize(),
          fromProto(proto.getStage()),
          proto.getLatestGenerationStamp(),
          proto.getMinBytesRcvd(), proto.getMaxBytesRcvd(),
          proto.getHeader().getClientName(),
          fromProto(proto.getSource()),
          fromProtos(proto.getTargetsList()),
          fromProto(proto.getHeader().getBaseHeader().getToken()));
    }

    /**
     * Abstract OP_WRITE_BLOCK method. 
     * Write a block.
     */
    protected abstract void opWriteBlock(DataInputStream in, ExtendedBlock blk,
        int pipelineSize, BlockConstructionStage stage, long newGs,
        long minBytesRcvd, long maxBytesRcvd, String client, DatanodeInfo src,
        DatanodeInfo[] targets, Token<BlockTokenIdentifier> blockToken)
        throws IOException;

    /** Receive {@link Op#TRANSFER_BLOCK} */
    private void opTransferBlock(DataInputStream in) throws IOException {
      final OpTransferBlockProto proto =
        OpTransferBlockProto.parseFrom(vintPrefixed(in));

      opTransferBlock(in,
          fromProto(proto.getHeader().getBaseHeader().getBlock()),
          proto.getHeader().getClientName(),
          fromProtos(proto.getTargetsList()),
          fromProto(proto.getHeader().getBaseHeader().getToken()));
    }

    /**
     * Abstract {@link Op#TRANSFER_BLOCK} method.
     * For {@link BlockConstructionStage#TRANSFER_RBW}
     * or {@link BlockConstructionStage#TRANSFER_FINALIZED}.
     */
    protected abstract void opTransferBlock(DataInputStream in, ExtendedBlock blk,
        String client, DatanodeInfo[] targets,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException;

    /** Receive OP_REPLACE_BLOCK */
    private void opReplaceBlock(DataInputStream in) throws IOException {
      OpReplaceBlockProto proto = OpReplaceBlockProto.parseFrom(vintPrefixed(in));

      opReplaceBlock(in,
          fromProto(proto.getHeader().getBlock()),
          proto.getDelHint(),
          fromProto(proto.getSource()),
          fromProto(proto.getHeader().getToken()));
    }

    /**
     * Abstract OP_REPLACE_BLOCK method.
     * It is used for balancing purpose; send to a destination
     */
    protected abstract void opReplaceBlock(DataInputStream in,
        ExtendedBlock blk, String delHint, DatanodeInfo src,
        Token<BlockTokenIdentifier> blockToken) throws IOException;

    /** Receive OP_COPY_BLOCK */
    private void opCopyBlock(DataInputStream in) throws IOException {
      OpCopyBlockProto proto = OpCopyBlockProto.parseFrom(vintPrefixed(in));
      
      opCopyBlock(in,
          fromProto(proto.getHeader().getBlock()),
          fromProto(proto.getHeader().getToken()));
    }

    /**
     * Abstract OP_COPY_BLOCK method. It is used for balancing purpose; send to
     * a proxy source.
     */
    protected abstract void opCopyBlock(DataInputStream in, ExtendedBlock blk,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException;

    /** Receive OP_BLOCK_CHECKSUM */
    private void opBlockChecksum(DataInputStream in) throws IOException {
      OpBlockChecksumProto proto = OpBlockChecksumProto.parseFrom(vintPrefixed(in));
      
      opBlockChecksum(in,
          fromProto(proto.getHeader().getBlock()),
          fromProto(proto.getHeader().getToken()));
    }

    /**
     * Abstract OP_BLOCK_CHECKSUM method.
     * Get the checksum of a block 
     */
    protected abstract void opBlockChecksum(DataInputStream in,
        ExtendedBlock blk, Token<BlockTokenIdentifier> blockToken)
        throws IOException;
  }
  
  /** reply **/
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class PipelineAck {
    PipelineAckProto proto;
    public final static long UNKOWN_SEQNO = -2;

    /** default constructor **/
    public PipelineAck() {
    }
    
    /**
     * Constructor
     * @param seqno sequence number
     * @param replies an array of replies
     */
    public PipelineAck(long seqno, Status[] replies) {
      proto = PipelineAckProto.newBuilder()
        .setSeqno(seqno)
        .addAllStatus(Arrays.asList(replies))
        .build();
    }
    
    /**
     * Get the sequence number
     * @return the sequence number
     */
    public long getSeqno() {
      return proto.getSeqno();
    }
    
    /**
     * Get the number of replies
     * @return the number of replies
     */
    public short getNumOfReplies() {
      return (short)proto.getStatusCount();
    }
    
    /**
     * get the ith reply
     * @return the the ith reply
     */
    public Status getReply(int i) {
      return proto.getStatus(i);
    }
    
    /**
     * Check if this ack contains error status
     * @return true if all statuses are SUCCESS
     */
    public boolean isSuccess() {
      for (DataTransferProtos.Status reply : proto.getStatusList()) {
        if (reply != DataTransferProtos.Status.SUCCESS) {
          return false;
        }
      }
      return true;
    }
    
    /**** Writable interface ****/
    public void readFields(InputStream in) throws IOException {
      proto = PipelineAckProto.parseFrom(vintPrefixed(in));
    }

    public void write(OutputStream out) throws IOException {
      proto.writeDelimitedTo(out);
    }
    
    @Override //Object
    public String toString() {
      return proto.toString();
    }
  }

  /**
   * Header data for each packet that goes through the read/write pipelines.
   */
  public static class PacketHeader {
    /** Header size for a packet */
    private static final int PROTO_SIZE = 
      PacketHeaderProto.newBuilder()
        .setOffsetInBlock(0)
        .setSeqno(0)
        .setLastPacketInBlock(false)
        .setDataLen(0)
        .build().getSerializedSize();
    public static final int PKT_HEADER_LEN =
      6 + PROTO_SIZE;

    private int packetLen;
    private PacketHeaderProto proto;

    public PacketHeader() {
    }

    public PacketHeader(int packetLen, long offsetInBlock, long seqno,
                        boolean lastPacketInBlock, int dataLen) {
      this.packetLen = packetLen;
      proto = PacketHeaderProto.newBuilder()
        .setOffsetInBlock(offsetInBlock)
        .setSeqno(seqno)
        .setLastPacketInBlock(lastPacketInBlock)
        .setDataLen(dataLen)
        .build();
    }

    public int getDataLen() {
      return proto.getDataLen();
    }

    public boolean isLastPacketInBlock() {
      return proto.getLastPacketInBlock();
    }

    public long getSeqno() {
      return proto.getSeqno();
    }

    public long getOffsetInBlock() {
      return proto.getOffsetInBlock();
    }

    public int getPacketLen() {
      return packetLen;
    }

    @Override
    public String toString() {
      return "PacketHeader with packetLen=" + packetLen +
        "Header data: " + 
        proto.toString();
    }
    
    public void readFields(ByteBuffer buf) throws IOException {
      packetLen = buf.getInt();
      short protoLen = buf.getShort();
      byte[] data = new byte[protoLen];
      buf.get(data);
      proto = PacketHeaderProto.parseFrom(data);
    }
    
    public void readFields(DataInputStream in) throws IOException {
      this.packetLen = in.readInt();
      short protoLen = in.readShort();
      byte[] data = new byte[protoLen];
      in.readFully(data);
      proto = PacketHeaderProto.parseFrom(data);
    }


    /**
     * Write the header into the buffer.
     * This requires that PKT_HEADER_LEN bytes are available.
     */
    public void putInBuffer(final ByteBuffer buf) {
      assert proto.getSerializedSize() == PROTO_SIZE
        : "Expected " + (PROTO_SIZE) + " got: " + proto.getSerializedSize();
      try {
        buf.putInt(packetLen);
        buf.putShort((short) proto.getSerializedSize());
        proto.writeTo(new ByteBufferOutputStream(buf));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    public void write(DataOutputStream out) throws IOException {
      assert proto.getSerializedSize() == PROTO_SIZE
      : "Expected " + (PROTO_SIZE) + " got: " + proto.getSerializedSize();
      out.writeInt(packetLen);
      out.writeShort(proto.getSerializedSize());
      proto.writeTo(out);
    }

    /**
     * Perform a sanity check on the packet, returning true if it is sane.
     * @param lastSeqNo the previous sequence number received - we expect the current
     * sequence number to be larger by 1.
     */
    public boolean sanityCheck(long lastSeqNo) {
      // We should only have a non-positive data length for the last packet
      if (proto.getDataLen() <= 0 && proto.getLastPacketInBlock()) return false;
      // The last packet should not contain data
      if (proto.getLastPacketInBlock() && proto.getDataLen() != 0) return false;
      // Seqnos should always increase by 1 with each packet received
      if (proto.getSeqno() != lastSeqNo + 1) return false;
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof PacketHeader)) return false;
      PacketHeader other = (PacketHeader)o;
      return this.proto.equals(other.proto);
    }

    @Override
    public int hashCode() {
      return (int)proto.getSeqno();
    }
  }

  /**
   * The setting of replace-datanode-on-failure feature.
   */
  public enum ReplaceDatanodeOnFailure {
    /** The feature is disabled in the entire site. */
    DISABLE,
    /** Never add a new datanode. */
    NEVER,
    /**
     * DEFAULT policy:
     *   Let r be the replication number.
     *   Let n be the number of existing datanodes.
     *   Add a new datanode only if r >= 3 and either
     *   (1) floor(r/2) >= n; or
     *   (2) r > n and the block is hflushed/appended.
     */
    DEFAULT,
    /** Always add a new datanode when an existing datanode is removed. */
    ALWAYS;

    /** Check if the feature is enabled. */
    public void checkEnabled() {
      if (this == DISABLE) {
        throw new UnsupportedOperationException(
            "This feature is disabled.  Please refer to "
            + DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY
            + " configuration property.");
      }
    }

    /** Is the policy satisfied? */
    public boolean satisfy(
        final short replication, final DatanodeInfo[] existings,
        final boolean isAppend, final boolean isHflushed) {
      final int n = existings == null? 0: existings.length;
      if (n == 0 || n >= replication) {
        //don't need to add datanode for any policy.
        return false;
      } else if (this == DISABLE || this == NEVER) {
        return false;
      } else if (this == ALWAYS) {
        return true;
      } else {
        //DEFAULT
        if (replication < 3) {
          return false;
        } else {
          if (n <= (replication/2)) {
            return true;
          } else {
            return isAppend || isHflushed;
          }
        }
      }
    }

    /** Get the setting from configuration. */
    public static ReplaceDatanodeOnFailure get(final Configuration conf) {
      final boolean enabled = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT);
      if (!enabled) {
        return DISABLE;
      }

      final String policy = conf.get(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY,
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT);
      for(int i = 1; i < values().length; i++) {
        final ReplaceDatanodeOnFailure rdof = values()[i];
        if (rdof.name().equalsIgnoreCase(policy)) {
          return rdof;
        }
      }
      throw new HadoopIllegalArgumentException("Illegal configuration value for "
          + DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY
          + ": " + policy);
    }

    /** Write the setting to configuration. */
    public void write(final Configuration conf) {
      conf.setBoolean(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
          this != DISABLE);
      conf.set(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY,
          name());
    }
  }
}
