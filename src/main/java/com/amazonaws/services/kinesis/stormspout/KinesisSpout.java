/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.stormspout;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.state.zookeeper.ZookeeperStateManager;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Storm spout for Amazon Kinesis. The spout fetches data from Kinesis and emits a tuple for each data record.
 * <p>
 * Note: every spout task handles a distinct set of shards.
 */
public class KinesisSpout implements IRichSpout, Serializable {
    private static final long serialVersionUID = 7707829996758189836L;
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSpout.class);
    public static final String TOPOLOGY_NAME_MDC_KEY = "component";

    // Initialized before open
    private final InitialPositionInStream initialPosition;
    private final KinesisHelper shardListGetter;
    private final KinesisSpoutConfig config;
    private final long emptyRecordListSleepTimeMillis = 5L;
    private final IShardGetterBuilder getterBuilder;

    // Initialized on open
    private transient SpoutOutputCollector collector;
    private transient TopologyContext context;
    private transient ZookeeperStateManager stateManager;
    private transient long lastCommitTime;

    /**
     * Constructs an instance of the spout with just enough data to bootstrap the state from.
     * Construction done here is common to all spout tasks, whereas the IKinesisSpoutStateManager created
     * in activate() is task specific.
     *
     * @param config              Spout configuration.
     * @param credentialsProvider Used when making requests to Kinesis.
     * @param clientConfiguration Client configuration used when making calls to Kinesis.
     */
    public KinesisSpout(KinesisSpoutConfig config,
                        AWSCredentialsProvider credentialsProvider,
                        ClientConfiguration clientConfiguration) {
        this.config = config;
        KinesisHelper helper = new KinesisHelper(config.getStreamName(),
                credentialsProvider,
                clientConfiguration,
                config.getRegion());
        this.shardListGetter = helper;
        this.getterBuilder =
                new KinesisShardGetterBuilder(config.getStreamName(),
                        helper,
                        config.getMaxRecordsPerCall(),
                        config.getEmptyRecordListBackoffMillis());
        this.initialPosition = config.getInitialPositionInStream();
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") final Map conf,
                     final TopologyContext spoutContext,
                     final SpoutOutputCollector spoutCollector) {
        config.setTopologyName((String) conf.get(Config.TOPOLOGY_NAME));

        this.context = spoutContext;
        this.collector = spoutCollector;
        this.stateManager = new ZookeeperStateManager(config, shardListGetter, getterBuilder, initialPosition);
        MDC.put(TOPOLOGY_NAME_MDC_KEY, (String) conf.get(Config.TOPOLOGY_NAME));
        LOG.info(this + " open() called with topoConfig task index " + spoutContext.getThisTaskIndex()
                + " for processing stream " + config.getStreamName());
    }

    @Override
    public void close() {
        MDC.remove(TOPOLOGY_NAME_MDC_KEY);
    }

    @Override
    public void activate() {
        LOG.debug(this + " activating. Starting to process stream " + config.getStreamName());
        final int taskIndex = context.getThisTaskIndex();
        final int totalNumTasks = context.getComponentTasks(context.getThisComponentId()).size();
        stateManager.activate();
        stateManager.rebalance(taskIndex, totalNumTasks);

        lastCommitTime = System.currentTimeMillis();
    }

    // A deactivated spout will not have nextTuple called on itself.
    // When the spout is deactivated, it will not continue writing the state to Zookeeper
    // periodically (it will flush on deactivate, then close the ZK connection).
    // It will however continue updating its local state in case it is activated again later
    // and needs to carry on working.
    @Override
    public void deactivate() {
        LOG.debug(this + " deactivating.");
        try {
            stateManager.deactivate();
        } catch (Exception e) {
            LOG.warn(this + " could not deactivate stateManager.", e);
        }

    }

    @Override
    public void nextTuple() {
        synchronized (stateManager) {
            // Task has no assignments.
            if (!stateManager.hasGetters()) {
                // Sleep here for a bit, so we don't consume too much cpu.
                try {
                    Thread.sleep(emptyRecordListSleepTimeMillis);
                } catch (InterruptedException e) {
                    LOG.debug(this + " sleep was interrupted.");
                }
                return;
            }

            final IShardGetter getter = stateManager.getNextGetter();
            String currentShardId = getter.getAssociatedShard();
            Record rec = null;
            boolean isRetry = false;

            if (stateManager.shouldRetry(currentShardId)) {
                rec = stateManager.recordToRetry(currentShardId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ShardId " + currentShardId + ": Re-emitting record with partition key " + rec.getPartitionKey() + ", sequence number "
                            + rec.getSequenceNumber());
                }
                isRetry = true;
            } else {
                final Records records = getter.getNext(1);
                final ImmutableList<Record> recordList = records.getRecords();
                if ((recordList != null) && (!recordList.isEmpty())) {
                    rec = recordList.get(0);
                }
                if (records.isReshard()) {
                    LOG.info(this + " detected reshard event for shard " + currentShardId);
                    stateManager.handleReshard();
                }
            }

            if (rec != null) {
                // Copy record (ByteBuffer.duplicate()) so bolts in the same JVM don't affect the object (e.g. retries)
                Record recordToEmit = copyRecord(rec);
                List<Object> tuple = config.getScheme().deserialize(recordToEmit);
                LOG.info(this + " emitting record with seqnum " + recordToEmit.getSequenceNumber() + " from shard "
                        + currentShardId + " with data: " + tuple);
                collector.emit(tuple, MessageIdUtil.constructMessageId(currentShardId, recordToEmit.getSequenceNumber()));
                stateManager.emit(currentShardId, recordToEmit, isRetry);
            } else {
                // Sleep here for a bit if there were no records to emit.
                try {
                    Thread.sleep(emptyRecordListSleepTimeMillis);
                } catch (InterruptedException e) {
                    LOG.debug(this + " sleep was interrupted.");
                }
            }

            // Do periodic ZK commit of shard states.
            if (System.currentTimeMillis() - lastCommitTime >= config.getCheckpointIntervalMillis()) {
                LOG.debug(this + " committing local shard states to ZooKeeper.");

                stateManager.commitShardStates();
                lastCommitTime = System.currentTimeMillis();
            } else {
                LOG.debug(this + " Not committing to ZooKeeper.");
            }
        }
    }

    /**
     * Creates a copy of the record so we don't get interference from bolts that execute in the same JVM.
     * We invoke ByteBuffer.duplicate() so the ByteBuffer state is decoupled.
     *
     * @param record Kinesis record
     * @return Copied record.
     */
    private Record copyRecord(Record record) {
        Record duplicate = new Record();
        duplicate.setPartitionKey(record.getPartitionKey());
        duplicate.setSequenceNumber(record.getSequenceNumber());
        duplicate.setData(record.getData().duplicate());
        duplicate.setApproximateArrivalTimestamp(record.getApproximateArrivalTimestamp());
        return duplicate;
    }

    @Override
    public void ack(Object msgId) {
        synchronized (stateManager) {
            assert msgId instanceof String : "Expecting msgId_ to be a String";
            final String seqNum = (String) MessageIdUtil.sequenceNumberOfMessageId((String) msgId);
            final String shardId = MessageIdUtil.shardIdOfMessageId((String) msgId);
            if (LOG.isDebugEnabled()) {
                LOG.debug(this + " Processing ack() for " + msgId + ", shardId " + shardId + " seqNum " + seqNum);
            }
            stateManager.ack(shardId, seqNum);
        }
    }

    @Override
    public void fail(Object msgId) {
        synchronized (stateManager) {
            assert msgId instanceof String : "Expecting msgId_ to be a String";
            final String seqNum = (String) MessageIdUtil.sequenceNumberOfMessageId((String) msgId);
            final String shardId = MessageIdUtil.shardIdOfMessageId((String) msgId);
            LOG.info(this + " Processing failed: " + shardId + ", seqNum " + seqNum);
            stateManager.fail(shardId, seqNum);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(config.getScheme().getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("taskIndex",
                context.getThisTaskIndex()).toString();
    }
}
