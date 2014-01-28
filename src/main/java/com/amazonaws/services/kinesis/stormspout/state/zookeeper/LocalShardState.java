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

package com.amazonaws.services.kinesis.stormspout.state.zookeeper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tracks the state of a shard (e.g. current shard position).
 */
class LocalShardState {
    private static final Logger LOG = LoggerFactory.getLogger(LocalShardState.class);

    private final String shardId;

    private String latestValidSeqNum;
    private boolean dirty;

    private List<String> emittedSeqNums;
    private SortedSet<MutableInt> ackedSortedSet;

    private Queue<String> retryQueue;
    private String lastEmitted;

    /**
     * Constructor.
     * 
     * @param shardId ID of the shard this LocalShardState is tracking.
     * @param latestZookeeperSeqNum the last checkpoint stored in Zookeeper.
     */
    LocalShardState(final String shardId, final String latestZookeeperSeqNum) {
        this.shardId = shardId;
        this.latestValidSeqNum = latestZookeeperSeqNum;
        this.dirty = false;
        this.emittedSeqNums = new ArrayList<>();
        this.ackedSortedSet = new TreeSet<>();
        this.retryQueue = new LinkedList<>();
        this.lastEmitted = "";
    }

    /**
     * Call when a record is emitted in nextTuple.
     *
     * @param seqNum  the sequence number of the record.
     */
    void emit(final String seqNum) {
        // Add to *end* only if it is not a retry.
        if (!emittedSeqNums.contains(seqNum)) {
            LOG.debug(this + " adding " + seqNum + " to emitted seqnums.");
            emittedSeqNums.add(seqNum);
        }
        lastEmitted = seqNum;
    }

    /**
     * Call when a record is acknowledged. This will try to update the latest offset to be
     * stored in Zookeeper, if possible.
     *
     * @param seqNum  the sequence number of the record.
     */
    void ack(final String seqNum) {
        final MutableInt indexInEmittedList = new MutableInt(emittedSeqNums.indexOf(seqNum));

        // Allow acking a record more than once (useful to handle behavior for retrying trimmed
        // records in CommonSpout).
        if (indexInEmittedList.intValue() == -1) {
            LOG.debug(this + " " + seqNum + " got acked more than once."
                     + " This could be expected, no actions taken.");
            return;
        }

        // Mark the index of the current seqNum as acked.
        ackedSortedSet.add(indexInEmittedList);

        if (ackedSortedSet.first().intValue() == 0) {
            LOG.debug(this + " first element of set is 0 - computing new sequence number to checkpoint.");
            validateNewSeqNum();
        }
    }

    /** 
     * Note: At this time, we don't retry failed messages. We treat this like an ack to unblock the spout task.
     * Call when a record is failed. It is then added to a retry queue that is queried by
     * nextTuple().
     *
     * @param failedSequenceNumber  sequence number of failed record.
     */
    void fail(final String failedSequenceNumber) {
        // Only fail if it's actually been emitted.
        if (emittedSeqNums.indexOf(failedSequenceNumber) != -1) {
            // Note: At this time, we don't support retry/replay of failed messages, so we treat like ack.
            ack(failedSequenceNumber);
            //retryQueue.add(failedSequenceNumber);
        }
    }

    /**
     * Get a sequence number to retry.
     *
     * Pre : shouldRetry().
     * @return a sequence number.
     */
    String retry() {
        assert shouldRetry() : "Nothing to retry.";
        return retryQueue.remove();
    }

    /**
     * @return true if there are sequence numbers that need to be retried.
     */
    boolean shouldRetry() {
        return !retryQueue.isEmpty();
    }

    /**
     * Get the latest sequence number validated by the shard state. This should be stored
     * periodically in Zookeeper.
     *
     * @return a sequence number.
     */
    String getLatestValidSeqNum() {
        return latestValidSeqNum;
    }

    /**
     * @return the sequence number of the last emitted record.
     */
    String getLastEmitted() {
        return lastEmitted;
    }

    /**
     * @return true if the latest valid sequence number has changed (new records were processed).
     */
    boolean isDirty() {
        return dirty;
    }

    /**
     * Reset dirty flag upon saving state (checkpoint).
     */
    void commit() {
        dirty = false;
    }

    /**
     * Helper log function. Checks that debug logging is enabled before evaluating the
     * detailedToString() function.
     *
     * @param prefix  prefix to prepend to log message.
     */
    void logMe(String prefix) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(this + " " + prefix + " " + detailedToString());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("shardId", shardId)
            .toString();
    }

    private String detailedToString() {
        return ReflectionToStringBuilder.toString(this);
    }

    /**
     * Updates internal data structures to reflect acked records.
     * E.g. advance the sequence number to be used for checkpoint.
     */
    private void validateNewSeqNum() {
        logMe("BEFORE computing new candidate checkpoint sequence number.");

        Iterator<MutableInt> it = ackedSortedSet.iterator();
        int lastElemOfSubSeq = it.next().intValue();
        it.remove();

        // If the first element of the set is 0, then we can move the latestValidSeqNum forward.
        // We compute the largest subsequence of ackedSortedSet that can be validated to update
        // latestValidSeqNum.
        // We will also remove those elements from ackedSortedSet as they will then be considered to
        // be validated.
        while (it.hasNext()) {
            if (it.next().intValue() == lastElemOfSubSeq + 1) {
                lastElemOfSubSeq++;
                it.remove();
            } else {
                break;
            }
        }

        // We can now index into emittedSeqNums with lastElemOfSubSeq to find out which is the
        // highest seqnum we can validate. Since they are stored in the order they arrive and
        // we have guaranteed all previous values (with the subseq) have been acked, it is safe
        // to do this.
        latestValidSeqNum = emittedSeqNums.get(lastElemOfSubSeq);
        dirty = true;

        // No need to keep all values from the subseq in memory anymore, they have been validated.
        emittedSeqNums.subList(0, lastElemOfSubSeq + 1).clear();

        // Last step is to offset all elements left in the ackedSortedSet by the length of the
        // subseq, to make sure they still point to the correct element in the (now shorter)
        // emittedSeqNums list.
        for (MutableInt v: ackedSortedSet) {
            v.subtract(lastElemOfSubSeq + 1);
        }

        logMe("AFTER computing new candidate checkpoint sequence number.");
    }
}
