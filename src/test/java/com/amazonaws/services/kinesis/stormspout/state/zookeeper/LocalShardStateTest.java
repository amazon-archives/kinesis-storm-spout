/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.kinesis.model.Record;

/**
 * Verifies that section 2.3.1 of the specs is implemented correctly.
 */
public class LocalShardStateTest {

    private LocalShardState state;

    @Before
    public void setUp() {
        state = new LocalShardState("shardId-0", "", 3);
    }

    @Test
    public void ackDoesNotCauseRetry() {
        state.emit(newRecordWithSequenceNumber("01"), false);
        state.ack("01");

        assertFalse(state.shouldRetry());
    }

    /** Acking a seq num that was not previously emitted does not cause the state to change. */
    @Test
    public void ackDoesNotUpdateVisibleStateOnUnkownSeqNum() {
        // TODO: Remove final String lastEmitted = state.getLastEmitted();
        final String latestValidSeqNum = state.getLatestValidSeqNum();
        final boolean dirty = state.isDirty();

        state.ack("01");
        // TODO: Remove assertThat(state.getLastEmitted(), is(equalTo(lastEmitted)));
        assertThat(state.getLatestValidSeqNum(), is(equalTo(latestValidSeqNum)));
        assertThat(state.isDirty(), is(equalTo(dirty)));

        state.emit(newRecordWithSequenceNumber("01"), false);
        state.ack("02");
        // TODO: Remove assertThat(state.getLastEmitted(), is(equalTo("01")));
        assertThat(state.getLatestValidSeqNum(), is(equalTo(latestValidSeqNum)));
        assertThat(state.isDirty(), is(equalTo(dirty)));
    }

    /** See specs@2.3.1 */
    @Test
    public void ackingContinousSeqNumsShouldUpdateLatestValidSeqNum() {
        state.emit(newRecordWithSequenceNumber("01"), false);
        state.emit(newRecordWithSequenceNumber("02"), false);
        state.emit(newRecordWithSequenceNumber("03"), false);

        state.ack("02");
        state.ack("01");

        assertThat(state.getLatestValidSeqNum(), is(equalTo("02")));
    }

    /** An ack that results to an update to the latest valid seq num should set the dirty flag. */
    @Test
    public void ackingContinousSeqNumsShouldSetDirtyFlag() {
        state.emit(newRecordWithSequenceNumber("01"), false);
        state.emit(newRecordWithSequenceNumber("02"), false);

        assertFalse(state.isDirty());
        state.ack("01");
        assertTrue(state.isDirty());
        state.commit("01");
        assertFalse(state.isDirty());
    }

    /** See specs@2.3.1 */
    @Test
    public void ackingShouldOnlyUpdateIfNoGap() {
        state.emit(newRecordWithSequenceNumber("01"), false);
        state.emit(newRecordWithSequenceNumber("02"), false);
        state.emit(newRecordWithSequenceNumber("03"), false);
        state.emit(newRecordWithSequenceNumber("04"), false);

        state.ack("01");
        assertThat(state.getLatestValidSeqNum(), is(equalTo("01")));

        state.ack("03");
        state.ack("04");
        assertThat(state.getLatestValidSeqNum(), is(equalTo("01")));

        state.ack("02");
        assertThat(state.getLatestValidSeqNum(), is(equalTo("04")));
    }

    /** Failing a record causes a retry of that record. */
    @Test
    public void failCausesRetry() {
        String sequenceNumber = "01";
        Record record = newRecordWithSequenceNumber(sequenceNumber);
        state.emit(record, false);
        state.fail(sequenceNumber);

        assertTrue(state.shouldRetry());
        assertThat(state.recordToRetry(), is(equalTo(record)));
        state.emit(record, true);
        assertFalse(state.shouldRetry());
    }

    /** Failing a seq num that was not previously emitted does not cause the state to change. */
    @Test
    public void failShouldNotUpdateVisibleStateOnUnknownSeqNums() {
        final boolean shouldRetry = state.shouldRetry();

        state.fail("01");
        assertThat(state.shouldRetry(), is(equalTo(shouldRetry)));

        state.emit(newRecordWithSequenceNumber("01"), true);
        state.fail("02");
        assertThat(state.shouldRetry(), is(equalTo(shouldRetry)));
    }

    private Record newRecordWithSequenceNumber(final String sequenceNumber) {
        Record record = new Record();
        record.setPartitionKey("TestPartitionKey");
        record.setSequenceNumber(sequenceNumber);
        return record;
    }
}
