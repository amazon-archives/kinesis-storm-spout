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

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNode;
import com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNodeList;
import org.junit.*;

import java.math.BigInteger;
import java.util.Map;

import static org.junit.Assert.*;


/**
 * Tests for InflightRecordTracker.
 */
public class InflightRecordTrackerTest {

    private static final String SHARD_ID = "shardId-0";
    private static final int RECORD_RETRY_LIMIT = 3;
    private static final String INITIAL_SEQUENCE_NUMBER = "000";
    private InflightRecordTracker tracker;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        tracker = new InflightRecordTracker(SHARD_ID, INITIAL_SEQUENCE_NUMBER, RECORD_RETRY_LIMIT);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker#getCheckpointSequenceNumber()}.
     */
    @Test
    public final void testGetCheckpointSequenceNumber() {
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        Record record = newRecordWithSequenceNumber("2");
        tracker.onEmit(record, isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("3"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("4"), isRetry);
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        tracker.onFail("1");
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        tracker.onAck("1");
        assertEquals("1", tracker.getCheckpointSequenceNumber());
        tracker.onAck("3");
        assertEquals("1", tracker.getCheckpointSequenceNumber());

        isRetry = true;
        tracker.onFail("2");
        tracker.onEmit(record, isRetry);
        assertEquals("1", tracker.getCheckpointSequenceNumber());
        tracker.onFail("2");
        tracker.onEmit(record, isRetry);
        assertEquals("1", tracker.getCheckpointSequenceNumber());
        tracker.onFail("2");
        tracker.onEmit(record, isRetry);
        assertEquals("1", tracker.getCheckpointSequenceNumber());
        tracker.onFail("2");
        assertEquals("3", tracker.getCheckpointSequenceNumber());

        tracker.onAck("4");
        assertEquals("4", tracker.getCheckpointSequenceNumber());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker#onEmit(com.amazonaws.services.kinesis.model.Record, boolean)}.
     */
    @Test
    public final void testOnEmitNotRetry() {
        Record record = newRecordWithSequenceNumber("1");
        boolean isRetry = false;
        tracker.onEmit(record, isRetry);
        RecordNodeList list = tracker.getRecordNodeList();
        assertEquals(1, list.size());
        assertSame(record, list.getFirst().getRecord());
        assertEquals(0, list.getFirst().getRetryCount());
        assertFalse(tracker.shouldRetry());
        assertTrue(tracker.getRetryQueue().isEmpty());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker#onAck(java.lang.String)}.
     */
    @Test
    public final void testOnAckSingle() {
        String sequenceNumber = "1";
        Record record = newRecordWithSequenceNumber(sequenceNumber);
        boolean isRetry = false;
        tracker.onEmit(record, isRetry);
        tracker.onAck(sequenceNumber);
        RecordNodeList list = tracker.getRecordNodeList();
        assertEquals(0, list.size());
        assertFalse(tracker.shouldRetry());
        assertTrue(tracker.getRetryQueue().isEmpty());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker#onFail(java.lang.String)}.
     */
    @Test
    public final void testOnFailSingle() {
        String sequenceNumber = "1";
        Record record = newRecordWithSequenceNumber(sequenceNumber);
        boolean isRetry = false;
        tracker.onEmit(record, isRetry);
        tracker.onFail(sequenceNumber);
        RecordNodeList list = tracker.getRecordNodeList();
        assertEquals(1, list.size());
        assertSame(record, list.getFirst().getRecord());
        assertTrue(tracker.shouldRetry());
        assertEquals(sequenceNumber, tracker.recordToRetry().getSequenceNumber());
        assertEquals(0, list.getFirst().getRetryCount());
        assertEquals(1, tracker.getRetryQueue().size());
        assertEquals(sequenceNumber, tracker.getRetryQueue().peek());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker#onFail(java.lang.String)}.
     */
    @Test
    public final void testPoisonPill() {
        String sequenceNumber = "1";
        Record record = newRecordWithSequenceNumber(sequenceNumber);
        boolean isRetry = false;
        tracker.onEmit(record, isRetry);
        tracker.onFail(sequenceNumber);
        RecordNodeList list = tracker.getRecordNodeList();
        assertEquals(1, list.size());
        assertSame(record, list.getFirst().getRecord());
        assertTrue(tracker.shouldRetry());
        assertEquals(sequenceNumber, tracker.recordToRetry().getSequenceNumber());
        assertEquals(0, list.getFirst().getRetryCount());
        assertEquals(1, tracker.getRetryQueue().size());
        assertEquals(sequenceNumber, tracker.getRetryQueue().peek());

        isRetry = true;
        tracker.onEmit(record, isRetry);
        assertEquals(0, tracker.getRetryQueue().size());
        assertEquals(1, list.size());
        assertEquals(1, list.getFirst().getRetryCount());
        assertFalse(tracker.shouldRetry());
        assertNull(tracker.recordToRetry());

        tracker.onFail(sequenceNumber);
        tracker.onEmit(record, isRetry);
        assertEquals(2, list.getFirst().getRetryCount());

        tracker.onFail(sequenceNumber);
        tracker.onEmit(record, isRetry);
        assertEquals(3, list.getFirst().getRetryCount());

        // At this point, we'll exhaust the retry limit and we'll treat this like an ack
        tracker.onFail(sequenceNumber);
        assertEquals(0, list.size());
        assertFalse(tracker.shouldRetry());

        tracker.onEmit(record, isRetry);
        assertEquals(0, list.size());
        assertFalse(tracker.shouldRetry());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker#shouldRetry()}.
     */
    @Test
    public final void testShouldRetry() {
        String sequenceNumber = "1";
        Record record = newRecordWithSequenceNumber(sequenceNumber);
        boolean isRetry = false;
        tracker.onEmit(record, isRetry);
        tracker.onFail(sequenceNumber);
        assertTrue(tracker.shouldRetry());
        assertEquals(sequenceNumber, tracker.getRetryQueue().peek());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker#recordToRetry()}.
     */
    @Test
    public final void testRecordToRetry() {
        String sequenceNumber = "1";
        Record record = newRecordWithSequenceNumber(sequenceNumber);
        boolean isRetry = false;
        tracker.onEmit(record, isRetry);
        tracker.onFail(sequenceNumber);
        assertTrue(tracker.shouldRetry());
        assertEquals(sequenceNumber, tracker.getRetryQueue().peek());
        assertEquals(sequenceNumber, tracker.recordToRetry().getSequenceNumber());
    }

    /**
     *
     */
    @Test
    public final void testAckNoPreviousNextAcked() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("3"), isRetry);
        tracker.onAck("2");

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(3, tracker.getRecordNodeList().size());
        tracker.onAck("1");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNull(seqNumToNodeMap.get("1"));
        assertNull(seqNumToNodeMap.get("2"));
        assertEquals(1, tracker.getRecordNodeList().size());
        assertEquals("2", tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckNoPreviousNextPending() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(2, tracker.getRecordNodeList().size());
        tracker.onAck("1");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNull(seqNumToNodeMap.get("1"));
        assertNotNull(seqNumToNodeMap.get("2"));
        assertEquals(1, tracker.getRecordNodeList().size());
        assertEquals("1", tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckNoPreviousNoNext() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(1, tracker.getRecordNodeList().size());
        tracker.onAck("1");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNull(seqNumToNodeMap.get("1"));
        assertEquals(0, tracker.getRecordNodeList().size());
        assertEquals("1", tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckPreviousPendingNextAcked() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("3"), isRetry);
        tracker.onAck("3");

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(3, tracker.getRecordNodeList().size());
        tracker.onAck("2");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNotNull(seqNumToNodeMap.get("1"));
        assertNull(seqNumToNodeMap.get("2"));
        assertNotNull(seqNumToNodeMap.get("3"));
        assertEquals(2, tracker.getRecordNodeList().size());
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckPreviousPendingNextPending() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("3"), isRetry);

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(3, tracker.getRecordNodeList().size());
        tracker.onAck("2");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNotNull(seqNumToNodeMap.get("1"));
        assertNotNull(seqNumToNodeMap.get("2"));
        assertNotNull(seqNumToNodeMap.get("3"));
        assertEquals(3, tracker.getRecordNodeList().size());
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckPreviousPendingNoNext() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(2, tracker.getRecordNodeList().size());
        tracker.onAck("2");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNotNull(seqNumToNodeMap.get("1"));
        assertNotNull(seqNumToNodeMap.get("2"));
        assertEquals(2, tracker.getRecordNodeList().size());
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckPreviousAckedNextAcked() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("3"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("4"), isRetry);
        tracker.onAck("2");
        tracker.onAck("4");

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(4, tracker.getRecordNodeList().size());
        tracker.onAck("3");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNotNull(seqNumToNodeMap.get("1"));
        assertNull(seqNumToNodeMap.get("2"));
        assertNull(seqNumToNodeMap.get("3"));
        assertNotNull(seqNumToNodeMap.get("4"));
        assertEquals(2, tracker.getRecordNodeList().size());
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckPreviousAckedNextPending() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("3"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("4"), isRetry);
        tracker.onAck("2");

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(4, tracker.getRecordNodeList().size());
        tracker.onAck("3");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNotNull(seqNumToNodeMap.get("1"));
        assertNull(seqNumToNodeMap.get("2"));
        assertNotNull(seqNumToNodeMap.get("3"));
        assertNotNull(seqNumToNodeMap.get("4"));
        assertEquals(3, tracker.getRecordNodeList().size());
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    @Test
    public final void testAckPreviousAckedNoNext() {
        boolean isRetry = false;
        tracker.onEmit(newRecordWithSequenceNumber("1"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("2"), isRetry);
        tracker.onEmit(newRecordWithSequenceNumber("3"), isRetry);
        tracker.onAck("2");

        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        assertEquals(3, tracker.getRecordNodeList().size());
        tracker.onAck("3");
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        assertNotNull(seqNumToNodeMap.get("1"));
        assertNull(seqNumToNodeMap.get("2"));
        assertNotNull(seqNumToNodeMap.get("3"));
        assertEquals(2, tracker.getRecordNodeList().size());
        assertEquals(INITIAL_SEQUENCE_NUMBER, tracker.getCheckpointSequenceNumber());
        validateInternalState();
    }

    private void validateInternalState() {
        Map<String, RecordNode> seqNumToNodeMap = tracker.getSequenceNumberToRecordNodeMap();
        RecordNodeList list = tracker.getRecordNodeList();
        BigInteger checkpoint = new BigInteger(tracker.getCheckpointSequenceNumber());

        assertEquals(seqNumToNodeMap.size(), list.size());
        RecordNode node = list.getFirst();
        RecordNode expectedLast = node;
        RecordNode expectedPrevious = null;
        while (node != null) {
            String sequenceNumber = node.getRecord().getSequenceNumber();
            RecordNode nodeInMap = seqNumToNodeMap.get(sequenceNumber);
            assertNotNull(nodeInMap);
            assertSame(node, nodeInMap);
            assertSame(expectedPrevious, node.getPrev());
            assertTrue(checkpoint.compareTo(new BigInteger(sequenceNumber)) < 0);
            if (node.isAcked()) {
                assertTrue((node.getPrev() == null) || (!node.getPrev().isAcked()));
            }
            expectedPrevious = node;
            expectedLast = node;
            node = node.getNext();
        }
        assertSame(expectedLast, list.getLast());
    }

    private Record newRecordWithSequenceNumber(final String sequenceNumber) {
        Record record = new Record();
        record.setPartitionKey("TestPartitionKey");
        record.setSequenceNumber(sequenceNumber);
        return record;
    }

}
