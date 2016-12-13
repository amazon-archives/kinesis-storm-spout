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
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for RecordNodeList.
 */
public class RecordNodeListTest {

    private static final String SHARD_ID = "shardId-0";
    private static final int RECORD_RETRY_LIMIT = 3;
    private static final String INITIAL_SEQUENCE_NUMBER = "000";
    private InflightRecordTracker tracker = new InflightRecordTracker(SHARD_ID, INITIAL_SEQUENCE_NUMBER,
            RECORD_RETRY_LIMIT);
    private RecordNodeList list = null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        list = tracker.new RecordNodeList();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNodeList#addToList(com.amazonaws.services.kinesis.model.Record)}.
     */
    @Test
    public final void testAddToList() {
        Assert.assertNull(list.getFirst());
        Assert.assertNull(list.getLast());
        Assert.assertEquals(0, list.size());
        RecordNode firstNode = addNodeToListAndValidate(list, "1");
        RecordNode previous = firstNode;
        for (int i = 2; i < 15; i++) {
            RecordNode node = addNodeToListAndValidate(list, Integer.toString(i));
            assertSame(firstNode, list.getFirst());
            assertSame(previous, node.getPrev());
            assertNull(node.getNext());
            assertSame(node, previous.getNext());
            assertSame(node, list.getLast());
            previous = node;
        }
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNodeList#remove(com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNode)}.
     */
    @Test
    public final void testRemoveNodeSingle() {
        RecordNode node = addNodeToListAndValidate(list, "1");
        assertSame(node, list.getFirst());
        removeNodeAndValidate(list, node);
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNodeList#remove(com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNode)}.
     */
    @Test
    public final void testRemoveNodeMultiple() {
        RecordNode first = addNodeToListAndValidate(list, "1");
        RecordNode second = addNodeToListAndValidate(list, "2");
        RecordNode third = addNodeToListAndValidate(list, "3");
        RecordNode fourth = addNodeToListAndValidate(list, "4");

        assertSame(first, list.getFirst());
        assertSame(fourth, list.getLast());
        // Remove middle node
        removeNodeAndValidate(list, third);
        // Remove first node
        removeNodeAndValidate(list, first);
        // Remove node at the end
        removeNodeAndValidate(list, fourth);
        // Remove the last remaining node
        removeNodeAndValidate(list, second);
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNodeList#getFirst()}.
     */
    @Test
    public final void testGetFirst() {
        assertNull(list.getFirst());
        RecordNode node = addNodeToListAndValidate(list, "1");
        assertSame(node, list.getFirst());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNodeList#getLast()}.
     */
    @Test
    public final void testGetLast() {
        assertNull(list.getLast());
        RecordNode node = addNodeToListAndValidate(list, "1");
        assertSame(node, list.getLast());
        removeNodeAndValidate(list, node);
        assertNull(list.getLast());
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.stormspout.state.zookeeper.InflightRecordTracker.RecordNodeList#size()}.
     */
    @Test
    public final void testGetSize() {
        assertEquals(0, list.size());
        addNodeToListAndValidate(list, "1");
        assertEquals(1, list.size());
    }

    private RecordNode addNodeToListAndValidate(RecordNodeList list, String sequenceNumber) {
        int initialSize = list.size();
        Record record = newRecordWithSequenceNumber(sequenceNumber);
        RecordNode node = list.addToList(record);
        assertEquals(initialSize + 1, list.size());
        assertSame(node, list.getLast());
        return node;
    }

    private void removeNodeAndValidate(RecordNodeList list, RecordNode nodeToRemove) {
        int initialSize = list.size();
        RecordNode previous = nodeToRemove.getPrev();
        RecordNode next = nodeToRemove.getNext();
        list.remove(nodeToRemove);

        if (previous != null) {
            Assert.assertSame(next, previous.getNext());
        }
        if (next != null) {
            Assert.assertSame(previous, next.getPrev());
        }

        assertNotSame(nodeToRemove, list.getFirst());
        assertNotSame(nodeToRemove, list.getLast());
        assertEquals(initialSize - 1, list.size());
        if (initialSize == 1) {
            assertNull(list.getFirst());
            assertNull(list.getLast());
        }

    }

    private Record newRecordWithSequenceNumber(final String sequenceNumber) {
        Record record = new Record();
        record.setPartitionKey("TestPartitionKey");
        record.setSequenceNumber(sequenceNumber);
        return record;
    }

}
