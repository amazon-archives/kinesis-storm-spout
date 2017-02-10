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

package com.amazonaws.services.kinesis.stormspout;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

/**
 *
 */
public class KinesisShardGetterTest {
    String streamName = "TestStream";
    ShardInfo shardId = new ShardInfo("shardId-00001", true);

    AmazonKinesisClient mockKinesisClient = Mockito.mock(AmazonKinesisClient.class);
    KinesisShardGetter getter = new KinesisShardGetter(streamName, shardId, mockKinesisClient);

    /**
     * Test method for {@link com.amazonaws.services.kinesis.stormspout.KinesisShardGetter#getNext(int)}.
     */
    @Test
    public final void testGetNextWithAmazonServiceException() {
        when(mockKinesisClient.getRecords(isA(GetRecordsRequest.class)))
                .thenThrow(new AmazonServiceException("Test Exception"));
        Records records = getter.getNext(1);
        assertTrue(records.getRecords().isEmpty());
    }

    /**
     * Test method for {@link com.amazonaws.services.kinesis.stormspout.KinesisShardGetter#getNext(int)}.
     */
    @Test
    public final void testGetNextNonZeroRecords() {

        GetRecordsResult result = new GetRecordsResult();
        result.setNextShardIterator("TestIterator");
        result.setMillisBehindLatest(1l);
        List<Record> records = new ArrayList<>();

        Record record = new Record();
        String partitionKey = "TestKey";
        record.setPartitionKey(partitionKey);
        String sequenceNumber = "12304987";
        record.setSequenceNumber(sequenceNumber);

        records.add(record);
        result.setRecords(records);
        ImmutableList<Record> listRecords = ImmutableList.copyOf(records);

        when(mockKinesisClient.getRecords(isA(GetRecordsRequest.class))).thenReturn(result);

        Records actualRecords = getter.getNext(1);

        assertEquals(records.size(), actualRecords.getRecords().size());
        Record actualRecord = actualRecords.getRecords().get(0);
        assertEquals(partitionKey, actualRecord.getPartitionKey());
        assertEquals(sequenceNumber, actualRecord.getSequenceNumber());
    }

}
