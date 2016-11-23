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

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.BufferedGetter.TimeProvider;
import com.google.common.collect.ImmutableList;

import junit.framework.Assert;

/**
 * Unit tests for the BufferedGetter class.
 */
public class BufferedGetterTest {

    /**
     * Test method for {@link com.amazonaws.services.kinesis.stormspout.BufferedGetter#getNext(int)}.
     */
    @Test
    public final void testEmptyRecordListBackoff() {
        IShardGetter underlyingGetter = mock(IShardGetter.class);
        boolean endOfShard = false;
        ImmutableList.Builder<Record> recordBuilder = new ImmutableList.Builder<>();
        Records records = new Records(recordBuilder.build(), endOfShard);
        when(underlyingGetter.getNext(anyInt())).thenReturn(records);
        TimeProvider timeProvider = mock(TimeProvider.class);
        long time = System.currentTimeMillis();
        when(timeProvider.getCurrentTimeMillis()).thenReturn(time);
        BufferedGetter getter = new BufferedGetter(underlyingGetter, 1, 1L, timeProvider);
        getter.getNext(1);
        when(underlyingGetter.getNext(anyInt())).thenThrow(new AssertionError("Did not backoff"));
        getter.getNext(1);
    }

    @Test
    public final void testEmptyRecordListAfterBackoff() {
        IShardGetter underlyingGetter = mock(IShardGetter.class);
        boolean endOfShard = false;
        ImmutableList.Builder<Record> recordBuilder = new ImmutableList.Builder<>();
        Records records = new Records(recordBuilder.build(), endOfShard);
        when(underlyingGetter.getNext(anyInt())).thenReturn(records);
        TimeProvider timeProvider = mock(TimeProvider.class);
        long time = System.currentTimeMillis();
        when(timeProvider.getCurrentTimeMillis()).thenReturn(time);
        BufferedGetter getter = new BufferedGetter(underlyingGetter, 1, 1L, timeProvider);
        getter.getNext(1);
        Record record = new Record();
        String sequenceNumber = "123398";
        record.setSequenceNumber(sequenceNumber);
        recordBuilder.add(record);
        Records records2 = new Records(recordBuilder.build(), endOfShard);
        when(underlyingGetter.getNext(anyInt())).thenReturn(records2);
        when(timeProvider.getCurrentTimeMillis()).thenReturn(time + 10L);
        Records testRecords = getter.getNext(1);
        Assert.assertEquals(sequenceNumber, testRecords.getRecords().get(0).getSequenceNumber());

        String sequenceNumber2 = "43890";
        record.setSequenceNumber(sequenceNumber2);
        recordBuilder.add(record);
        records2 = new Records(recordBuilder.build(), endOfShard);
        when(underlyingGetter.getNext(anyInt())).thenReturn(records2);
        when(timeProvider.getCurrentTimeMillis()).thenReturn(time + 10L);
        testRecords = getter.getNext(1);
        Assert.assertEquals(sequenceNumber2, testRecords.getRecords().get(0).getSequenceNumber());
    }

}
