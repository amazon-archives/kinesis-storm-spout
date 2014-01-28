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

import java.util.Iterator;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.exceptions.InvalidSeekPositionException;
import com.google.common.collect.ImmutableList;

/**
 * Allows users to do efficient getter.getNext(1) calls in exchange for maybe pulling
 * more data than necessary from Kinesis.
 */
class BufferedGetter implements IShardGetter {
    private final IShardGetter getter;
    private final int maxBufferSize;

    private Records buffer;
    private Iterator<Record> it;

    /**
     * Creates a (shard) getter that buffers records.
     * 
     * @param underlyingGetter Unbuffered shard getter.
     * @param maxBufferSize max number of records to fetch from the underlying getter.
     */
    public BufferedGetter(final IShardGetter underlyingGetter, final int maxBufferSize) {
        this.getter = underlyingGetter;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Records getNext(int maxNumberOfRecords) {
        ensureBuffered();

        if (!it.hasNext() && buffer.isEndOfShard()) {
            return new Records(ImmutableList.<Record> of(), true);
        }

        ImmutableList.Builder<Record> recs = new ImmutableList.Builder<>();
        int recsSize = 0;

        while (recsSize < maxNumberOfRecords) {
            if (it.hasNext()) {
                recs.add(it.next());
                recsSize++;
            } else if (!it.hasNext() && !buffer.isEndOfShard()) {
                rebuffer();
                // No more data in shard.
                if (!it.hasNext()) {
                    break;
                }
            } else {
                // No more records, end of shard.
                break;
            }
        }

        return new Records(recs.build(), false);
    }

    @Override
    public void seek(ShardPosition position) throws InvalidSeekPositionException {
        getter.seek(position);
        buffer = null;
        it = null;
    }

    @Override
    public String getAssociatedShard() {
        return getter.getAssociatedShard();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("getter", getter.toString())
                .toString();
    }

    private void ensureBuffered() {
        if (buffer == null || it == null) {
            rebuffer();
        }
    }

    // Post : buffer != null && it != null
    private void rebuffer() {
        buffer = getter.getNext(maxBufferSize);
        it = buffer.getRecords().iterator();
    }
}
