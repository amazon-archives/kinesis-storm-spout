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

import org.junit.Test;

import com.amazonaws.regions.Regions;

/**
 * Unit tests for the KinesisSpoutConfig class.
 */
public class KinesisSpoutConfigTest {

    /**
     * Test null check for withRegion() with null param.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testWithRegionNullCheckWithNull() {
        KinesisSpoutConfig config = new KinesisSpoutConfig("testStream", "testZookeeperConnection");
        config.withRegion(null);
    }

    /**
     * Test null check for withRegion() with non-null param.
     */
    @Test
    public final void testWithRegionNullCheckWithNonNull() {
        KinesisSpoutConfig config = new KinesisSpoutConfig("testStream", "testZookeeperConnection");
        Regions region = Regions.US_WEST_2;
        config.withRegion(region);
    }
}
