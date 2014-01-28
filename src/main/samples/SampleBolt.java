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


import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.DefaultKinesisRecordScheme;

public class SampleBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 177788290277634253L;
    private static final Logger LOG = LoggerFactory.getLogger(SampleBolt.class);
    private static final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Record record = (Record)input.getValueByField(DefaultKinesisRecordScheme.FIELD_RECORD);
        ByteBuffer buffer = record.getData();
        String data = null; 
        try {
            data = decoder.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            LOG.error("Exception when decoding record ", e);
        }
        LOG.info("SampleBolt got record : " + record.getPartitionKey() + ", " + data);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
