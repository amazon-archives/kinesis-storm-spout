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


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.amazonaws.services.kinesis.stormspout.serializer.RecordSerializer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.stormspout.DefaultKinesisRecordScheme;
import com.amazonaws.services.kinesis.stormspout.InitialPositionInStream;
import com.amazonaws.services.kinesis.stormspout.KinesisSpout;
import com.amazonaws.services.kinesis.stormspout.KinesisSpoutConfig;

public class SampleTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SampleTopology.class);
    private static String topologyName = "SampleTopology";
    private static String streamName;
    private static InitialPositionInStream initialPositionInStream;
    private static String zookeeperEndpoint;
    private static String zookeeperPrefix;

    public static void main(String[] args) throws IllegalArgumentException, KeeperException, InterruptedException, AlreadyAliveException, InvalidTopologyException, IOException {
        String propertiesFile = null;
        String mode = null;
        
        if (args.length != 2) {
            printUsageAndExit();
        } else {
            propertiesFile = args[0];
            mode = args[1];
        }
        
        configure(propertiesFile);

        final KinesisSpoutConfig config =
                new KinesisSpoutConfig(streamName, zookeeperEndpoint).withZookeeperPrefix(zookeeperPrefix)
                        .withInitialPositionInStream(initialPositionInStream)                        ;

        final KinesisSpout spout = new KinesisSpout(config, new CustomCredentialsProviderChain(), new ClientConfiguration());
        TopologyBuilder builder = new TopologyBuilder();
        LOG.info("Using Kinesis stream: " + config.getStreamName());

        // Using number of shards as the parallelism hint for the spout.
        builder.setSpout("kinesis_spout", spout, 2);
        builder.setBolt("print_bolt", new SampleBolt(), 2).fieldsGrouping("kinesis_spout", new Fields(DefaultKinesisRecordScheme.FIELD_PARTITION_KEY));

        Config topoConf = new Config();
        topoConf.setFallBackOnJavaSerialization(true);
        topoConf.registerSerialization(Record.class, RecordSerializer.class);
        topoConf.setDebug(false);

        if (mode.equals("LocalMode")) {
            LOG.info("Starting sample storm topology in LocalMode ...");
            new LocalCluster().submitTopology("test_spout", topoConf, builder.createTopology());
        } else if (mode.equals("RemoteMode")) {
            topoConf.setNumWorkers(1);
            topoConf.setMaxSpoutPending(5000);
            LOG.info("Submitting sample topology " + topologyName + " to remote cluster.");
            StormSubmitter.submitTopology(topologyName, topoConf, builder.createTopology());            
        } else {
            printUsageAndExit();
        }

    }
    
    private static void configure(String propertiesFile) throws IOException {
        FileInputStream inputStream = new FileInputStream(propertiesFile);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }

        String topologyNameOverride = properties.getProperty(ConfigKeys.TOPOLOGY_NAME_KEY);
        if (topologyNameOverride != null) {
            topologyName = topologyNameOverride;
        }
        LOG.info("Using topology name " + topologyName);

        String streamNameOverride = properties.getProperty(ConfigKeys.STREAM_NAME_KEY);
        if (streamNameOverride != null) {
            streamName = streamNameOverride;
        }
        LOG.info("Using stream name " + streamName);

        String initialPositionOverride = properties.getProperty(ConfigKeys.INITIAL_POSITION_IN_STREAM_KEY);
        if (initialPositionOverride != null) {
             initialPositionInStream = InitialPositionInStream.valueOf(initialPositionOverride);
        }
        LOG.info("Using initial position " + initialPositionInStream.toString() + " (if a checkpoint is not found).");

        String zookeeperEndpointOverride = properties.getProperty(ConfigKeys.ZOOKEEPER_ENDPOINT_KEY);
        if (zookeeperEndpointOverride != null) {
            zookeeperEndpoint = zookeeperEndpointOverride;
        }
        LOG.info("Using zookeeper endpoint " + zookeeperEndpoint);

        String zookeeperPrefixOverride = properties.getProperty(ConfigKeys.ZOOKEEPER_PREFIX_KEY);
        if (zookeeperPrefixOverride != null) {            
            zookeeperPrefix = zookeeperPrefixOverride;
        }
        LOG.info("Using zookeeper prefix " + zookeeperPrefix);

    }
    
    private static void printUsageAndExit() {
        System.out.println("Usage: " + SampleTopology.class.getName() + " <propertiesFile> <LocalMode or RemoteMode>");
        System.exit(-1);
    }

}
