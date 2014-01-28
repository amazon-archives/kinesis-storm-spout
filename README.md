# Amazon Kinesis Storm Spout

The **Amazon Kinesis Storm Spout** helps Java developers integrate [Amazon Kinesis][aws-kinesis] with [Storm](http://storm-project.net/).

## Requirements

 + [AWS SDK for Java](http://aws.amazon.com/sdkforjava)
 + [Java 1.7 (Java SE 7)](http://www.oracle.com/technetwork/java/javase/overview/index.html) or later
 + [Apache Commons Lang](http://commons.apache.org/proper/commons-lang/) 3.0 or later
 + [Google Guava](https://code.google.com/p/guava-libraries/) 13.0 or later
 + And, of course, [Amazon Kinesis](aws-kinesis) and [Storm](http://storm-project.net/)

## Overview

The Kinesis Storm spout fetches data records from Amazon Kinesis and emits them as tuples. The spout stores state in [ZooKeeper](http://zookeeper.apache.org/) to track the current position in the stream.

To use the spout, you'll need to add it to your Storm topology. 

+ **KinesisSpout**: Constructs an instance of the spout, using your AWS credentials and the configuration specified in KinesisSpoutConfig (as well as com.amazonaws.ClientConfiguration, via the AWS SDK). Each task executed by the spout operates on a distinct set of Kinesis shards. Shard states are periodically committed to ZooKeeper. When the spout is deactivated, it will disconnect from ZooKeeper, but the spout will continue monitoring its local state so you can activate it again later.
+ **KinesisSpoutConfig**: Configures the spout, including the Storm topology name, the Kinesis stream name, the endpoint for connecting to ZooKeeper and the prefix for the ZooKeeper paths where the spout state is stored. See the samples folder for configuration examples.
+ **DefaultKinesisRecordScheme**: This default scheme, used by the sample topology, emits a tuple of `(partitionKey, record)`. If you want to emit more structured data, you can provide your own implementation of IKinesisRecordScheme.

The samples folder includes a sample topology and sample bolt, using the number of Kinesis shards as the parallelism hint for the spout. For more information about Storm topologies and bolts, see the [Storm wiki](https://github.com/nathanmarz/storm/wiki).

## Using the Sample

1. Edit the *.properties file to configure your Storm topology, Kinesis Stream, and ZooKeeper details. For your AWS Credentials, we receommend using IAM roles on EC2 when possible. You can also specify your credentials using system properties, environment variables, or AwsCredentials.properties.
2. Package the spout and the sample (including all dependencies but excluding Storm itself) into one JAR file.
3. Deploy the package to Storm via the JAR file, e.g., `storm jar my-spout-sample.jar SampleTopology sample.properties RemoteMode` 

### Future Work

+ Replay failed records
+ Handle closed, split, and merged shards 

## Related Resources

[Amazon Kinesis Developer Guide](http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html)  
[Amazon Kinesis API Reference](http://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html)  

[Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client)  
[Amazon Kinesis Connector Library](https://github.com/awslabs/amazon-kinesis-connectors)

[aws-kinesis]: http://aws.amazon.com/kinesis/
