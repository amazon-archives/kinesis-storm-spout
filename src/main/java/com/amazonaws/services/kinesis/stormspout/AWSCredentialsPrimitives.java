package com.amazonaws.services.kinesis.stormspout;


/**
 * Encapsulates AWS Credentials fields required to bootstrap a CredentialsProvider
 */
public class AWSCredentialsPrimitives {

    private final String awsAccessKeyId;
    private final String awsSecretKey;
    private final String roleArn;
    private final String roleSessionName;

    public AWSCredentialsPrimitives(String awsAccessKeyId,
                                    String awsSecretKey){
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretKey = awsSecretKey;
        this.roleArn = null;
        this.roleSessionName = null;
    }

    public AWSCredentialsPrimitives(String awsAccessKeyId,
                                    String awsSecretKey,
                                    String roleArn,
                                    String roleSessionName){
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretKey = awsSecretKey;
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
    }

    public String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getRoleSessionName() {
        return roleSessionName;
    }
}
