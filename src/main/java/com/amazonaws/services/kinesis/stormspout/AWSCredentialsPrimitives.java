package com.amazonaws.services.kinesis.stormspout;


/**
 * Encapsulates AWS Credentials fields required to bootstrap a CredentialsProvider
 */
public class AWSCredentialsPrimitives {

    private final String awsAccessKeyId;
    private final String awsSecretKey;
    private final String roleArn;
    private final String roleSessionName;

    /**
     *
     * @param awsAccessKeyId The AWS Access Key ID used for authentication with Kinesis
     * @param awsSecretKey The AWS Secret Key used for authentication with Kinesis
     */
    public AWSCredentialsPrimitives(String awsAccessKeyId,
                                    String awsSecretKey){
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretKey = awsSecretKey;
        this.roleArn = null;
        this.roleSessionName = null;
    }

    /**
     *
     * @param awsAccessKeyId The AWS Access Key ID used for authentication with Kinesis.
     * @param awsSecretKey The AWS Secret Key used for authentication with Kinesis.
     * @param roleArn The AWS Role ARN used for STS-based authentication with Kinesis.
     * @param roleSessionName The AWS Role Session Name used for STS-based authentication with Kinesis.
     */
    public AWSCredentialsPrimitives(String awsAccessKeyId,
                                    String awsSecretKey,
                                    String roleArn,
                                    String roleSessionName){

        if((roleArn == null || roleArn.trim().isEmpty()) ||
                (roleSessionName == null || roleSessionName.trim().isEmpty())){
            throw new IllegalArgumentException("roleArn and roleSessionName but not be empty or null.");
        }

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
