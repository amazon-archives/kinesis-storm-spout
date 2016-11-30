package com.amazonaws.services.kinesis.stormspout;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;

/**
 * Encapsulates AWS Credentials fields required to bootstrap a CredentialsProvider
 */
public class AWSCredentialsPrimitives {

    private final String awsAccessKeyId;
    private final String awsSecretKey;
    private final String roleArn;
    private final String roleSessionName;
    private final boolean useEC2Role;
    private final boolean useOverrideAccessKeys;

    /**
     *
     * @param awsAccessKeyId The AWS Access Key ID used for authentication with Kinesis.
     * @param awsSecretKey The AWS Secret Key used for authentication with Kinesis.
     * @return A new AWSCredentialsPrimitives instantiated with the specified long-lived access keys.
     */
    public static AWSCredentialsPrimitives makeWithLongLivedAccessKeys(String awsAccessKeyId, String awsSecretKey){
        return new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey,null,null,false, false);
    }

    /**
     *
     * @param roleArn The AWS Role ARN used for IAM Role based authentication with Kinesis.
     * @param roleSessionName The AWS Role Session Name used for IAM Role based authentication with Kinesis.
     * @return A new AWSCredentialsPrimitives instantiated with the specified IAM Role info.
     */
    public static AWSCredentialsPrimitives makeWithEC2Role(String roleArn, String roleSessionName){
        return new AWSCredentialsPrimitives(null, null, roleArn, roleSessionName, true, false);
    }

    /**
     *
     * @param roleArn The AWS Role ARN used for IAM Role based authentication with Kinesis.
     * @param roleSessionName The AWS Role Session Name used for IAM Role based authentication with Kinesis.
     * @param awsAccessKeyId The AWS Access Key ID used for authentication with Kinesis.
     * @param awsSecretKey The AWS Secret Key used for authentication with Kinesis.
     * @return A new AWSCredentialsPrimitives instantiated with the specified IAM Role info and long-lived access keys.
     */
    public static AWSCredentialsPrimitives makeWithEC2RoleAndLongLivedAccessKeys(String roleArn, String roleSessionName,
                                                                                 String awsAccessKeyId, String awsSecretKey){
        return new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey, roleArn, roleSessionName, true, true);
    }

    /**
     *
     * @param awsAccessKeyId The AWS Access Key ID used for authentication with Kinesis.
     * @param awsSecretKey The AWS Secret Key used for authentication with Kinesis.
     * @param roleArn The AWS Role ARN used for IAM Role based authentication with Kinesis.
     * @param roleSessionName The AWS Role Session Name used for IAM Role based authentication with Kinesis.
     * @param useEC2Role Signals whether to use the underlying EC2 Instance Role for authentication with Kinesis.
     * @param useOverrideAccessKeys For EC2 Role, signals whether to use the specified Access Keys.
     */
    private AWSCredentialsPrimitives(String awsAccessKeyId, String awsSecretKey, String roleArn, String roleSessionName,
                                     boolean useEC2Role, boolean useOverrideAccessKeys){

        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretKey = awsSecretKey;
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        this.useEC2Role = useEC2Role;
        this.useOverrideAccessKeys = useOverrideAccessKeys;
    }

    public AWSCredentialsProvider makeNewAWSCredentialsProvider(){

        AWSCredentialsProvider awsCredentialsProvider;

        if(useEC2Role){
            // if useEC2Role is specified create an STSAssumeRoleSessionCredentialsProvider.
            STSAssumeRoleSessionCredentialsProvider.Builder stsBuilder =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName);

            if(useOverrideAccessKeys) {
                // use the supplied access keys, if specified. this is useful for testing local (not on an EC2 instance).
                stsBuilder.withLongLivedCredentials(new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        return awsAccessKeyId;
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return awsSecretKey;
                    }
                });
            }

            awsCredentialsProvider = stsBuilder.build();

        }else{
            // otherwise, create the base AWSCredentialsProvider using the specfied access keys.
            awsCredentialsProvider = new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new AWSCredentials() {
                        @Override
                        public String getAWSAccessKeyId() {
                            return awsAccessKeyId;
                        }

                        @Override
                        public String getAWSSecretKey() {
                            return awsSecretKey;
                        }
                    };
                }

                @Override
                public void refresh() {
                }
            };
        }

        return awsCredentialsProvider;
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
