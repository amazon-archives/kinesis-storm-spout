package com.amazonaws.services.kinesis.stormspout;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

/**
 * Unit tests for the AWSCredentialsPrimitives class.
 */
public class AWSCredentialsPrimitivesTest {

    @Test
    public void testMakeWithLongLivedAccessKeys(){

        String awsAccessKeyId = UUID.randomUUID().toString();
        String awsSecretKey = UUID.randomUUID().toString();

        AWSCredentialsPrimitives awsCredentialsPrimitives =
                AWSCredentialsPrimitives.makeWithLongLivedAccessKeys(awsAccessKeyId, awsSecretKey);

        // Test public properties are set correctly
        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertNull(awsCredentialsPrimitives.getRoleArn());
        assertNull(awsCredentialsPrimitives.getRoleSessionName());

        // Test that the correct AWSCredentialsProvider is made
        AWSCredentialsProvider awsCredentialsProvider = awsCredentialsPrimitives.makeNewAWSCredentialsProvider();

        assertThat(awsCredentialsProvider, instanceOf(AWSCredentialsProvider.class));
        assertEquals(awsAccessKeyId, awsCredentialsProvider.getCredentials().getAWSAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsProvider.getCredentials().getAWSSecretKey());
    }

    @Test
    public void testMakeWithEC2Role(){

        String roleArn = UUID.randomUUID().toString();
        String roleSessionName = UUID.randomUUID().toString();

        AWSCredentialsPrimitives awsCredentialsPrimitives =
                AWSCredentialsPrimitives.makeWithEC2Role(roleArn, roleSessionName);

        // Test public properties are set correctly
        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());
        assertNull(awsCredentialsPrimitives.getAwsAccessKeyId());
        assertNull(awsCredentialsPrimitives.getAwsSecretKey());

        // Test that the correct AWSCredentialsProvider (STSAssumeRoleSessionCredentialsProvider) is made
        AWSCredentialsProvider awsCredentialsProvider = awsCredentialsPrimitives.makeNewAWSCredentialsProvider();

        assertThat(awsCredentialsProvider, instanceOf(STSAssumeRoleSessionCredentialsProvider.class));
    }

    @Test
    public void testMakeWithEC2RoleAndLongLivedAccessKeys(){

        String awsAccessKeyId = UUID.randomUUID().toString();
        String awsSecretKey = UUID.randomUUID().toString();
        String roleArn = UUID.randomUUID().toString();
        String roleSessionName = UUID.randomUUID().toString();

        AWSCredentialsPrimitives awsCredentialsPrimitives =
                AWSCredentialsPrimitives.makeWithEC2RoleAndLongLivedAccessKeys(
                        roleArn, roleSessionName, awsAccessKeyId, awsSecretKey);

        // Test public properties are set correctly
        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());
        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());

        // Test that the correct AWSCredentialsProvider (STSAssumeRoleSessionCredentialsProvider) is made
        AWSCredentialsProvider awsCredentialsProvider = awsCredentialsPrimitives.makeNewAWSCredentialsProvider();

        assertThat(awsCredentialsProvider, instanceOf(STSAssumeRoleSessionCredentialsProvider.class));
    }
}
