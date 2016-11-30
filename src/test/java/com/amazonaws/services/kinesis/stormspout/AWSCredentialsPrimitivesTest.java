package com.amazonaws.services.kinesis.stormspout;

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Unit tests for the AWSCredentialsPrimitives class.
 */
public class AWSCredentialsPrimitivesTest {

    @Test
    public void testInstantiateWithoutRoleInfo(){

        String awsAccessKeyId = UUID.randomUUID().toString();
        String awsSecretKey = UUID.randomUUID().toString();

        AWSCredentialsPrimitives awsCredentialsPrimitives = new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey);

        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertNull(awsCredentialsPrimitives.getRoleArn());
        assertNull(awsCredentialsPrimitives.getRoleSessionName());
    }

    @Test
    public void testInstantiateWithRoleInfo(){

        String awsAccessKeyId = UUID.randomUUID().toString();
        String awsSecretKey = UUID.randomUUID().toString();
        String roleArn = UUID.randomUUID().toString();
        String roleSessionName = UUID.randomUUID().toString();

        AWSCredentialsPrimitives awsCredentialsPrimitives = new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey,
                roleArn, roleSessionName);

        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testInstantiateWithNullRoleInfo(){

        String awsAccessKeyId = UUID.randomUUID().toString();
        String awsSecretKey = UUID.randomUUID().toString();
        String roleArn = UUID.randomUUID().toString();;
        String roleSessionName = null;

        AWSCredentialsPrimitives awsCredentialsPrimitives = new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey,
                roleArn, roleSessionName);

        // test w\ single role field null
        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());

        // test w\ both role field null
        roleArn = null;
        awsCredentialsPrimitives = new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey,
                roleArn, roleSessionName);
        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());

        // test w\ empty string for roleArn
        roleArn = "";
        awsCredentialsPrimitives = new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey,
                roleArn, roleSessionName);
        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());

        // test w\ bad string for roleArn
        roleArn = "    ";
        awsCredentialsPrimitives = new AWSCredentialsPrimitives(awsAccessKeyId, awsSecretKey,
                roleArn, roleSessionName);
        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());
    }
}
