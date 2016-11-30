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

        AWSCredentialsPrimitives awsCredentialsPrimitives =
                AWSCredentialsPrimitives.makeWithLongLivedAccessKeys(awsAccessKeyId, awsSecretKey);

        assertEquals(awsAccessKeyId, awsCredentialsPrimitives.getAwsAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsPrimitives.getAwsSecretKey());
        assertNull(awsCredentialsPrimitives.getRoleArn());
        assertNull(awsCredentialsPrimitives.getRoleSessionName());
        assertFalse(awsCredentialsPrimitives.useEC2Role());
    }

    @Test
    public void testInstantiateWithEC2RoleInfo(){

        String roleArn = UUID.randomUUID().toString();
        String roleSessionName = UUID.randomUUID().toString();

        AWSCredentialsPrimitives awsCredentialsPrimitives =
                AWSCredentialsPrimitives.makeWithEC2Role(roleArn, roleSessionName);

        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());
        assertTrue(awsCredentialsPrimitives.useEC2Role());
    }

    @Test
    public void testInstantiateWithEC2RoleInfoAndLongLivedAccessKeys(){

        String awsAccessKeyId = UUID.randomUUID().toString();
        String awsSecretKey = UUID.randomUUID().toString();
        String roleArn = UUID.randomUUID().toString();
        String roleSessionName = UUID.randomUUID().toString();

        AWSCredentialsPrimitives awsCredentialsPrimitives =
                AWSCredentialsPrimitives.makeWithEC2RoleAndLongLivedAccessKeys(
                        roleArn, roleSessionName, awsAccessKeyId, awsSecretKey);

        assertEquals(roleArn, awsCredentialsPrimitives.getRoleArn());
        assertEquals(roleSessionName, awsCredentialsPrimitives.getRoleSessionName());
        assertFalse(awsCredentialsPrimitives.useEC2Role());
    }
}
