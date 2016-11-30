package com.amazonaws.services.kinesis.stormspout;

import clojure.lang.Obj;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for the KinesisHelper class.
 */
public class KinesisHelperTest {

    @Test
    /**
     * Tests that the correct AWSCredentialsProvider is returned.
     *
     * Exercises internal serialization/deserialization of the class.
     */
    public void testGetKinesisCredsProvider(){

        String streamName = "streamName";
        String awsAccessKeyId = UUID.randomUUID().toString();
        String awsSecretKey = UUID.randomUUID().toString();
        String roleArn = UUID.randomUUID().toString();
        String roleSessionName = UUID.randomUUID().toString();
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        Region region = Region.getRegion(Regions.US_EAST_1);

        // Instantiate for AWSCredentialsProvider
        //

        // NOTE: Supplying only awsAccessKeyId, awsSecretKey
        AWSCredentialsPrimitives awsCredentialsPrimitives =
                AWSCredentialsPrimitives.makeWithLongLivedAccessKeys(awsAccessKeyId, awsSecretKey);

        KinesisHelper kinesisHelper = new KinesisHelper(streamName,
                awsCredentialsPrimitives,
                clientConfiguration,
                region);

        // Should return the base AWSCredentialsProvider
        AWSCredentialsProvider awsCredentialsProvider = kinesisHelper.getKinesisCredsProvider();

        assertThat(awsCredentialsProvider, instanceOf(AWSCredentialsProvider.class));
        assertEquals(awsAccessKeyId, awsCredentialsProvider.getCredentials().getAWSAccessKeyId());
        assertEquals(awsSecretKey, awsCredentialsProvider.getCredentials().getAWSSecretKey());
        assertFalse(awsCredentialsPrimitives.useEC2Role());
        assertFalse(awsCredentialsPrimitives.useOverrideAccessKeys());


        // Instantiate for STSAssumeRoleSessionCredentialsProvider
        //

        // NOTE: Supplying awsAccessKeyId, awsSecretKey, roleArn, roleSessionName
        awsCredentialsPrimitives = AWSCredentialsPrimitives.makeWithEC2RoleAndLongLivedAccessKeys (
                roleArn, roleSessionName, awsAccessKeyId, awsSecretKey);

        kinesisHelper = new KinesisHelper(streamName,
                awsCredentialsPrimitives,
                clientConfiguration,
                region);

        // Should return the base STSAssumeRoleSessionCredentialsProvider
        awsCredentialsProvider = kinesisHelper.getKinesisCredsProvider();

        assertThat(awsCredentialsProvider, instanceOf(STSAssumeRoleSessionCredentialsProvider.class));
    }
}
