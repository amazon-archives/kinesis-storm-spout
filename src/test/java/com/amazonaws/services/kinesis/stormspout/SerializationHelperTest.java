package com.amazonaws.services.kinesis.stormspout;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by christian on 12/15/16.
 */
public class SerializationHelperTest {

    private final static String AWS_PROFILE_FILE = "aws_credentials.test";

    // Don't worry, are random
    private final static String ACCESS_KEY_ID = "ASIAJRT6JJTB43V33CWA";
    private final static String SECRET_KEY_ID = "mPOaL73gmuK5uv7tJiXLMdccfRgvQOwcZTkfJDK1";

    private static String getCredentialsTestFile(String name) {
        URL url = SerializationHelperTest.class.getResource("/" + name);
        return url.getFile();
    }

    /**
     * Test method for {@link com.amazonaws.services.kinesis.stormspout.SerializationHelper}.
     */
    @Test
    public void testSerializeAndDeserializeAWSCredentialsProviderUsingBasic() throws Exception {
        // Given
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_KEY_ID);
        final AWSCredentialsProvider credentialsProviderExpected = new AWSStaticCredentialsProvider(awsCreds);
        AWSCredentialsProvider credentialsProviderActual = null;

        // when SerializeAnd and Deserialize
        final byte[] serializedCredentialsProvider = SerializationHelper.kryoSerializeObject(credentialsProviderExpected);
        credentialsProviderActual = (AWSCredentialsProvider) SerializationHelper.kryoDeserializeObject(serializedCredentialsProvider);

        // then
        assertTrue(credentialsProviderActual != null);
        assertEquals(credentialsProviderExpected.getClass().getCanonicalName(), credentialsProviderActual.getClass().getCanonicalName());
        assertEquals(credentialsProviderExpected.getClass().toString(), credentialsProviderActual.getClass().toString());

        assertEquals(credentialsProviderExpected.getCredentials().getAWSSecretKey(), credentialsProviderActual.getCredentials().getAWSSecretKey());
        assertEquals(credentialsProviderExpected.getCredentials().getAWSAccessKeyId(), credentialsProviderActual.getCredentials().getAWSAccessKeyId());

    }

    /**
     * Test method for {@link com.amazonaws.services.kinesis.stormspout.SerializationHelper}.
     */
    @Test
    public void testSerializeAndDeserializeAWSCredentialsProviderUsingProfile() throws Exception {
        // Given.
        final ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(getCredentialsTestFile(AWS_PROFILE_FILE));
        final ProfileCredentialsProvider credentialsProviderExpected = new ProfileCredentialsProvider(profilesConfigFile, "default");
        AWSCredentialsProvider credentialsProviderActual = null;

        // when SerializeAnd and Deserialize
        final byte[] serializedCredentialsProvider = SerializationHelper.kryoSerializeObject(credentialsProviderExpected);
        credentialsProviderActual = (AWSCredentialsProvider) SerializationHelper.kryoDeserializeObject(serializedCredentialsProvider);

        // then
        assertTrue(credentialsProviderActual != null);
        assertEquals(credentialsProviderExpected.getClass().getCanonicalName(), credentialsProviderActual.getClass().getCanonicalName());
        assertEquals(credentialsProviderExpected.getClass().toString(), credentialsProviderActual.getClass().toString());

        assertEquals(credentialsProviderExpected.getCredentials().getAWSSecretKey(), credentialsProviderActual.getCredentials().getAWSSecretKey());
        assertEquals(credentialsProviderExpected.getCredentials().getAWSAccessKeyId(), credentialsProviderActual.getCredentials().getAWSAccessKeyId());
    }

    /**
     * Test method for {@link com.amazonaws.services.kinesis.stormspout.SerializationHelper}.
     */
    @Test
    public void testSerializeAndDeserializeClientConfiguration() throws Exception {
        // Given.
        final ClientConfiguration clientConfigurationExpected = new ClientConfiguration();
        ClientConfiguration clientConfigurationActual = null;

        // when SerializeAnd and Deserialize
        final byte[] serializedCredentialsProvider = SerializationHelper.kryoSerializeObject(clientConfigurationExpected);
        clientConfigurationActual = (ClientConfiguration) SerializationHelper.kryoDeserializeObject(serializedCredentialsProvider);

        // then
        assertTrue(clientConfigurationActual != null);
        assertEquals(clientConfigurationExpected.getClass().getCanonicalName(), clientConfigurationActual.getClass().getCanonicalName());
        assertEquals(clientConfigurationExpected.getClass().toString(), clientConfigurationActual.getClass().toString());

        assertEquals(clientConfigurationExpected.getUserAgentPrefix(), clientConfigurationActual.getUserAgentPrefix());
        assertEquals(clientConfigurationExpected.getUserAgentSuffix(), clientConfigurationActual.getUserAgentSuffix());
    }

    /**
     * Test method for {@link com.amazonaws.services.kinesis.stormspout.SerializationHelper}.
     */
    @Test
    public void testSerializeAndDeserializeRegion() throws Exception {
        // Given.
        final Regions region = Regions.US_WEST_2;
        final Region regionExpected = Region.getRegion(region);
        Region regionActual = null;

        // when SerializeAnd and Deserialize
        final byte[] serializedCredentialsProvider = SerializationHelper.kryoSerializeObject(regionExpected);
        regionActual = (Region) SerializationHelper.kryoDeserializeObject(serializedCredentialsProvider);

        // then
        assertTrue(regionActual != null);
        assertEquals(regionExpected.getClass().getCanonicalName(), regionActual.getClass().getCanonicalName());
        assertEquals(regionExpected.getClass().toString(), regionActual.getClass().toString());

        assertEquals(regionExpected.getName(), regionActual.getName());
        assertEquals(regionExpected.getDomain(), regionActual.getDomain());
    }
}