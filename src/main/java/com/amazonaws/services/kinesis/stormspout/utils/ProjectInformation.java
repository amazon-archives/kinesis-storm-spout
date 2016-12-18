package com.amazonaws.services.kinesis.stormspout.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Use to get information from Maven pom file.
 */
public class ProjectInformation {

    private final static String PROJECT_PROPERTIES = "project.properties";
    private static Properties props;

    protected static Properties getProperties() {
        if (props == null) {
            InputStream resourceAsStream = ProjectInformation.class.getClassLoader().getResourceAsStream(PROJECT_PROPERTIES);
            props = new Properties();
            try {
                props.load(resourceAsStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return props;
    }

    /**
     * Get the project version number from Maven pom file.
     * @return project version
     */
    public static String getVersion() {
        return getProperties().getProperty("version");
    }

    /**
     * Get the project groupId from Maven pom file.
     * @return groupId
     */
    public static String getgroupId() {
        return getProperties().getProperty("groupId");
    }

    /**
     * Get the project artifactId from Maven pom file.
     * @return artifactId
     */
    public static String getArtifactId() {
        return getProperties().getProperty("artifactId");
    }
}
