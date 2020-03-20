package com.infosys.connectors.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * This is a config class handling couchbase configuration.
 */

public class CouchbaseConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseConfig.class);
    private String CB_HOSTNAME;
    private String CB_BUCKET;
    private String CB_USERNAME;
    private String CB_PASSWORD;
    private String appVersion;
    private String appName;
    private int NO_OF_RETRY;

    private static int CB_CONN_TIMEOUT;


    public CouchbaseConfig() {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String connectorConfigPath = rootPath + "connector.properties";
        String sinkConfigPath = rootPath + "couchbase-sink.properties";

        try {
            Properties appProps = new Properties();
            appProps.load(new FileInputStream(connectorConfigPath));

            Properties connectorProps = new Properties();
            connectorProps.load(new FileInputStream(sinkConfigPath));

            appVersion = appProps.getProperty("version");
            appName = appProps.getProperty("name");
            NO_OF_RETRY = Integer.parseInt(connectorProps.getProperty("connection.retry"));
            CB_CONN_TIMEOUT = Integer.parseInt(connectorProps.getProperty("connection.timeout.ms"));
            CB_HOSTNAME = connectorProps.getProperty("connection.cluster_address");
            CB_BUCKET = connectorProps.getProperty("connection.bucket");
            CB_USERNAME = connectorProps.getProperty("connection.username");
            CB_PASSWORD = connectorProps.getProperty("connection.password");
        } catch (Exception ex) {
            LOGGER.error("Exception " + ex.getMessage() + " Occurred. Shutting down the connector");
        }
    }

    public String getCbHostname() {
        return CB_HOSTNAME;
    }

    public String getCbBucket() {
        return CB_BUCKET;
    }

    public String getCbUsername() {
        return CB_USERNAME;
    }

    public String getCbPassword() {
        return CB_PASSWORD;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public String getAppName() {
        return appName;
    }

    public int getNoOfRetry() {
        return NO_OF_RETRY;
    }

    public int getCbConnTimeout() {
        return CB_CONN_TIMEOUT;
    }
}

