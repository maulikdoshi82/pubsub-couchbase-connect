package com.infosys.connectors.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * This is a config class handling couchbase dcp configuration.
 */
public class CouchbaseDcpConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDcpConfig.class);
    private static String CB_HOSTNAME;
    private static String CB_BUCKET;
    private static String CB_USERNAME;
    private static String CB_PASSWORD;
    private static String appVersion;
    private static String appName;

    public CouchbaseDcpConfig() {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String connectorConfigPath = rootPath + "connector.properties";
        String sinkConfigPath = rootPath + "couchbase-source.properties";

        try {
            Properties appProps = new Properties();
            appProps.load(new FileInputStream(connectorConfigPath));

            Properties connectorProps = new Properties();
            connectorProps.load(new FileInputStream(sinkConfigPath));

            appVersion = appProps.getProperty("version");
            appName = appProps.getProperty("name");

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

    public void setCbHostname(String cbHostname) {
        CB_HOSTNAME = cbHostname;
    }

    public String getCbBucket() {
        return CB_BUCKET;
    }

    public void setCbBucket(String cbBucket) {
        CB_BUCKET = cbBucket;
    }

    public String getCbUsername() {
        return CB_USERNAME;
    }

    public void setCbUsername(String cbUsername) {
        CB_USERNAME = cbUsername;
    }

    public String getCbPassword() {
        return CB_PASSWORD;
    }

    public void setCbPassword(String cbPassword) {
        CB_PASSWORD = cbPassword;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        CouchbaseDcpConfig.appVersion = appVersion;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        CouchbaseDcpConfig.appName = appName;
    }
}

