package com.infosys.connectors.config;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SolaceSourceConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceSourceConfig.class);
    static JCSMPProperties properties = new JCSMPProperties();
    static String TOPIC_NAME = "cb/topic1";

    public void setProperties() {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String sinkConfigPath = rootPath + "solace-source.properties";

        try {
            Properties solConProps = new Properties();
            solConProps.load(new FileInputStream(sinkConfigPath));

            TOPIC_NAME = solConProps.getProperty("sol.topics");
            properties.setProperty(JCSMPProperties.HOST, solConProps.getProperty("sol.host"));
            properties.setProperty(JCSMPProperties.USERNAME, solConProps.getProperty("sol.username"));
            properties.setProperty(JCSMPProperties.PASSWORD, solConProps.getProperty("sol.password"));
            properties.setProperty(JCSMPProperties.VPN_NAME, solConProps.getProperty("sol.vpn_name"));
        }
        catch (IOException ex) {
                LOGGER.error("Exception " + ex.getMessage() + " Occurred. Shutting down the connector");
        }

    }

    public JCSMPProperties getProperties(){
        return properties;
    }

    public String getTopic(){
        return TOPIC_NAME;
    }

    public void setTopic(String topic){
        TOPIC_NAME = topic;
    }
}
