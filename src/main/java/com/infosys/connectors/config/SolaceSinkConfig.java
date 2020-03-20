package com.infosys.connectors.config;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * This is a config class handling solace as a sink configuration.
 */

public class SolaceSinkConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceSinkConfig.class);
    private JCSMPProperties properties = new JCSMPProperties();
    private String TOPIC_NAME = "cb/topic1";
    private String RECEIVER_TYPE="queue"; //default is to queue
    private String QUEUE_NAME = "cbtest";

    public String getReceiverType() {
        return RECEIVER_TYPE;
    }

    public String getQueueName() {
        return QUEUE_NAME;
    }


    public void setProperties() {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String sinkConfigPath = rootPath + "solace-sink.properties";

        try {
            Properties solConProps = new Properties();
            solConProps.load(new FileInputStream(sinkConfigPath));

            RECEIVER_TYPE = solConProps.getProperty("sol.receiver.type");
            if(RECEIVER_TYPE.equalsIgnoreCase("queue"))
                QUEUE_NAME = solConProps.getProperty("sol.queues");
            else
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

    public String getTopicName(){
        return TOPIC_NAME;
    }

}
