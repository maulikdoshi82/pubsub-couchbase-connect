package com.infosys.connectors.cbsink;

import com.solacesystems.jcsmp.JCSMPProperties;

public class SolaceSourceConfig {
    static JCSMPProperties properties = new JCSMPProperties();
    static String HOST_NAME = "tcp://mr1qvxdm3zqyo3.messaging.solace.cloud:21056";
    static String USER_NAME = "solace-cloud-client";
    static String PASSWORD = "44ok9n13tqb6t6ngth5q15e8gr";
    static String VPN_NAME = "msgvpn-1u6o37qngonn";
    static String TOPIC_NAME = "cb/topic1";

    public void setProperties() {
        properties.setProperty(JCSMPProperties.HOST, HOST_NAME);
        properties.setProperty(JCSMPProperties.USERNAME, USER_NAME);
        properties.setProperty(JCSMPProperties.PASSWORD, PASSWORD);
        properties.setProperty(JCSMPProperties.VPN_NAME, VPN_NAME);
    }

    public void setProperties(String host, String usr, String pwd, String vpn) {
        properties.setProperty(JCSMPProperties.HOST, host);
        properties.setProperty(JCSMPProperties.USERNAME, usr);
        properties.setProperty(JCSMPProperties.PASSWORD, pwd);
        properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
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
