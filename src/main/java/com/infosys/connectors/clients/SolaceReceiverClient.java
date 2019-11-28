package com.infosys.connectors.clients;

import com.infosys.connectors.config.SolaceSourceConfig;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;

public class SolaceReceiverClient {
    SolaceSourceConfig solConfig = new SolaceSourceConfig();

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        SolaceReceiverClient.topic = topic;
    }

    private static Topic topic;
    private static JCSMPSession session;

    public JCSMPSession getSession() throws JCSMPException {
        solConfig.setProperties();
        topic = JCSMPFactory.onlyInstance().createTopic(solConfig.getTopic());
        session = JCSMPFactory.onlyInstance().createSession(solConfig.getProperties());
        return session;
    }

}
