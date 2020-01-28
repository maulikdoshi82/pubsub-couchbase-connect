package com.infosys.connectors.clients;

import com.infosys.connectors.config.SolaceSourceConfig;
import com.solacesystems.jcsmp.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SolaceReceiverClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceReceiverClient.class);

    SolaceSourceConfig solConfig = new SolaceSourceConfig();
    private Topic topic;
    private Queue queue;
    private JCSMPSession session;

    public Topic getTopicName() {
        return topic;
    }

    public Queue getQueueName() {
        return queue;
    }

    public JCSMPSession getSession() throws JCSMPException {
        solConfig.setProperties();
        LOGGER.info(solConfig.getReceiverType());
        if (solConfig.getReceiverType().equalsIgnoreCase("QUEUE"))
            queue = JCSMPFactory.onlyInstance().createQueue((solConfig.getQueueName()));
        else
            topic = JCSMPFactory.onlyInstance().createTopic(solConfig.getTopicName());
        session = JCSMPFactory.onlyInstance().createSession(solConfig.getProperties());
        LOGGER.info("Session Connected");
        return session;
    }

}
