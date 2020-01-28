package com.infosys.connectors.clients;

import com.infosys.connectors.config.SolaceSinkConfig;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceProducerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceProducerClient.class);
    private static XMLMessageProducer prod;
    private static Topic topic;
    private static Queue queue;
    private static JCSMPSession session;
    SolaceSinkConfig scconfig = new SolaceSinkConfig();

    public SolaceProducerClient(){
        scconfig.setProperties();
    }

    public boolean startSession()  {
        try{
        session = JCSMPFactory.onlyInstance().createSession(scconfig.getProperties());
        session.connect();
        prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                LOGGER.info("Producer received response for msg: " + messageID);
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                LOGGER.error("Producer received error for msg:",
                        messageID, timestamp, e);
                if(LOGGER.isTraceEnabled())
                    e.printStackTrace();
            }
        });
        if(scconfig.getReceiverType().equalsIgnoreCase("TOPIC"))
            topic = JCSMPFactory.onlyInstance().createTopic(scconfig.getTopicName());
        else
            queue = JCSMPFactory.onlyInstance().createQueue(scconfig.getQueueName());
        LOGGER.debug("Solace is connected. Awaiting message");
    } catch (JCSMPException ConnEx){
        LOGGER.error("Connection error occurred " + ConnEx.getLocalizedMessage());
        return false;
    }
        return true;
    }


    public void closeSession(){
        session.closeSession();
    }

    public void sendMessage(String text) throws JCSMPException{
        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        msg.setText(text);
        prod.send(msg,topic);
        LOGGER.debug("Message " +  msg.getText() + " Sent on Topic " + topic.getName());
        msg.reset();
    }
}




