package com.infosys.connectors.cbsource;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceAsSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceAsSink.class);
    private static XMLMessageProducer prod;
    private static Topic topic;
    private static JCSMPSession session;
    SolaceSinkConfig scconfig = new SolaceSinkConfig();

    public SolaceAsSink(){
        scconfig.setProperties();
    }

    public void startSession() throws JCSMPException {
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
                        new Object[]{messageID, timestamp, e});
            }
        });
        topic = JCSMPFactory.onlyInstance().createTopic(scconfig.getTopic());
        LOGGER.debug("Solace Connected. You can now send message to topic: " + topic.getName());

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




