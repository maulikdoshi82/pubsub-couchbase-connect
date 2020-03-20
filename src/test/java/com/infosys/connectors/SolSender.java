package com.infosys.connectors;

import com.solacesystems.jcsmp.*;
import org.json.simple.JSONObject;
import java.sql.Timestamp;

public class SolSender {
    private static XMLMessageProducer prod;
    private static Topic topic;
    private static JCSMPSession session;
    private static JCSMPProperties props = new JCSMPProperties();
    private static int NO_OF_MESSAGES = 150;

    public static void main(String[] args) {

        props.setProperty(JCSMPProperties.HOST, "tcp://mr1qvxdm3zqyo3.messaging.solace.cloud:21056");
        props.setProperty(JCSMPProperties.USERNAME, "solace-cloud-client");
        props.setProperty(JCSMPProperties.PASSWORD, "ccqo8u2hdbp9hfqsc0l5o2ukfg");
        props.setProperty(JCSMPProperties.VPN_NAME, "msgvpn-20ta6idoiwsb");

        try {
            session = JCSMPFactory.onlyInstance().createSession(props);
            session.connect();
            prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
                @Override
                public void responseReceived(String messageID) {
                    System.out.println("Producer received response for msg: " + messageID);
                }

                @Override
                public void handleError(String messageID, JCSMPException e, long timestamp) {
                    System.out.println("Producer received error for msg:" + messageID + timestamp + e);
                }
            });

            topic = JCSMPFactory.onlyInstance().createTopic("cb/topic1");

            for (int i = 0; i < NO_OF_MESSAGES; i++) {
                JSONObject obj = new JSONObject();
                obj.put("key", ("" + i)); // This is an important field.
                obj.put("name", "foo");
                obj.put("num", new Integer(100));
                obj.put("balance", new Double(1000.21));
                obj.put("is_vip", new Boolean(true));
                obj.put("sent_time", ("" + new Timestamp(System.currentTimeMillis())));

                TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                msg.setText(obj.toJSONString());
                //System.out.println("Message Sent");
                prod.send(msg, topic);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
