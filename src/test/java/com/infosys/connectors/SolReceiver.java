package com.infosys.connectors;

import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.sql.Timestamp;
import com.opencsv.CSVWriter;
import java.util.concurrent.CountDownLatch;

public class SolReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolReceiver.class);
    private static int NO_OF_MESSAGES = 150;
    private static File file = new File("/Volumes/SD/Solace/log/solreceiver"+ NO_OF_MESSAGES +".csv");
    private static Topic topic;
    private static JCSMPSession session;
    private static JCSMPProperties props = new JCSMPProperties();
    public static void main(String[] args) {

        props.setProperty(JCSMPProperties.HOST, "tcp://mr1qvxdm3zqyo3.messaging.solace.cloud:21056");
        props.setProperty(JCSMPProperties.USERNAME, "solace-cloud-client");
        props.setProperty(JCSMPProperties.PASSWORD, "ccqo8u2hdbp9hfqsc0l5o2ukfg");
        props.setProperty(JCSMPProperties.VPN_NAME, "msgvpn-20ta6idoiwsb");
        topic = JCSMPFactory.onlyInstance().createTopic("cb/topic1");
        try {
            FileWriter outputfile = new FileWriter(file);
            CSVWriter writer = new CSVWriter(outputfile, ';',
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);
            session = JCSMPFactory.onlyInstance().createSession(props);
            session.connect();
            final CountDownLatch latch = new CountDownLatch(1); // used for synchronizing b/w threads
            final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage msg) {
                    if (msg instanceof TextMessage) {
                        String [] message = new String[2];
                        message[0] = ((TextMessage) msg).getText();
                        //Date now = new Date();
                        String strDate = "" + new Timestamp(System.currentTimeMillis());
                        message[1]=strDate;
                        LOGGER.debug("TextMessage received: " + ((TextMessage) msg).getText());
                        /** Acknowledging a message. Might be debatable **/
                        writer.writeNext(message);
                        try{
                            writer.flush();
                        }catch (IOException ex){
                            ex.printStackTrace();
                        }
                        msg.ackMessage();
                    } else {
                        LOGGER.info("A non-Json message received. Ignored for now");
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    LOGGER.error("Consumer received exception", e);
                }

            });

            session.addSubscription(topic);
            LOGGER.info("Solace connected. Awaiting message...");
            cons.start();

            try {
                latch.await(); // block here until message received, and latch will flip
            } catch (InterruptedException e) {
                LOGGER.error("I was awoken while waiting");
            }
        }catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
