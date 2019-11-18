/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.infosys.connectors;

import java.util.concurrent.CountDownLatch;

import com.couchbase.client.java.document.json.JsonObject;
import com.infosys.connectors.cbsink.CouchbaseAsSink;
import com.infosys.connectors.cbsink.SolaceSourceConfig;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceAsSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceAsSource.class);

    public static void main(String[] args) throws JCSMPException {

        CouchbaseAsSink cbSink = new CouchbaseAsSink();
        SolaceSourceConfig solConfig = new SolaceSourceConfig();
        solConfig.setProperties();
        final Topic topic = JCSMPFactory.onlyInstance().createTopic(solConfig.getTopic());
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(solConfig.getProperties());
        session.connect();
        cbSink.startCluster();

        final CountDownLatch latch = new CountDownLatch(2); // used for
        // synchronizing b/w threads
        /** Only supports JsonMessage for now. Key field should be added in the message */
        final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                if (msg instanceof TextMessage) {
                    LOGGER.info("TextMessage received: " + ((TextMessage)msg).getText());
                    JsonObject jObj = JsonObject.fromJson(((TextMessage) msg).getText());
                    cbSink.upsertDocument("test", jObj);
                    /** Acknowledging a message. Might be debatable **/
                    msg.ackMessage();
                    LOGGER.debug("Document saved in Couchbase::" + jObj.toString());
                } else {
                    LOGGER.info("A non-Json message received. Ignored for now");
                }
            }

            @Override
            public void onException(JCSMPException e) {
                LOGGER.error("Consumer received exception",e);
                latch.countDown();  // unblock main thread
            }
        });
        session.addSubscription(topic);
        LOGGER.info("Solace & Couchbase are connected. Awaiting message...");
        cons.start();

        try {
            latch.await(); // block here until message received, and latch will flip
        } catch (InterruptedException e) {
            LOGGER.error("I was awoken while waiting");
        }
        // Close consumer
        cons.close();
        cbSink.shutDownConnector();
        session.closeSession();
    }
}