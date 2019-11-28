package com.infosys.connectors;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.java.document.json.JsonObject;
import com.infosys.connectors.clients.SolaceProducerClient;
import com.infosys.connectors.clients.CouchbaseDcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.CompletableSubscriber;
import rx.Subscription;

/**
 * This example starts from the current point in time and publishes every change that happens.
 * This example is based on java-dcp-client provided by couchbase
 */
public class CouchbaseAsSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseAsSource.class);

    public static void main(String[] args) {
        SolaceProducerClient solClient = new SolaceProducerClient();
        Client cbClient = new CouchbaseDcpClient().setClient();

        try {
            // Start solace session
            solClient.startSession();

            // If we are in a rollback scenario, rollback the partition and restart the stream.
            cbClient.controlEventHandler(new ControlEventHandler() {
                @Override
                public void onEvent(final ChannelFlowController flowController, final ByteBuf event) {
                    if (DcpSnapshotMarkerRequest.is(event)) {
                        flowController.ack(event);
                    }
                    if (RollbackMessage.is(event)) {
                        final short partition = RollbackMessage.vbucket(event);
                        cbClient.rollbackAndRestartStream(partition, RollbackMessage.seqno(event))
                                .subscribe(new CompletableSubscriber() {
                                    @Override
                                    public void onCompleted() {
                                        LOGGER.info("Rollback for partition " + partition + " complete!");
                                    }

                                    @Override
                                    public void onError(Throwable e) {
                                        LOGGER.error("Rollback for partition " + partition + " failed!");
                                        e.printStackTrace();
                                    }

                                    @Override
                                    public void onSubscribe(Subscription d) {
                                    }
                                });
                    }
                    event.release();
                }
            });

            // Send the Mutations to Solace
            cbClient.dataEventHandler(new DataEventHandler() {
                @Override
                public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                    if (DcpMutationMessage.is(event)) {
                        if (DcpMutationMessage.revisionSeqno(event) == 1) {
                            LOGGER.info("New Record" + JsonObject.fromJson(
                                    DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)));
                            try {
                                solClient.sendMessage(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                                LOGGER.info("Sent Message::" + DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        } else {
                            LOGGER.info("Updated Record" + JsonObject.fromJson(
                                    DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)));
                            try {
                                //content doesn't have the key.
                                //String message = "Key" + DcpMutationMessage.key(event);
                                String message = DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8);
                                solClient.sendMessage(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                                LOGGER.info("Sent Message::" + DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    } else if (DcpDeletionMessage.is(event)) {
                        LOGGER.info("Deleted Record: " + DcpDeletionMessage.keyString(event));
                        try {
                            solClient.sendMessage("DeletedRecordId::" + DcpDeletionMessage.keyString(event));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    event.release();
                }
            });

            // Connect the sockets
            cbClient.connect().await();

            // Initialize the state (start now, never stop)
            cbClient.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();

            // Start streaming on all partitions
            cbClient.startStreaming().await();
            LOGGER.info("Awaiting Stream");

        } catch (Exception ex) {
            cbClient.stopStreaming();
            cbClient.disconnect();
            solClient.closeSession();
        }
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                LOGGER.info("Initiating Connector shutdown as SIGINT is called.");
                cbClient.stopStreaming();
                cbClient.disconnect();
                solClient.closeSession();
                LOGGER.info("Connector shutdown. All clients disconnected");
            }
        });
    }
}
