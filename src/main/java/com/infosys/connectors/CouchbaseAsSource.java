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
import com.infosys.connectors.clients.CouchbaseDcpClient;
import com.infosys.connectors.clients.SolaceProducerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.CompletableSubscriber;
import rx.Subscription;

/**
 * This sample connector is based on java-dcp-client provided by couchbase
 * The class connects Solace and Couchbase. It also registers a DCP Client to receive feed from Couchbase and push it to Solace.
 * It does handle Solace shutdown. Queueing of Couchbase Messages is still pending.
 */
public class CouchbaseAsSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseAsSource.class);
    private static int countRecords = 0;
    public static void main(String[] args) {
        SolaceProducerClient solClient = new SolaceProducerClient();
        Client cbClient = new CouchbaseDcpClient().setClient();
        boolean SolConnect = solClient.startSession();
        // Start solace session
        if (SolConnect) {
            try {
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
                            countRecords++;
                            if (DcpMutationMessage.revisionSeqno(event) == 1) {
                                LOGGER.debug("Received New Record" + JsonObject
                                        .fromJson(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)));
                                try {
                                    solClient.sendMessage(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                                    LOGGER.debug("Sent Message::"
                                            + DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            } else {
                                LOGGER.debug("Updated Record" + JsonObject
                                        .fromJson(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)));
                                try {
                                    solClient.sendMessage(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                                    LOGGER.debug("Sent Message::"
                                            + DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            }
                        } else if (DcpDeletionMessage.is(event)) {
                            LOGGER.debug("Deleted Record: " + DcpDeletionMessage.keyString(event));
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
                LOGGER.trace("Session State" + cbClient.sessionState());
                // Start streaming on all partitions
                cbClient.startStreaming().await();
                LOGGER.info("Couchbase & Solace are connected. Awaiting Stream");

            } catch (Exception ex) {
                LOGGER.info("An exception occurred:: " + ex.getMessage());
                if (cbClient.sessionState().equals(true)) {
                    cbClient.stopStreaming();
                    cbClient.disconnect();
                }
                solClient.closeSession();
                System.exit(0);
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    LOGGER.info("Initiating Connector shutdown as SIGINT is called.");
                    if (cbClient.sessionState().equals(true)) {
                        cbClient.stopStreaming();
                        cbClient.disconnect();
                    }
                    LOGGER.info("Number of Records Processed::" + countRecords);
                    solClient.closeSession();
                    LOGGER.info("Connector shutdown. All clients disconnected");
                }
            });
        } else {
            LOGGER.info("Solace is not connected. Shutting down connector");
        }
    }
}
