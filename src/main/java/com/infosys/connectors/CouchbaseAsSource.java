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
import com.infosys.connectors.cbsource.CouchbaseSourceConfig;
import com.infosys.connectors.cbsource.SolaceAsSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.CompletableSubscriber;
import rx.Subscription;

import java.util.concurrent.TimeUnit;

/**
 * This example starts from the current point in time and publishes every change that happens.
 * This example is based on java-dcp-client provided by couchbase
 */
public class CouchbaseAsSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseAsSource.class);
    private static Client client;
    private static SolaceAsSink solConfig = new SolaceAsSink();

    public static void shutDownConnector(){

        solConfig.closeSession();
        // Once the time is over, shutdown.
        client.disconnect().await();
    }
    public static void main(String[] args) throws Exception {

        // Connect to localhost and use the travel-sample bucket
        CouchbaseSourceConfig cbconfig = new CouchbaseSourceConfig();
        cbconfig.setClient();
        client = cbconfig.getClient();

        // Start solace session
        solConfig.startSession();

        // If we are in a rollback scenario, rollback the partition and restart the stream.
        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(final ChannelFlowController flowController, final ByteBuf event) {
                if (DcpSnapshotMarkerRequest.is(event)) {
                    flowController.ack(event);
                }
                if (RollbackMessage.is(event)) {
                    final short partition = RollbackMessage.vbucket(event);
                    client.rollbackAndRestartStream(partition, RollbackMessage.seqno(event))
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
        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                if (DcpMutationMessage.is(event)) {
                    if(DcpMutationMessage.revisionSeqno(event) == 1) {
                        LOGGER.info("New Record" + JsonObject.fromJson(
                                DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)));
                        try{
                            solConfig.sendMessage(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8));
                        }catch(Exception ex)
                        {ex.printStackTrace();}
                    }
                    else {
                        LOGGER.info("Updated Record" + JsonObject.fromJson(
                                DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)));
                        try {//content doesn't have the key.
                            //String message = "Key" + DcpMutationMessage.key(event);
                              String message =  DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8);
                            solConfig.sendMessage(message);
                            LOGGER.info("Sent Message::" + message);
                        }catch(Exception ex)
                        {ex.printStackTrace();}
                    }
                    } else if (DcpDeletionMessage.is(event)) {
                    LOGGER.info("Deleted Record: " + DcpDeletionMessage.keyString(event));
                    try {
                        solConfig.sendMessage("Record Deleted" + DcpDeletionMessage.keyString(event));
                    }catch(Exception ex)
                    {ex.printStackTrace();}
                }
                event.release();
            }
        });

        // Connect the sockets
        client.connect().await();

        // Initialize the state (start now, never stop)
        client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();

        // Start streaming on all partitions
        client.startStreaming().await();
        LOGGER.info("Awaiting Stream");

        // Sleep for some time to print the mutations
        // The printing happens on the IO threads!
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
        shutDownConnector();
    }

}
