package com.infosys.connectors.clients;

import com.couchbase.client.dcp.Client;
import com.infosys.connectors.config.CouchbaseDcpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseDcpClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDcpClient.class);
    private static Client cb_client;
    private static CouchbaseDcpConfig cbConfig = new CouchbaseDcpConfig();

    public Client getClient(){
        LOGGER.debug("Client Requested" + cb_client.toString());
        return cb_client;
    }

    public Client setClient() {
        cb_client = Client.configure()
                .hostnames(cbConfig.getCbHostname())
                .bucket(cbConfig.getCbBucket())
                .username(cbConfig.getCbUsername())
                .password(cbConfig.getCbPassword())
                .build();
        LOGGER.info("Client Set Requested");
        return cb_client;
    }
}
