package com.infosys.connectors.cbsource;

import com.couchbase.client.dcp.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseSourceConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceConfig.class);
    static String CB_HOSTNAME = "localhost";
    static String CB_BUCKET="travel-sample";
    static String CB_USERNAME="Administrator";
    static String CB_PASSWORD="password";
    static Client cb_client;


    public Client getClient(){
        LOGGER.debug("Client Requested" + cb_client.toString());
        return cb_client;
    }

    public void setClient(){
        cb_client = Client.configure()
                .hostnames(CB_HOSTNAME)
                .bucket(CB_BUCKET)
                .username(CB_USERNAME)
                .password(CB_PASSWORD)
                .build();
        LOGGER.debug("Client Set Requested" + cb_client.toString());
    }
}
