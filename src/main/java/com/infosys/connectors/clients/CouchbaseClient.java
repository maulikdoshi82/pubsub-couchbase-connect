package com.infosys.connectors.clients;

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.infosys.connectors.config.CouchbaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.java.error.*;

/**
 * This is a sample couchbase client for storing documents in Couchbase.
 * In connector code, CouchbaseAsSink uses it for storing records.
 */
public class CouchbaseClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseClient.class);
    private static Cluster cluster;
    private static Bucket bucket;

    public boolean startCluster() {
        CouchbaseConfig cbConfig = new CouchbaseConfig();
        cluster = CouchbaseCluster.create(cbConfig.getCbHostname());
        cluster.authenticate(cbConfig.getCbUsername(), cbConfig.getCbPassword());
        int i = 0;
        for (i = 0; i < cbConfig.getNoOfRetry(); i++) {
            try {
                bucket = cluster.openBucket(cbConfig.getCbBucket(), cbConfig.getCbConnTimeout(), TimeUnit.MILLISECONDS);
                break;
            } catch (Exception ex) {

                if (LOGGER.isDebugEnabled())
                    ex.printStackTrace();

                if (ex instanceof ConfigurationException) {
                    String cause = "" + ex.getCause();
                    if (cause.contains("Connection refused")) {
                        LOGGER.error("Couchbase Server is not running @" + cbConfig.getCbHostname());
                        i = cbConfig.getNoOfRetry();
                    } else {
                        LOGGER.error(ex.getMessage() + " cause" + ex.getCause());
                    }
                } else if (ex instanceof InvalidPasswordException) {
                    LOGGER.error("Invalid Authentication for Couchbase Server. Please check User/Password.");
                } else if (ex instanceof BucketDoesNotExistException) {
                    LOGGER.error("Bucket does not exist::" + cbConfig.getCbBucket());
                    i = cbConfig.getNoOfRetry();
                } else {
                    LOGGER.error("An exception is thrown " + ex.getMessage()
                            + " .For detailed root cause, enable Debug Mode");
                }

                if (i < cbConfig.getNoOfRetry()) {
                    LOGGER.info("No of Attempt:: " + (i + 1) + " in " + (1000 * (i + 1)) + " Millis");
                    try {
                        Thread.sleep(1000 * (i + 1));
                    } catch (InterruptedException iex) {
                        LOGGER.error("Exception caught while retrying. Someone woke me up");
                    }
                }
            }
        }
        if (i < cbConfig.getNoOfRetry()) {
            System.out.println("Did this work too?");
            bucket.bucketManager().createN1qlPrimaryIndex(true, false);
            return true;
        }
        return false;
    }

    public void shutDownConnector() {
        bucket.close();
        cluster.disconnect();
    }

    public void upsertDocument(String key, JsonObject doc) {
        try {
            doc.put("cb_received_time", ("" + new Timestamp(System.currentTimeMillis())));
            bucket.upsert(JsonDocument.create(key, doc));
        } catch (RuntimeException tex) {
            LOGGER.error("TimeOut");
        }
    }

}
