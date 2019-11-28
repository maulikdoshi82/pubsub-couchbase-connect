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

import java.util.Map;

/**
 * This example starts from the current point in time and publishes every change that happens.
 * This example is based on java-dcp-client provided by couchbase
 */
public class CouchbaseClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseClient.class);
    private static Cluster cluster;
    private static Bucket bucket;

    public void startCluster(){
        CouchbaseConfig cbConfig = new CouchbaseConfig();
        cluster = CouchbaseCluster.create(cbConfig.getCbHostname());
        cluster.authenticate(cbConfig.getCbUsername(),cbConfig.getCbPassword());
        bucket = cluster.openBucket(cbConfig.getCbBucket());
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);
    }

    public void shutDownConnector(){
        bucket.close();
        cluster.disconnect();
    }
    public void upsertDocument(String key, JsonObject doc) {
        bucket.upsert(JsonDocument.create(key, doc));
    }

}
