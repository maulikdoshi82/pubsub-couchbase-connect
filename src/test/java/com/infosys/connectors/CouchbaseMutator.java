package com.infosys.connectors;

import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.BucketDoesNotExistException;
import com.couchbase.client.java.error.InvalidPasswordException;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class CouchbaseMutator {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseMutator.class);
    private static Cluster cluster;
    private static Bucket bucket;
    private static int NO_OF_MESSAGES = 150;
    private static int PERF_TEST_MINUTES = 30;
    public boolean startCluster() {
        cluster = CouchbaseCluster.create("127.0.0.1");
        cluster.authenticate("Administrator", "password");
        int i = 0;
        for (i = 0; i < 3; i++) {
            try {
                bucket = cluster.openBucket("sample", 1000, TimeUnit.MILLISECONDS);
                break;
            } catch (Exception ex) {

                if (LOGGER.isDebugEnabled())
                    ex.printStackTrace();

                if (ex instanceof ConfigurationException) {
                    String cause = "" + ex.getCause();
                    if (cause.contains("Connection refused")) {
                        LOGGER.error("Couchbase Server is not running");
                        i = 3;
                    } else {
                        LOGGER.error(ex.getMessage() + " cause" + ex.getCause());
                    }
                } else if (ex instanceof InvalidPasswordException) {
                    LOGGER.error("Invalid Authentication for Couchbase Server. Please check User/Password.");
                } else if (ex instanceof BucketDoesNotExistException) {
                    LOGGER.error("Bucket does not exist::");
                    i = 3;
                } else {
                    LOGGER.error("An exception is thrown " + ex.getMessage()
                            + " .For detailed root cause, enable Debug Mode");
                }

                if (i < 3) {
                    LOGGER.info("No of Attempt:: " + (i + 1) + " in " + (1000 * (i + 1)) + " Millis");
                    try {
                        Thread.sleep(1000 * (i + 1));
                    } catch (InterruptedException iex) {
                        LOGGER.error("Exception caught while retrying. Someone woke me up");
                    }
                }
            }
        }
        if (i < 3) {
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
            doc.put("cb_updated_time", ("" + new Timestamp(System.currentTimeMillis())));
            bucket.upsert(JsonDocument.create(key, doc));
        } catch (RuntimeException tex) {
            LOGGER.error("TimeOut");
        }
    }

    public static void main(String[] args){
        CouchbaseMutator cbMutator = new CouchbaseMutator();
        boolean cbCon = cbMutator.startCluster();
        String key = "";
        int countBatches=1;
        long initiateTime = System.currentTimeMillis();
        long currentTime = 0;
        while(cbCon){
            LOGGER.info("Sending next batch of records::" + countBatches);
            for (int i = 0; i < NO_OF_MESSAGES; i++) {
                JSONObject obj = new JSONObject();
                key = "" + i;
                obj.put("key", key); // This is an important field.
                obj.put("name", "foo");
                obj.put("num", new Integer(100));
                obj.put("balance", new Double(1000.21));
                obj.put("is_vip", new Boolean(true));
                cbMutator.upsertDocument(key,JsonObject.fromJson(obj.toJSONString()));
            }
            try {
                currentTime = System.currentTimeMillis();
                if((currentTime - initiateTime)  > (PERF_TEST_MINUTES * 60 * 1000)) //break the loop after PERF_TEST_MINUTES
                    break;
                long sleepTime = (((1000 * countBatches) + initiateTime) - currentTime); // wait for a second if not done yet.
                if(sleepTime > 0)
                    Thread.sleep(sleepTime);
            }catch(Exception ex){
                ex.printStackTrace();
            }
            countBatches++;
        }
        LOGGER.info("Initiated Time::" + initiateTime);
        LOGGER.info("End Time::" + currentTime);
        LOGGER.info("Number of Batches Sent::" + countBatches);
    }
}
