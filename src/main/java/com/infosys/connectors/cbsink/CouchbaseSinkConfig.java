package com.infosys.connectors.cbsink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;

public class CouchbaseSinkConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSinkConfig.class);

    private static String hostName = "localhost";
    private static String cbUserName = "Administrator";
    private static String cbPassword = "password";
    private static String bucketName = "sample";
    private Map<String, String> configProperties = new HashMap<String, String>();;

    public CouchbaseSinkConfig(){
        configProperties.put("BUCKET_NAME","sample");
        configProperties.put("USER_NAME",cbUserName);
        configProperties.put("PASSWORD",cbPassword);
        configProperties.put("HOST_NAME",hostName);

    }

    public Map getConfigProps(){
        return configProperties;
    }
    public void setConfigPros(String key, String val){
        configProperties.put(key, val);
    }

}
