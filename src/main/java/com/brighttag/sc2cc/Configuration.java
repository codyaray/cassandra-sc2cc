package com.brighttag.sc2cc;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
  private static Logger log = LoggerFactory.getLogger(Configuration.class);

  static final String CASSANDRA_CLUSTER_NAME = "com.brighttag.sc2cc.cluster.name";
  static final String CASSANDRA_CLUSTER_HOSTS = "com.brighttag.sc2cc.cluster.hosts";
  static final String CASSANDRA_KEYSPACE_NAME = "com.brighttag.sc2cc.keyspace.name";
  static final String CASSANDRA_OLD_COLUMN_FAMILY = "com.brighttag.sc2cc.column.family.old";
  static final String CASSANDRA_NEW_COLUMN_FAMILY = "com.brighttag.sc2cc.column.family.new";
  static final String TRANSFORMER_TASK_SIZE = "com.brighttag.sc2cc.transformer.task.size";
  static final String TRANSFORMER_QUEUE_SIZE = "com.brighttag.sc2cc.transformer.queue.size";
  static final String TRANSFORMER_THREAD_NUM = "com.brighttag.sc2cc.transformer.thread.num";

  public static Properties getProperties(String filename) {
    Properties properties = new Properties();
    properties.putAll(getDefaults()); // rocoto doesn't like defaults given to Properties constructor
    try {
      properties.load(Configuration.class.getResourceAsStream(filename));
    } catch (IOException e) {
      log.error("Problem reading configuration file {}", filename, e);
    }
    return properties;
  }

  private static Properties getDefaults() {
    Properties properties = new Properties();
    properties.setProperty(CASSANDRA_CLUSTER_NAME,  "Test Cluster");
    properties.setProperty(CASSANDRA_CLUSTER_HOSTS, "127.0.0.1:9160");
    properties.setProperty(TRANSFORMER_TASK_SIZE,   "500");
    properties.setProperty(TRANSFORMER_QUEUE_SIZE,  "10");
    properties.setProperty(TRANSFORMER_THREAD_NUM,  "5");
    return properties;
  }
}
