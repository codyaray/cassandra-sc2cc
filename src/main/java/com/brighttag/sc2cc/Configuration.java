package com.brighttag.sc2cc;

import java.io.IOException;
import java.util.Properties;

public class Configuration {
  static final String CASSANDRA_CLUSTER_NAME = "com.brighttag.sc2cc.cluster.name";
  static final String CASSANDRA_CLUSTER_HOSTS = "com.brighttag.sc2cc.cluster.hosts";
  static final String CASSANDRA_KEYSPACE_NAME = "com.brighttag.sc2cc.keyspace.name";
  static final String CASSANDRA_OLD_COLUMN_FAMILY = "com.brighttag.sc2cc.column.family.old";
  static final String CASSANDRA_NEW_COLUMN_FAMILY = "com.brighttag.sc2cc.column.family.new";
  static final String GROUP_SIZE = "com.brighttag.sc2cc.group.size";

  private static final String DEFAULT_CLUSTER_NAME = "TutorialCluster";
  private static final String DEFAULT_CLUSTER_HOSTS = "127.0.0.1:9160";
  private static final String DEFAULT_KEYSPACE_NAME = "Tutorial";
  private static final String DEFAULT_OLD_COLUMN_FAMILY = "CountryStateCity";
  private static final String DEFAULT_NEW_COLUMN_FAMILY = "CountryStateCity";
  private static final int DEFAULT_GROUP_SIZE = 10000;

  public static Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty(CASSANDRA_CLUSTER_NAME + ".resolved",
        "${" + CASSANDRA_CLUSTER_NAME  + "|" + DEFAULT_CLUSTER_NAME  + "}");
    properties.setProperty(CASSANDRA_CLUSTER_HOSTS + ".resolved",
        "${" + CASSANDRA_CLUSTER_HOSTS + "|" + DEFAULT_CLUSTER_HOSTS + "}");
    properties.setProperty(CASSANDRA_KEYSPACE_NAME + ".resolved",
        "${" + CASSANDRA_KEYSPACE_NAME + "|" + DEFAULT_KEYSPACE_NAME + "}");
    properties.setProperty(CASSANDRA_OLD_COLUMN_FAMILY + ".resolved",
        "${" + CASSANDRA_OLD_COLUMN_FAMILY + "|" + DEFAULT_OLD_COLUMN_FAMILY + "}");
    properties.setProperty(CASSANDRA_NEW_COLUMN_FAMILY + ".resolved",
        "${" + CASSANDRA_NEW_COLUMN_FAMILY + "|" + DEFAULT_NEW_COLUMN_FAMILY + "}");
    properties.setProperty(GROUP_SIZE + ".resolved",
        "${" + GROUP_SIZE + "|" + DEFAULT_GROUP_SIZE + "}");
    return properties;
  }

  public static Properties getProperties(String filename) {
    Properties properties = new Properties();
    try {
      properties.load(Configuration.class.getResourceAsStream(filename));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return properties;
  }
}
