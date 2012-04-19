package com.brighttag.sc2cc;

import java.io.IOException;
import java.util.Properties;

import static com.brighttag.sc2cc.HectorCassandraModule.CASSANDRA_CLUSTER_NAME;
import static com.brighttag.sc2cc.HectorCassandraModule.CASSANDRA_CLUSTER_HOSTS;
import static com.brighttag.sc2cc.HectorCassandraModule.CASSANDRA_KEYSPACE_NAME;
import static com.brighttag.sc2cc.HectorCassandraModule.CASSANDRA_COLUMN_FAMILY;
import static com.brighttag.sc2cc.HectorCassandraModule.GEO_DATA_FILE_LOCATION;

public class Configuration {
  private static final String DEFAULT_CLUSTER_NAME = "TutorialCluster";
  private static final String DEFAULT_CLUSTER_HOSTS = "127.0.0.1:9160";
  private static final String DEFAULT_KEYSPACE_NAME = "Tutorial";
  private static final String DEFAULT_COLUMN_FAMILY = "CountryStateCity";
  private static final String DEFAULT_GEO_DATA_FILE = "data/geodata.txt";

  public static Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty(CASSANDRA_CLUSTER_NAME + ".resolved",
        "${" + CASSANDRA_CLUSTER_NAME  + "|" + DEFAULT_CLUSTER_NAME  + "}");
    properties.setProperty(CASSANDRA_CLUSTER_HOSTS + ".resolved",
        "${" + CASSANDRA_CLUSTER_HOSTS + "|" + DEFAULT_CLUSTER_HOSTS + "}");
    properties.setProperty(CASSANDRA_KEYSPACE_NAME + ".resolved",
        "${" + CASSANDRA_KEYSPACE_NAME + "|" + DEFAULT_KEYSPACE_NAME + "}");
    properties.setProperty(CASSANDRA_COLUMN_FAMILY + ".resolved",
        "${" + CASSANDRA_COLUMN_FAMILY + "|" + DEFAULT_COLUMN_FAMILY + "}");
    properties.setProperty(GEO_DATA_FILE_LOCATION + ".resolved",
        "${" + GEO_DATA_FILE_LOCATION  + "|" + DEFAULT_GEO_DATA_FILE + "}");
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
