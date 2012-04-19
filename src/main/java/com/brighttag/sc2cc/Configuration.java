package com.brighttag.sc2cc;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;

public class Configuration {
  private static final String DEFAULT_CLUSTER_NAME = "TutorialCluster";
  private static final String DEFAULT_KEYSPACE_NAME = "Tutorial";
  private static final String DEFAULT_CLUSTER_HOSTS = "127.0.0.1:9160";
  private static final String DEFAULT_COLUMN_FAMILY_NAME = "CountryStateCity";
  private static final String DEFAULT_DATA_FILE = "data/geodata.txt";

  private final String clusterName;
  private final String clusterHosts;
  private final String keyspaceName;
  private final String columnFamilyName;
  private final String dataFile;

  private Configuration(Builder builder) {
    this.clusterName = builder.clusterName;
    this.clusterHosts = builder.clusterHosts;
    this.keyspaceName = builder.keyspaceName;
    this.columnFamilyName = builder.columnFamilyName;
    this.dataFile = builder.dataFile;
  }

  public static Configuration fromProperties() {
    return fromProperties("/sc2cc.properties");
  }

  public static Configuration fromProperties(String filename) {
    Properties properties = new Properties();
    try {
      properties.load(Configuration.class.getResourceAsStream(filename));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return fromProperties(properties);
  }

  public static Configuration fromProperties(Properties properties) {
    String clusterName = properties.getProperty("cluster.name", DEFAULT_CLUSTER_NAME);
    String clusterHosts = properties.getProperty("cluster.hosts", DEFAULT_CLUSTER_HOSTS);
    String keyspaceName = properties.getProperty("keyspace.name", DEFAULT_KEYSPACE_NAME);
    String dataFile = properties.getProperty("geodata.file", DEFAULT_DATA_FILE);

    return new Configuration.Builder()
        .clusterName(clusterName)
        .clusterHosts(clusterHosts)
        .keyspaceName(keyspaceName)
        .dataFile(dataFile)
        .build();
  }

  /**
   * @return the cluster
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * @return the cluster
   */
  public String getClusterHosts() {
    return clusterHosts;
  }

  /**
   * @return the cluster
   */
  public String getKeyspaceName() {
    return keyspaceName;
  }

  /**
   * @return the column family name
   */
  public String getColumnFamilyName() {
    return columnFamilyName;
  }

  /**
   * @return the data file
   */
  public String getDataFile() {
    return dataFile;
  }

  public static class Builder {
    private String clusterName = DEFAULT_CLUSTER_NAME;
    private String clusterHosts = DEFAULT_CLUSTER_HOSTS;
    private String keyspaceName = DEFAULT_KEYSPACE_NAME;
    private String columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
    private String dataFile = DEFAULT_DATA_FILE;

    public Builder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder clusterHosts(String clusterHosts) {
      this.clusterHosts = clusterHosts;
      return this;
    }

    public Builder keyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    public Builder columnFamilyName(String columnFamilyName) {
      this.columnFamilyName = columnFamilyName;
      return this;
    }

    public Builder dataFile(String dataFile) {
      this.dataFile = dataFile;
      return this;
    }

    Configuration build() {
      Preconditions.checkNotNull(clusterName);
      Preconditions.checkNotNull(clusterHosts);
      Preconditions.checkNotNull(keyspaceName);
      Preconditions.checkNotNull(columnFamilyName);
      Preconditions.checkNotNull(dataFile);
      return new Configuration(this);
    }
  }
}
