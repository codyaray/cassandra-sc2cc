package com.brighttag.sc2cc;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraContext {
  private final Cluster cluster;
  private final Keyspace keyspace;

  public CassandraContext(Configuration config) {
    cluster = HFactory.getOrCreateCluster(config.getClusterName(), config.getClusterHosts());

    ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
    ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
    keyspace = HFactory.createKeyspace(config.getKeyspaceName(), cluster, ccl);
  }

  /**
   * @return the keyspace
   */
  public Keyspace getKeyspace() {
    return keyspace;
  }

  /**
   * @return the cluster
   */
  public Cluster getCluster() {
    return cluster;
  }
}
