package com.brighttag.sc2cc;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.googlecode.rocoto.configuration.ConfigurationModule;
import com.googlecode.rocoto.configuration.readers.PropertiesReader;
import com.googlecode.rocoto.configuration.readers.SystemPropertiesReader;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class HectorCassandraModule extends AbstractModule {

  static final String CASSANDRA_CLUSTER_NAME = "com.brighttag.cassandra.cluster.name";
  static final String CASSANDRA_CLUSTER_HOSTS = "com.brighttag.cassandra.cluster.hosts";
  static final String CASSANDRA_KEYSPACE_NAME = "com.brighttag.cassandra.keyspace.name";
  static final String CASSANDRA_COLUMN_FAMILY = "com.brighttag.cassandra.column.family";
  static final String GEO_DATA_FILE_LOCATION = "com.brighttag.geo.data.file.location";

  @Override
  protected void configure() {
    install(new ConfigurationModule()
        .addConfigurationReader(new SystemPropertiesReader())
        .addConfigurationReader(new PropertiesReader(Configuration.getProperties()))
        .addConfigurationReader(new PropertiesReader(Configuration.getProperties("/sc2cc.properties"))));
  }

  @Provides @Singleton
  protected Keyspace provideKeyspace(Cluster cluster, ConfigurableConsistencyLevel ccl,
      @Named(CASSANDRA_KEYSPACE_NAME + ".resolved") String keyspaceName) {
    return HFactory.createKeyspace(keyspaceName, cluster, ccl);    
  }

  @Provides @Singleton
  protected Cluster provideCluster(
      @Named(CASSANDRA_CLUSTER_NAME + ".resolved") String clusterName,
      @Named(CASSANDRA_CLUSTER_HOSTS + ".resolved") String clusterHosts) {
    return HFactory.getOrCreateCluster(clusterName, clusterHosts);
  }

  @Provides @Singleton
  protected ConfigurableConsistencyLevel provideConfigurableConsistencyLevel() {
    ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
    ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
    return ccl;
  }
}
