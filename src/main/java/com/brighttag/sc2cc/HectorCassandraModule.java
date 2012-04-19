package com.brighttag.sc2cc;

import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.googlecode.rocoto.configuration.ConfigurationModule;
import com.googlecode.rocoto.configuration.readers.PropertiesReader;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import static com.brighttag.sc2cc.Configuration.CASSANDRA_CLUSTER_NAME;
import static com.brighttag.sc2cc.Configuration.CASSANDRA_CLUSTER_HOSTS;
import static com.brighttag.sc2cc.Configuration.CASSANDRA_KEYSPACE_NAME;
import static com.brighttag.sc2cc.Configuration.TRANSFORMER_GROUP_SIZE;

public class HectorCassandraModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new ConfigurationModule()
        .addConfigurationReader(new OrderedPropertiesReader(
            Configuration.getProperties("/sc2cc.properties"),
            Configuration.getProperties(),
            System.getProperties())));
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
    ccl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
    return ccl;
  }

  // I'll blow up this module if configured with a non-integer!
  @Provides @Singleton
  @Named(TRANSFORMER_GROUP_SIZE)
  int provideGroupSize(@Named(TRANSFORMER_GROUP_SIZE) String groupSize) {
    return Integer.parseInt(groupSize);
  }

  static class OrderedPropertiesReader extends PropertiesReader {
    public OrderedPropertiesReader(Properties... properties) {
      super(merge(properties));
    }
    
    private static Properties merge(Properties... properties) {
      Properties props = new Properties();
      for (Properties p : properties) {
        props.putAll(p);
      }
      return props;
    }
  }
}
