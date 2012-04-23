package com.brighttag.sc2cc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.googlecode.rocoto.configuration.ConfigurationModule;
import com.googlecode.rocoto.configuration.readers.PropertiesReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import static com.brighttag.sc2cc.Configuration.CASSANDRA_CLUSTER_NAME;
import static com.brighttag.sc2cc.Configuration.CASSANDRA_CLUSTER_HOSTS;
import static com.brighttag.sc2cc.Configuration.CASSANDRA_KEYSPACE_NAME;
import static com.brighttag.sc2cc.Configuration.TRANSFORMER_TASK_SIZE;
import static com.brighttag.sc2cc.Configuration.TRANSFORMER_QUEUE_SIZE;
import static com.brighttag.sc2cc.Configuration.TRANSFORMER_THREAD_NUM;

public class HectorCassandraModule extends AbstractModule {
  private static Logger log = LoggerFactory.getLogger(HectorCassandraModule.class);

  @Override
  protected void configure() {
    install(new ConfigurationModule()
        .addConfigurationReader(new PropertiesReader(Configuration.getProperties("/sc2cc.properties"))));
  }

  // I'll blow up this module if configured with a non-integer!
  @Provides @Singleton
  protected ExecutorService provideExecutorService(
      @Named(TRANSFORMER_THREAD_NUM) String workers,
      @Named(TRANSFORMER_QUEUE_SIZE) String queueSize) {
    int nThreads = Integer.parseInt(workers);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(nThreads, nThreads,
        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(Integer.parseInt(queueSize)),
        new ThreadPoolExecutor.CallerRunsPolicy());
    return executor;
  }

  @Provides @Singleton
  protected Keyspace provideKeyspace(Cluster cluster, ConfigurableConsistencyLevel ccl,
      @Named(CASSANDRA_KEYSPACE_NAME) String keyspaceName) {
    return HFactory.createKeyspace(keyspaceName, cluster, ccl);    
  }

  @Provides @Singleton
  protected Cluster provideCluster(
      @Named(CASSANDRA_CLUSTER_NAME) String clusterName,
      @Named(CASSANDRA_CLUSTER_HOSTS) String clusterHosts) {
    log.info("Using cassandra cluster: {}", clusterName);
    log.info("Using cassandra hosts: {}", clusterHosts);
    return HFactory.getOrCreateCluster(clusterName, clusterHosts);
  }

  @Provides @Singleton
  protected ConfigurableConsistencyLevel provideConfigurableConsistencyLevel() {
    ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
    ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
    ccl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
    return ccl;
  }

  // I, too, will blow up this module if configured with a non-integer!
  @Provides @Singleton @Named(TRANSFORMER_TASK_SIZE)
  int provideTaskSize(@Named(TRANSFORMER_TASK_SIZE) String taskSize) {
    log.info("Using transformer task size: {}", taskSize);
    return Integer.parseInt(taskSize);
  }
}
