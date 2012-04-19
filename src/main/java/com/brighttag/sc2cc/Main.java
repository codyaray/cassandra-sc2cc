package com.brighttag.sc2cc;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.brighttag.sc2cc.Configuration.CASSANDRA_NEW_COLUMN_FAMILY;
import static com.brighttag.sc2cc.Configuration.CASSANDRA_OLD_COLUMN_FAMILY;
import static com.brighttag.sc2cc.Configuration.TRANSFORMER_GROUP_SIZE;

public class Main {
  private static Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new HectorCassandraModule());

    Transformer transformer = injector.getInstance(Transformer.class);
    String oldColumnFamily = injector.getInstance(Key.get(String.class, Names.named(CASSANDRA_OLD_COLUMN_FAMILY + ".resolved")));
    String newColumnFamily = injector.getInstance(Key.get(String.class, Names.named(CASSANDRA_NEW_COLUMN_FAMILY + ".resolved")));
    int groupSize = injector.getInstance(Key.get(Integer.class, Names.named(TRANSFORMER_GROUP_SIZE + ".resolved")));

    try {
      long startTime = System.currentTimeMillis();
      List<ListenableFuture<Integer>> futures = transformer.transform(oldColumnFamily, newColumnFamily, groupSize);
      int total = sum(Futures.allAsList(futures).get()); // waits for completion

      log.info("Transformed {} rows in {} ms", total, System.currentTimeMillis() - startTime);
    } catch (InterruptedException e) {
      log.warn("Interrupted while transforming data", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      log.error("Error while rewriting data", e);
    }

    transformer.shutdown();
  }

  // surprised there's nothing that does this somewhere on the classpath already
  private static int sum(List<Integer> counts) {
    int total = 0;
    for (int count : counts) {
      total += count;
    }
    return total;
  }
}