package com.brighttag.sc2cc;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.brighttag.sc2cc.Configuration.CASSANDRA_OLD_COLUMN_FAMILY;
import static com.brighttag.sc2cc.Configuration.CASSANDRA_NEW_COLUMN_FAMILY;
import static com.brighttag.sc2cc.Configuration.GROUP_SIZE;

/**
 * Multi-threaded bulk transformer to rewrite SuperColumn-based data 
 * to use Composite Columns instead.
 *
 * Execute this class by invoking the following at the project root:
 *   mvn -e exec:java -Dexec.mainClass="com.brighttag.sc2cc.SuperColumnToCompositeTransformer"
 */
public class SuperColumnToCompositeTransformer {
  private static Logger log = LoggerFactory.getLogger(SuperColumnToCompositeTransformer.class);

  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new HectorCassandraModule());

    SuperColumnToCompositeTransformer transformer = injector.getInstance(SuperColumnToCompositeTransformer.class);
    String oldColumnFamily = injector.getInstance(Key.get(String.class, Names.named(CASSANDRA_OLD_COLUMN_FAMILY + ".resolved")));
    String newColumnFamily = injector.getInstance(Key.get(String.class, Names.named(CASSANDRA_NEW_COLUMN_FAMILY + ".resolved")));
    int groupSize = injector.getInstance(Key.get(Integer.class, Names.named(GROUP_SIZE + ".resolved")));

    try {
      long startTime = System.currentTimeMillis();
      List<ListenableFuture<Integer>> futures = transformer.transform(oldColumnFamily, newColumnFamily, groupSize);
      int total = sum(Futures.allAsList(futures).get()); // waits for completion

      log.info("Transformed {} rows in {} ms", total, System.currentTimeMillis() - startTime);
    } catch (InterruptedException e) {
      log.warn("Interrupted while migrating data", e);
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

  private final Cluster cluster;
  private final Keyspace keyspace;
  private final ListeningExecutorService executor;

  @Inject
  public SuperColumnToCompositeTransformer(Cluster cluster, Keyspace keyspace) {
    this.cluster = cluster;
    this.keyspace = keyspace;
    this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
  }

  public void shutdown() {
    executor.shutdown();
    cluster.getConnectionManager().shutdown();
  }

  /**
   * Transforms Cassandra data from using Super Columns to Composite Columns,
   * rewriting data from {@code oldColumnFamily} to {@code newColumnFamily} in
   * the process. The {@code newColumnFamily} must already exist.
   *
   * Example:
   * 
   * Original Schema
   *     column_family
   *         row_key
   *             super_column_name
   *                 column_name=column_value
   *
   * Transformed Schema
   *     column_family
   *         row_key
   *             super_column_name:column_name=column_value
   * 
   * @param oldColumnFamily the name of the existing column family
   * @param newColumnFamily the name of the new, empty column family
   * @param groupSize the number of rows to rewrite per worker
   * @return a list of futures representing the number of rows inserted by each worker
   */
  public List<ListenableFuture<Integer>> transform(String oldColumnFamily, String newColumnFamily, int groupSize) {
    List<ListenableFuture<Integer>> rowCounts = Lists.newArrayList();
    Iterable<SuperRow<String,String,String,String>> keyIterator =
            new SuperRowIterator<String>(keyspace, oldColumnFamily, StringSerializer.get());

    List<SuperRow<String,String,String,String>> rows = Lists.newArrayList();
    for (SuperRow<String,String,String,String> row : keyIterator) {
      rows.add(row);
      if (rows.size() % groupSize == 0) {
        rowCounts.add(doRewrite(newColumnFamily, rows));
        rows.clear();
      }
    }

    return rowCounts;
  }

  private ListenableFuture<Integer> doRewrite(String columnFamily,
      List<SuperRow<String,String,String,String>> rows) {
    return executor.submit(new ReWriter(columnFamily, rows));
  }

  /**
   * A "worker" that rewrites the given {@code rows} to {@code columnFamily},
   * migrating the superColumn/column to composite(superColumn,column).
   */
  private class ReWriter implements Callable<Integer> {
    private final String columnFamily;
    private final List<SuperRow<String,String,String,String>> rows;

    ReWriter(String columnFamily, List<SuperRow<String,String,String,String>> rows) {
      this.columnFamily = columnFamily;
      this.rows = rows;
    }

    /**
     * Rewrites the row, changing superColumn/column to composite(superColumn,column)
     * @return the number of rows re-written by this worker
     */
    @Override
    public Integer call() throws Exception {
      int count = rows.size();

      Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
      for (SuperRow<String,String,String,String> row : rows) {
        for (HSuperColumn<String, String, String> superColumn : row.getSuperSlice().getSuperColumns()) {
          for (HColumn<String, String> column : superColumn.getColumns()) {
            HColumn<Composite,String> newColumn = column(superColumn, column);
            mutator.addInsertion(row.getKey(), columnFamily, newColumn);
          }
        }
      }
      mutator.execute();
      log.debug("Inserted {} rows", count);

      return Integer.valueOf(count);
    }
  }

  /**
   * Returns a new column with key {@code superColumn.name:column.name}
   * and value {@code column.value}.
   * 
   * @param superColumn original superColumn
   * @param column original column
   * @return new composite-keyed column
   */
  private static HColumn<Composite, String> column(
      HSuperColumn<String,String,String> superColumn, HColumn<String,String> column) {
    Composite newColKey = composite(superColumn.getName(), column.getName());
    return HFactory.createColumn(
        newColKey, column.getValue(), new CompositeSerializer(), StringSerializer.get());
  }

  /**
   * Returns a two-component composite
   * 
   * @param superColumn the first component of the returned composite
   * @param column the second component of the returned composite
   * @return a two-component composite
   */
  private static Composite composite(String superColumn, String column) {
    Composite composite = new Composite();
    composite.addComponent(0, superColumn, ComponentEquality.EQUAL);
    composite.addComponent(1, column, ComponentEquality.EQUAL);
    return composite;
  }
}
