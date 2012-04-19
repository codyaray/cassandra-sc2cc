package com.brighttag.sc2cc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;


import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-threaded bulk loader for geo data
 *
 * Execute this class by invoking the following at the project root:
 *   mvn -e exec:java -Dexec.mainClass="com.brighttag.sc2cc.composite.CompositeDataLoader"
 */
public class CompositeDataLoader {
  private static Logger log = LoggerFactory.getLogger(CompositeDataLoader.class);

  public static void main(String[] args) {
    Configuration config = Configuration.fromProperties();
    CassandraContext context = new CassandraContext(config);

    CompositeDataLoader loader = new CompositeDataLoader(context);

    try {
      long startTime = System.currentTimeMillis();
      List<ListenableFuture<Integer>> futures = loader.load(config.getDataFile());
      int total = sum(futures);

      log.info("Inserted {} timezones in {} ms", total, System.currentTimeMillis() - startTime);
    } catch (IOException e) {
      log.error("Error reading data from file", e);
    } catch (InterruptedException e) {
      log.warn("Interrupted while loading data", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      log.error("Error inserting data into cassandra", e);
    }

    loader.shutdown();
  }

  // naive wait for completion
  private static int sum(List<ListenableFuture<Integer>> counts)
      throws InterruptedException, ExecutionException {
    int total = 0;
    for (Future<Integer> count : counts) {
      total = total + count.get().intValue();
    }
    return total;
  }

  private final CassandraContext context;
  private final ListeningExecutorService executor;

  public CompositeDataLoader(CassandraContext context) {
    this.context = context;
    this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
  }

  public void shutdown() {
    executor.shutdown();
    context.getCluster().getConnectionManager().shutdown();
  }

  public List<ListenableFuture<Integer>> load(String file) throws IOException {
    return load(file, 250);
  }

  public List<ListenableFuture<Integer>> load(String file, int groupSize) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    List<ListenableFuture<Integer>> sums = new ArrayList<ListenableFuture<Integer>>();

    // read groupSize lines and hand off to worker
    List<String> lines = new ArrayList<String>(groupSize);

    String line;
    while ((line = reader.readLine()) != null) {
      lines.add(line);
      if (lines.size() % groupSize == 0) {
        sums.add(doParse(lines));
        lines.clear();
      }
    }
    // parse and insert the remaining lines
    sums.add(doParse(lines));

    return sums;
  }

  private ListenableFuture<Integer> doParse(List<String> lines) {
    return executor.submit(new LineParser(lines, context.getKeyspace()));
  }

  static class LineParser implements Callable<Integer> {
    // key for static composite, First row of dynamic composite
    public static final String COMPOSITE_KEY = "ALL";
    public static final String COUNTRY_STATE_CITY_CF = "CountryStateCity";

    private final List<String> lines;
    private final Keyspace keyspace;

    LineParser(List<String> lines, Keyspace keyspace) {
      this.lines = ImmutableList.copyOf(lines);
      this.keyspace = keyspace;
    }

    public Integer call() throws Exception {
      int count = lines.size();

      Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
      for (String row : lines) {
        GeoTimezone geoDataLine = GeoTimezone.parse(row);
        mutator.addInsertion(COMPOSITE_KEY, COUNTRY_STATE_CITY_CF, geoDataLine.toStaticColumn());
      }
      mutator.execute();
      log.debug("Inserted {} columns", count);

      return Integer.valueOf(count);
    }
  }

  static class GeoTimezone {
    public static final char SEPARATOR_CHAR = ',';

    static GeoTimezone parse(String line) {
      String[] vals = StringUtils.split(StringEscapeUtils.unescapeCsv(line), SEPARATOR_CHAR);
      log.debug("array size: {} for row: {}", vals.length, line);

      // extra un-escape to handle the case of "Washington, D.C." 
      String cityName = StringEscapeUtils.unescapeCsv(vals[2]);

      return new GeoTimezone(vals[0], vals[1], cityName, vals[3]);
    }

    private final String countryCode;
    private final String stateCode;
    private final String cityName;
    private final String timezone;

    public GeoTimezone(String countryCode, String stateCode, String cityName, String timezone) {
      this.countryCode = countryCode;
      this.stateCode = stateCode;
      this.cityName = cityName;
      this.timezone = timezone;    
    }

    /**
     * Creates an HColumn with a column name composite of the form:
     *   ['country_code']:['state]:['city name'])
     * and a value of ['timezone']
     */
    HColumn<Composite,String> toStaticColumn() {
      Composite composite = new Composite();
      composite.addComponent(countryCode, StringSerializer.get());
      composite.addComponent(stateCode, StringSerializer.get());
      composite.addComponent(cityName, StringSerializer.get());
      return HFactory.createColumn(
          composite, timezone, new CompositeSerializer(), StringSerializer.get());
    }

    String getCountryCode() {
      return countryCode;
    }

    String getStateCode() {
      return stateCode;
    }

    String getCityName() {
      return cityName;
    }

    String getTimezone() {
      return timezone;
    }
  }
}
