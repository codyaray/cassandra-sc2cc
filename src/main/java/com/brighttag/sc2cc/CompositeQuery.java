package com.brighttag.sc2cc;

import java.util.Iterator;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import static me.prettyprint.hector.api.beans.Composite.ComponentEquality;

/**
 * Demonstrates how to slice across a range of columns for a given KEY.
 * 
 * This class assumes you have created the CountryStateCity column family in the
 * Tutorial keyspace and have already run {@link CompositeDataLoader}
 * 
 * Execute this class with the following invocation from the project root:
 *   mvn -e exec:java -Dexec.mainClass="com.brighttag.sc2cc.CompositeQuery"
 */
public class CompositeQuery {

  // this is the KEY for our index row
  private static String KEY = "ALL";

  // this is the first component of the Composite for which we will look
  private static String START_ARG = "US";

  public static void main(String[] args) {
    Configuration config = Configuration.fromProperties();
    CassandraContext context = new CassandraContext(config);

    CompositeQuery compositeQuery = new CompositeQuery(context.getKeyspace(), config.getColumnFamilyName());

    Composite start = compositeFrom(START_ARG, ComponentEquality.EQUAL);
    Composite end = compositeFrom(START_ARG, ComponentEquality.GREATER_THAN_EQUAL);

    compositeQuery.printColumnsFor(start, end);
  }

  /**
   * Encapsulates the creation of Composite to make it easier to experiment with values
   * 
   * @param componentName
   * @param equality
   * @return
   */
  private static Composite compositeFrom(String componentName, ComponentEquality equality) {
    Composite composite = new Composite();
    composite.addComponent(0, componentName, equality);
    return composite;
  }

  private final Keyspace keyspace;
  private final String columnFamilyName;

  CompositeQuery(Keyspace keyspace, String columnFamilyName) {
    this.keyspace = keyspace;
    this.columnFamilyName = columnFamilyName;
  }

  /**
   * Prints out the columns we found with a summary of how many there were
   * 
   * @param start
   * @param end
   */
  public void printColumnsFor(Composite start, Composite end) {
    CompositeQueryIterator iter = new CompositeQueryIterator(KEY, start, end);

    int count = 0;
    System.out.printf("Printing all columns starting with %s", START_ARG);
    for (HColumn<Composite, String> column : iter) {
      System.out.printf("Country code: %s  Admin Code: %s  Name: %s  Timezone: %s \n",
          column.getName().get(0, StringSerializer.get()),
          column.getName().get(1, StringSerializer.get()),
          column.getName().get(2, StringSerializer.get()),
          column.getValue());
      count++;
    }
    System.out.printf("Found %d columns\n", count);
  }

   /**
   * Demonstrates the use of Hector's ColumnSliceIterator
   * for "paging" automatically over the results
   */
  class CompositeQueryIterator implements Iterable<HColumn<Composite, String>> {

    private final ColumnSliceIterator<String, Composite, String> sliceIterator;

    CompositeQueryIterator(String key, Composite start, Composite end) {
      SliceQuery<String, Composite, String> sliceQuery = HFactory.createSliceQuery(
          keyspace, StringSerializer.get(),
          new CompositeSerializer(), StringSerializer.get());
      sliceQuery.setColumnFamily(columnFamilyName);
      sliceQuery.setKey(key);

      sliceIterator = new ColumnSliceIterator<String,Composite,String>(sliceQuery, start, end, false);
    }

    public Iterator<HColumn<Composite, String>> iterator() {
      return sliceIterator;
    }
  }
}
