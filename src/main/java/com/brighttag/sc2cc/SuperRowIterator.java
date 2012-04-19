package com.brighttag.sc2cc;

import java.util.Iterator;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;

/**
 * This class returns each key in the specified Super Column Family as an Iterator.
 * Heavily based upon Hector's {@code KeyIterator} implementation.
 *
 * @param <K> the type of the row key
 */
public class SuperRowIterator<K> implements Iterator<SuperRow<K,String,String,String>>,
    Iterable<SuperRow<K,String,String,String>> {

  private static final StringSerializer stringSerializer = StringSerializer.get();

  private static final int MAX_ROW_COUNT = 500;
  private static final int MAX_COL_COUNT = 2; // we only need this to tell if there are any columns in the row (to test for tombstones)

  private RangeSuperSlicesQuery<K, String, String, String> query;
  private Iterator<SuperRow<K, String, String, String>> rowsIterator;

  private SuperRow<K,String,String,String> nextValue = null;
  private SuperRow<K,String,String,String> lastReadValue = null;

  public SuperRowIterator(Keyspace keyspace, String columnFamily, Serializer<K> serializer) {
    query = HFactory
            .createRangeSuperSlicesQuery(keyspace, serializer, stringSerializer, stringSerializer, stringSerializer)
            .setColumnFamily(columnFamily)
            .setRange(null, null, false, MAX_COL_COUNT)
            .setRowCount(MAX_ROW_COUNT);

    runQuery(null);
  }

  @Override
  public Iterator<SuperRow<K,String,String,String>> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return nextValue != null;
  }

  @Override
  public SuperRow<K,String,String,String> next() {
    SuperRow<K,String,String,String> next = nextValue;
    findNext();
    return next;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void findNext() {
    nextValue = null;
    if (rowsIterator == null) {
      return;
    }
    while (rowsIterator.hasNext() && nextValue == null) {
      SuperRow<K, String, String, String> row = rowsIterator.next();
      lastReadValue = row;
      if (!row.getSuperSlice().getSuperColumns().isEmpty()) {
        nextValue = lastReadValue;
      }
    }
    if (!rowsIterator.hasNext() && nextValue == null) {
      runQuery(lastReadValue.getKey());
    }
  }

  private void runQuery(K start) {
    query.setKeys(start, null);

    rowsIterator = null;
    QueryResult<OrderedSuperRows<K, String, String, String>> result = query.execute();
    OrderedSuperRows<K, String, String, String> rows = (result != null) ? result.get() : null;
    rowsIterator = (rows != null) ? rows.iterator() : null;

    // we'll skip this first one, since it is the same as the last one from previous time we executed
    if (start != null && rowsIterator != null) {
      rowsIterator.next();   
    }

    if (!rowsIterator.hasNext()) {
      nextValue = null;    // all done.  our iterator's hasNext() will now return false;
    } else {
      findNext();
    }
  }
}
