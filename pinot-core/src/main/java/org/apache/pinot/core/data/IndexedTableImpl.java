package org.apache.pinot.core.data;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


/**
 * {@link IndexedTable} implementation for aggregating TableRecords based on combination of keys
 */
public class IndexedTableImpl implements IndexedTable {

  /** Factor used to add buffer to maxCapacity of the Collection used **/
  private static final double BUFFER_FACTOR = 1.2;
  /** Factor used to decide eviction threshold **/
  private static final double EVICTION_FACTOR = 1.1;

  private List<TableRecord> _records = new ArrayList<>();
  private ConcurrentMap<Object[], Integer> _lookupTable;
  private ReentrantReadWriteLock _readWriteLock;

  private DataSchema _dataSchema;
  private List<AggregationInfo> _aggregationInfos;
  private List<AggregationFunction> _aggregationFunctions;
  private List<SelectionSort> _orderBy;
  private int _evictCapacity;
  private int _bufferedCapacity;

  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity) {
    _dataSchema = dataSchema;
    _aggregationInfos = aggregationInfos;
    _orderBy = orderBy;

    // maintain a buffer size 20% more than max capacity
    // this is to ensure bottom records get a fair chance
    _bufferedCapacity = (int) (maxCapacity * BUFFER_FACTOR);
    // after eviction, bring down size to 10% more than max capacity
    _evictCapacity = (int) (maxCapacity * EVICTION_FACTOR);
    _lookupTable = new ConcurrentHashMap<>(_bufferedCapacity);
    _readWriteLock = new ReentrantReadWriteLock();

    if (CollectionUtils.isNotEmpty(aggregationInfos)) {
      _aggregationFunctions = new ArrayList<>(aggregationInfos.size());
      for (AggregationInfo aggregationInfo : aggregationInfos) {
        _aggregationFunctions.add(
            AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo).getAggregationFunction());
      }
    }
  }

  @Override
  public boolean upsert(@Nonnull TableRecord newRecord) {

    Object[] keys = newRecord._keys;
    Preconditions.checkNotNull(keys, "Cannot upsert record with null keys");

    if (size() >= _bufferedCapacity && !_lookupTable.containsKey(keys)) {
      _readWriteLock.writeLock().lock();
      try {
        if (size() >= _bufferedCapacity) {
          sort();
          _records = _records.subList(0, _evictCapacity);
          rebuildLookupTable();
        }
      } finally {
        _readWriteLock.writeLock().unlock();
      }
    }

    _readWriteLock.readLock().lock();
    try {
      _lookupTable.compute(keys, (k, index) -> {
        if (index == null) {
          index = size();
          _records.add(newRecord);
        } else {
          if (CollectionUtils.isNotEmpty(_aggregationFunctions)) {
            TableRecord existingRecord = _records.get(index);
            aggregate(existingRecord, newRecord);
          }
        }
        return index;
      });
    } finally {
      _readWriteLock.readLock().unlock();
    }

    return true;
  }

  private void aggregate(TableRecord existingRecord, TableRecord newRecord) {
    for (int i = 0; i < _aggregationFunctions.size(); i++) {
      existingRecord._values[i] = _aggregationFunctions.get(i).merge(existingRecord._values[i], newRecord._values[i]);
    }
  }

  private void rebuildLookupTable() {
    _lookupTable.clear();
    for (int i = 0; i < _records.size(); i++) {
      _lookupTable.put(_records.get(i)._keys, i);
    }
  }

  @Override
  public boolean merge(@Nonnull IndexedTable table) {
    Iterator<TableRecord> iterator = table.iterator();
    while (iterator.hasNext()) {
      upsert(iterator.next());
    }
    return true;
  }

  @Override
  public int size() {
    return _records.size();
  }

  @Override
  public Iterator<TableRecord> iterator() {
    return _records.iterator();
  }

  @Override
  public boolean sort() {
    if (CollectionUtils.isNotEmpty(_orderBy)) {
      Comparator<TableRecord> comparator;
      if (CollectionUtils.isNotEmpty(_aggregationInfos)) {
        comparator = OrderByUtils.getKeysAndValuesComparator(_dataSchema, _orderBy, _aggregationInfos);
      } else {
        comparator = OrderByUtils.getKeysComparator(_dataSchema, _orderBy);
      }
      _records.sort(comparator);
    }
    return true;
  }
}
