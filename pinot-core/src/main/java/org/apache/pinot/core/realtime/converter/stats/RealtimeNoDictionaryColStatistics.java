/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.realtime.converter.stats;

import java.util.Set;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.io.readerwriter.BaseSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.segment.creator.ColumnStatistics;

import static org.apache.pinot.core.common.Constants.UNKNOWN_CARDINALITY;


public class RealtimeNoDictionaryColStatistics implements ColumnStatistics {
  private final DataSourceMetadata _dataSourceMetadata;
  private final BaseSingleColumnSingleValueReaderWriter _forwardIndex;

  public RealtimeNoDictionaryColStatistics(DataSource dataSource) {
    _dataSourceMetadata = dataSource.getDataSourceMetadata();
    _forwardIndex = (BaseSingleColumnSingleValueReaderWriter) dataSource.getForwardIndex();
  }

  @Override
  public Object getMinValue() {
    return _dataSourceMetadata.getMinValue();
  }

  @Override
  public Object getMaxValue() {
    return _dataSourceMetadata.getMaxValue();
  }

  @Override
  public Object getUniqueValuesSet() {
    return null;
  }

  @Override
  public int getCardinality() {
    return UNKNOWN_CARDINALITY;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _forwardIndex.getLengthOfShortestElement();
  }

  @Override
  public int getLengthOfLargestElement() {
    return _forwardIndex.getLengthOfLongestElement();
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _dataSourceMetadata.getNumDocs();
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return 0;
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public PartitionFunction getPartitionFunction() {
    return _dataSourceMetadata.getPartitionFunction();
  }

  @Override
  public int getNumPartitions() {
    PartitionFunction partitionFunction = _dataSourceMetadata.getPartitionFunction();
    if (partitionFunction != null) {
      return partitionFunction.getNumPartitions();
    } else {
      return 0;
    }
  }

  @Override
  public Set<Integer> getPartitions() {
    return _dataSourceMetadata.getPartitions();
  }
}
