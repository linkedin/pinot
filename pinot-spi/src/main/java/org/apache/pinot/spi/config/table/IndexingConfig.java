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
package org.apache.pinot.spi.config.table;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class IndexingConfig extends BaseJsonConfig {
  private List<String> _invertedIndexColumns;
  private boolean _autoGeneratedInvertedIndex;
  private boolean _createInvertedIndexDuringSegmentGeneration;
  private List<String> _sortedColumn;
  private List<String> _bloomFilterColumns;
  private String _loadMode;
  private Map<String, String> _streamConfigs;
  private String _segmentFormatVersion;
  private String _columnMinMaxValueGeneratorMode;
  private List<String> _noDictionaryColumns; // TODO: replace this with noDictionaryConfig.
  private Map<String, String> _noDictionaryConfig;
  private List<String> _onHeapDictionaryColumns;
  private boolean _enableDefaultStarTree;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs;
  private SegmentPartitionConfig _segmentPartitionConfig;
  private boolean _aggregateMetrics;
  private boolean _nullHandlingEnabled;

  /**
   * The list of columns for which the variable length dictionary needs to be enabled in offline
   * segments. This is only valid for string and bytes columns and has no impact for columns of
   * other data types.
   */
  private List<String> _varLengthDictionaryColumns;

  @Nullable
  public List<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  public void setInvertedIndexColumns(List<String> invertedIndexColumns) {
    _invertedIndexColumns = invertedIndexColumns;
  }

  public boolean isAutoGeneratedInvertedIndex() {
    return _autoGeneratedInvertedIndex;
  }

  public void setAutoGeneratedInvertedIndex(boolean autoGeneratedInvertedIndex) {
    _autoGeneratedInvertedIndex = autoGeneratedInvertedIndex;
  }

  public boolean isCreateInvertedIndexDuringSegmentGeneration() {
    return _createInvertedIndexDuringSegmentGeneration;
  }

  public void setCreateInvertedIndexDuringSegmentGeneration(boolean createInvertedIndexDuringSegmentGeneration) {
    _createInvertedIndexDuringSegmentGeneration = createInvertedIndexDuringSegmentGeneration;
  }

  @Nullable
  public List<String> getSortedColumn() {
    return _sortedColumn;
  }

  public void setSortedColumn(List<String> sortedColumn) {
    _sortedColumn = sortedColumn;
  }

  @Nullable
  public List<String> getBloomFilterColumns() {
    return _bloomFilterColumns;
  }

  public void setBloomFilterColumns(List<String> bloomFilterColumns) {
    _bloomFilterColumns = bloomFilterColumns;
  }

  @Nullable
  public String getLoadMode() {
    return _loadMode;
  }

  public void setLoadMode(String loadMode) {
    _loadMode = loadMode;
  }

  @Nullable
  public Map<String, String> getStreamConfigs() {
    return _streamConfigs;
  }

  public void setStreamConfigs(Map<String, String> streamConfigs) {
    _streamConfigs = streamConfigs;
  }

  @Nullable
  public String getSegmentFormatVersion() {
    return _segmentFormatVersion;
  }

  public void setSegmentFormatVersion(String segmentFormatVersion) {
    _segmentFormatVersion = segmentFormatVersion;
  }

  @Nullable
  public String getColumnMinMaxValueGeneratorMode() {
    return _columnMinMaxValueGeneratorMode;
  }

  public void setColumnMinMaxValueGeneratorMode(String columnMinMaxValueGeneratorMode) {
    _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
  }

  @Nullable
  public List<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  public void setNoDictionaryColumns(List<String> noDictionaryColumns) {
    _noDictionaryColumns = noDictionaryColumns;
  }

  @Nullable
  public Map<String, String> getNoDictionaryConfig() {
    return _noDictionaryConfig;
  }

  public void setNoDictionaryConfig(Map<String, String> noDictionaryConfig) {
    _noDictionaryConfig = noDictionaryConfig;
  }

  @Nullable
  public List<String> getOnHeapDictionaryColumns() {
    return _onHeapDictionaryColumns;
  }

  public void setOnHeapDictionaryColumns(List<String> onHeapDictionaryColumns) {
    _onHeapDictionaryColumns = onHeapDictionaryColumns;
  }

  @Nullable
  public List<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }

  public void setVarLengthDictionaryColumns(List<String> varLengthDictionaryColumns) {
    _varLengthDictionaryColumns = varLengthDictionaryColumns;
  }

  public boolean isEnableDefaultStarTree() {
    return _enableDefaultStarTree;
  }

  public void setEnableDefaultStarTree(boolean enableDefaultStarTree) {
    _enableDefaultStarTree = enableDefaultStarTree;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    return _starTreeIndexConfigs;
  }

  public void setStarTreeIndexConfigs(List<StarTreeIndexConfig> starTreeIndexConfigs) {
    _starTreeIndexConfigs = starTreeIndexConfigs;
  }

  @Nullable
  public SegmentPartitionConfig getSegmentPartitionConfig() {
    return _segmentPartitionConfig;
  }

  public void setSegmentPartitionConfig(SegmentPartitionConfig segmentPartitionConfig) {
    _segmentPartitionConfig = segmentPartitionConfig;
  }

  public boolean isAggregateMetrics() {
    return _aggregateMetrics;
  }

  public void setAggregateMetrics(boolean value) {
    _aggregateMetrics = value;
  }

  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  public void setNullHandlingEnabled(boolean nullHandlingEnabled) {
    _nullHandlingEnabled = nullHandlingEnabled;
  }
}
