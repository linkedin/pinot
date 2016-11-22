/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.operator.docvaliterators.RealtimeMultiValueIterator;

public final class RealtimeMultiValueSet implements BlockValSet {
  /**
   *
   */
  private SingleColumnMultiValueReader reader;
  private int length;
  private DataType dataType;

  public RealtimeMultiValueSet(SingleColumnMultiValueReader reader, int length, DataType dataType) {
    super();
    this.reader = reader;
    this.length = length;
    this.dataType = dataType;
  }

  @Override
  public <T> T getSingleValues() {
    throw new UnsupportedOperationException(
        "Reading a batch of values is not supported for realtime multi-value BlockValSet.");
  }

  @Override
  public <T> T getMultiValues() {
    throw new UnsupportedOperationException(
        "Reading a batch of values is not supported for realtime multi-value BlockValSet.");
  }

  @Override
  public BlockValIterator iterator() {
    return new RealtimeMultiValueIterator(reader, length, dataType);
  }

  @Override
  public DataType getValueType() {
    return dataType;
  }

  @Override
  public void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds, int outStartPos) {
    throw new UnsupportedOperationException("Reading batch of multi-values in not implemented for realtime multivalue set");
  }

  @Override
  public int[] getDictionaryIds() {
    throw new UnsupportedOperationException(
        "Reading batch of multi-values in not implemented for realtime multi-value set");
  }

  @Override
  public int getDictionaryIdsForDocId(int docId, int[] outputDictIds) {
    throw new UnsupportedOperationException(
        "Reading value for a given docId in not implemented for realtime multi-value set");
  }
}
