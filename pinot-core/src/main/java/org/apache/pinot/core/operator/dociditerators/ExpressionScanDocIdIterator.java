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
package org.apache.pinot.core.operator.dociditerators;

import java.util.Map;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The {@code ExpressionScanDocIdIterator} is the scan-based doc id iterator that can handle filters on the expressions.
 * It leverages the projection operator to batch processing the records block by block.
 */
public class ExpressionScanDocIdIterator implements ScanBasedDocIdIterator {
  private final TransformFunction _transformFunction;
  private final PredicateEvaluator _predicateEvaluator;
  private final Map<String, DataSource> _dataSourceMap;
  private final int _endDocId;

  private final int[] _docIdBuffer = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
  private int _numDocIdsFilled;

  private int _currentDocId = -1;
  private IntIterator _intIterator = null;
  private int _blockEndDocId = 0;

  // NOTE: Number of entries scanned is not accurate because we might need to scan multiple columns in order to solve
  // the expression, but we only track the number of entries for the resolved expression
  private int _numEntriesScanned = 0;

  public ExpressionScanDocIdIterator(TransformFunction transformFunction, PredicateEvaluator predicateEvaluator,
      Map<String, DataSource> dataSourceMap, int startDocId, int endDocId) {
    _transformFunction = transformFunction;
    _predicateEvaluator = predicateEvaluator;
    _dataSourceMap = dataSourceMap;
    _blockEndDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  public int next() {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }

    // If there are remaining records in the current block, return them first
    if (_intIterator != null && _intIterator.hasNext()) {
      _currentDocId = _intIterator.next();
      return _currentDocId;
    }

    // Evaluate the records in the next block
    while (_blockEndDocId < _endDocId) {
      int blockStartDocId = _blockEndDocId;
      _blockEndDocId = Math.min(blockStartDocId + DocIdSetPlanNode.MAX_DOC_PER_CALL, _endDocId);
      ProjectionBlock projectionBlock =
          new ProjectionOperator(_dataSourceMap, new RangeDocIdSetOperator(blockStartDocId, _blockEndDocId))
              .nextBlock();
      MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
      processProjectionBlock(projectionBlock, matchingDocIds);
      if (!matchingDocIds.isEmpty()) {
        _intIterator = matchingDocIds.getIntIterator();
        _currentDocId = _intIterator.next();
        return _currentDocId;
      }
    }

    _currentDocId = Constants.EOF;
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    if (targetDocId <= _currentDocId) {
      return _currentDocId;
    }

    // Search the current block first
    if (targetDocId < _blockEndDocId) {
      while (_intIterator.hasNext()) {
        _currentDocId = _intIterator.next();
        if (_currentDocId >= targetDocId) {
          return _currentDocId;
        }
      }
    }

    // Search the block following the target document id
    _intIterator = null;
    _blockEndDocId = targetDocId;
    return next();
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }

  @Override
  public MutableRoaringBitmap applyAnd(MutableRoaringBitmap answer) {
    ProjectionOperator projectionOperator = new ProjectionOperator(_dataSourceMap, new BitmapDocIdSetOperator(answer));
    MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = projectionOperator.nextBlock()) != null) {
      processProjectionBlock(projectionBlock, matchingDocIds);
    }
    return matchingDocIds;
  }

  @SuppressWarnings("DuplicatedCode")
  private void processProjectionBlock(ProjectionBlock projectionBlock, MutableRoaringBitmap matchingDocIds) {
    TransformResultMetadata resultMetadata = _transformFunction.getResultMetadata();
    if (resultMetadata.isSingleValue()) {
      _numEntriesScanned += _numDocIdsFilled;
      if (resultMetadata.hasDictionary()) {
        int[] dictIds = _transformFunction.transformToDictIdsSV(projectionBlock);
        for (int i = 0; i < _numDocIdsFilled; i++) {
          if (_predicateEvaluator.applySV(dictIds[i])) {
            matchingDocIds.add(_docIdBuffer[i]);
          }
        }
      } else {
        switch (resultMetadata.getDataType()) {
          case INT:
            int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              if (_predicateEvaluator.applySV(intValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case LONG:
            long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              if (_predicateEvaluator.applySV(longValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case FLOAT:
            float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              if (_predicateEvaluator.applySV(floatValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case DOUBLE:
            double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              if (_predicateEvaluator.applySV(doubleValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case STRING:
            String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              if (_predicateEvaluator.applySV(stringValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case BYTES:
            byte[][] bytesValues = _transformFunction.transformToBytesValuesSV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              if (_predicateEvaluator.applySV(bytesValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      if (resultMetadata.hasDictionary()) {
        int[][] dictIdsArray = _transformFunction.transformToDictIdsMV(projectionBlock);
        for (int i = 0; i < _numDocIdsFilled; i++) {
          int[] dictIds = dictIdsArray[i];
          int numDictIds = dictIds.length;
          _numEntriesScanned += numDictIds;
          if (_predicateEvaluator.applyMV(dictIds, numDictIds)) {
            matchingDocIds.add(_docIdBuffer[i]);
          }
        }
      } else {
        switch (resultMetadata.getDataType()) {
          case INT:
            int[][] intValuesArray = _transformFunction.transformToIntValuesMV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              int[] values = intValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case LONG:
            long[][] longValuesArray = _transformFunction.transformToLongValuesMV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              long[] values = longValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case FLOAT:
            float[][] floatValuesArray = _transformFunction.transformToFloatValuesMV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              float[] values = floatValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case DOUBLE:
            double[][] doubleValuesArray = _transformFunction.transformToDoubleValuesMV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              double[] values = doubleValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case STRING:
            String[][] valuesArray = _transformFunction.transformToStringValuesMV(projectionBlock);
            for (int i = 0; i < _numDocIdsFilled; i++) {
              String[] values = valuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }
  }

  @Override
  public int getNumEntriesScanned() {
    return _numEntriesScanned;
  }

  private class BitmapDocIdSetOperator extends BaseOperator<DocIdSetBlock> {
    static final String OPERATOR_NAME = "BitmapDocIdSetOperator";

    final IntIterator _intIterator;

    BitmapDocIdSetOperator(ImmutableRoaringBitmap bitmap) {
      _intIterator = bitmap.getIntIterator();
    }

    @Override
    protected DocIdSetBlock getNextBlock() {
      _numDocIdsFilled = 0;
      while (_numDocIdsFilled < DocIdSetPlanNode.MAX_DOC_PER_CALL && _intIterator.hasNext()) {
        _docIdBuffer[_numDocIdsFilled++] = _intIterator.next();
      }
      if (_numDocIdsFilled > 0) {
        return new DocIdSetBlock(_docIdBuffer, _numDocIdsFilled);
      } else {
        return null;
      }
    }

    @Override
    public String getOperatorName() {
      return OPERATOR_NAME;
    }
  }

  /**
   * NOTE: This operator contains only one block.
   */
  private class RangeDocIdSetOperator extends BaseOperator<DocIdSetBlock> {
    static final String OPERATOR_NAME = "RangeDocIdSetOperator";

    private DocIdSetBlock _docIdSetBlock;

    RangeDocIdSetOperator(int startDocId, int endDocId) {
      _numDocIdsFilled = endDocId - startDocId;
      for (int i = 0; i < _numDocIdsFilled; i++) {
        _docIdBuffer[i] = startDocId + i;
      }
      _docIdSetBlock = new DocIdSetBlock(_docIdBuffer, _numDocIdsFilled);
    }

    @Override
    protected DocIdSetBlock getNextBlock() {
      DocIdSetBlock docIdSetBlock = _docIdSetBlock;
      _docIdSetBlock = null;
      return docIdSetBlock;
    }

    @Override
    public String getOperatorName() {
      return OPERATOR_NAME;
    }
  }
}
