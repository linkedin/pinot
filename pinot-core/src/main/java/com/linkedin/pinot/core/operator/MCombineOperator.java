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
package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.util.trace.TraceCallable;
import com.linkedin.pinot.core.util.trace.TraceRunnable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>MCombineOperator</code> class is the operator to combine selection results and aggregation only results.
 */
public class MCombineOperator extends BaseOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MCombineOperator.class);

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private long _timeOutMs;
  //Make this configurable
  //These two control the parallelism on a per query basis, depending on the number of segments to process
  private static int MIN_THREADS_PER_QUERY = 10;
  private static int MAX_THREADS_PER_QUERY = 10;
  private static int MIN_SEGMENTS_PER_THREAD = 10;

  static {
    int numCores = Runtime.getRuntime().availableProcessors();
    MIN_THREADS_PER_QUERY = Math.max(1, (int)(numCores * .5));
    //Dont have more than 10 threads per query
    MAX_THREADS_PER_QUERY = Math.min(MAX_THREADS_PER_QUERY, (int)(numCores * .5));
  }

  public MCombineOperator(List<Operator> operators, ExecutorService executorService, long timeOutMs,
      BrokerRequest brokerRequest) {
    _operators = operators;
    _executorService = executorService;
    _brokerRequest = brokerRequest;
    _timeOutMs = timeOutMs;
  }

  @Override
  public boolean open() {
    for (Operator op : _operators) {
      op.open();
    }
    return true;
  }


  @Override
  public Block getNextBlock() {
    final long startTime = System.currentTimeMillis();
    final long queryEndTime = System.currentTimeMillis() + _timeOutMs;
    int numGroups = Math.max(MIN_THREADS_PER_QUERY,
        Math.min(MAX_THREADS_PER_QUERY, (_operators.size() + MIN_SEGMENTS_PER_THREAD - 1) / MIN_SEGMENTS_PER_THREAD));
    //ensure that the number of groups is not more than the number of segments
    numGroups = Math.min(_operators.size(), numGroups);
    final List<List<Operator>> operatorGroups = new ArrayList<List<Operator>>(numGroups);
    for (int i = 0; i < numGroups; i++) {
      operatorGroups.add(new ArrayList<Operator>());
    }
    for (int i = 0; i < _operators.size(); i++) {
      operatorGroups.get(i % numGroups).add(_operators.get(i));
    }
    final BlockingQueue<Block> blockingQueue = new ArrayBlockingQueue<>(operatorGroups.size());
    // Submit operators.
    for (final List<Operator> operatorGroup : operatorGroups) {
      _executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          IntermediateResultsBlock mergedBlock = null;
          try {
            for (Operator operator : operatorGroup) {
              IntermediateResultsBlock blockToMerge = (IntermediateResultsBlock) operator.nextBlock();
              if (mergedBlock == null) {
                mergedBlock = blockToMerge;
              } else {
                try {
                  CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
                } catch (Exception e) {
                  LOGGER.error("Caught exception while merging two blocks (step 1).", e);
                  mergedBlock.addToExceptionsList(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
                }
              }
            }
          } catch (Exception e) {
            LOGGER.error("Caught exception while executing query.", e);
            mergedBlock = new IntermediateResultsBlock(e);
          }
          blockingQueue.offer(mergedBlock);
        }
      });
    }
    LOGGER.debug("Submitting operators to be run in parallel and it took:" + (System.currentTimeMillis() - startTime));

    // Submit merger job:
    Future<IntermediateResultsBlock> mergedBlockFuture =
        _executorService.submit(new TraceCallable<IntermediateResultsBlock>() {
          @Override
          public IntermediateResultsBlock callJob()
              throws Exception {
            int mergedBlocksNumber = 0;
            IntermediateResultsBlock mergedBlock = null;
            while ((queryEndTime > System.currentTimeMillis()) && (mergedBlocksNumber < operatorGroups.size())) {
              if (mergedBlock == null) {
                mergedBlock = (IntermediateResultsBlock) blockingQueue.poll(queryEndTime - System.currentTimeMillis(),
                    TimeUnit.MILLISECONDS);
                if (mergedBlock != null) {
                  mergedBlocksNumber++;
                }
                LOGGER.debug("Got response from operator 0 after: {}", (System.currentTimeMillis() - startTime));
              } else {
                IntermediateResultsBlock blockToMerge =
                    (IntermediateResultsBlock) blockingQueue.poll(queryEndTime - System.currentTimeMillis(),
                        TimeUnit.MILLISECONDS);
                if (blockToMerge != null) {
                  try {
                    LOGGER.debug("Got response from operator {} after: {}", mergedBlocksNumber,
                        (System.currentTimeMillis() - startTime));
                    CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
                    LOGGER.debug("Merged response from operator {} after: {}", mergedBlocksNumber,
                        (System.currentTimeMillis() - startTime));
                  } catch (Exception e) {
                    LOGGER.error("Caught exception while merging two blocks (step 2).", e);
                    mergedBlock.addToExceptionsList(
                        QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
                  }
                  mergedBlocksNumber++;
                }
              }
            }
            return mergedBlock;
          }
        });

    // Get merge results.
    IntermediateResultsBlock mergedBlock;
    try {
      mergedBlock = mergedBlockFuture.get(queryEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Caught InterruptedException.", e);
      mergedBlock = new IntermediateResultsBlock(QueryException.getException(QueryException.FUTURE_CALL_ERROR, e));
    } catch (ExecutionException e) {
      LOGGER.error("Caught ExecutionException.", e);
      mergedBlock = new IntermediateResultsBlock(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
    } catch (TimeoutException e) {
      LOGGER.error("Caught TimeoutException", e);
      mergedBlock =
          new IntermediateResultsBlock(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR, e));
    }

    // Trim merged results.
    if ((_brokerRequest.getAggregationsInfoSize() > 0) && (_brokerRequest.getGroupBy() != null)
        && (_brokerRequest.getGroupBy().getColumnsSize() > 0)) {
      trimToSize(_brokerRequest, mergedBlock);
    }

    // Update execution statistics.
    ExecutionStatistics executionStatistics = new ExecutionStatistics();
    for (Operator operator : _operators) {
      ExecutionStatistics executionStatisticsToMerge = operator.getExecutionStatistics();
      if (executionStatisticsToMerge != null) {
        executionStatistics.merge(executionStatisticsToMerge);
      }
    }
    mergedBlock.setNumDocsScanned(executionStatistics.getNumDocsScanned());
    mergedBlock.setNumEntriesScannedInFilter(executionStatistics.getNumEntriesScannedInFilter());
    mergedBlock.setNumEntriesScannedPostFilter(executionStatistics.getNumEntriesScannedPostFilter());
    mergedBlock.setTotalRawDocs(executionStatistics.getNumTotalRawDocs());

    return mergedBlock;
  }

  private void trimToSize(BrokerRequest brokerRequest, IntermediateResultsBlock mergedBlock) {
    AggregationGroupByOperatorService aggregationGroupByOperatorService =
        new AggregationGroupByOperatorService(brokerRequest.getAggregationsInfo(), brokerRequest.getGroupBy());
    List<Map<String, Serializable>> trimmedResults =
        aggregationGroupByOperatorService.trimToSize(mergedBlock.getAggregationGroupByOperatorResult());
    mergedBlock.setAggregationGroupByResult(trimmedResults);
  }

  @Override
  public Block getNextBlock(BlockId blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    for (Operator op : _operators) {
      op.close();
    }
    return true;
  }
}
