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

package com.linkedin.pinot.controller.helix.core.realtime.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Updates the flush threshold rows of the new segment metadata, based on the segment size and number of rows of previous segment
 * The formula used to compute new number of rows is:
 * targetNumRows = ideal_segment_size * (a * current_rows_to_size_ratio + b * previous_rows_to_size_ratio)
 * where a = 0.25, b = 0.75, prev ratio= ratio collected over all previous segment completions
 * This ensures that we take into account the history of the segment size and number rows
 */
public class SegmentSizeBasedFlushThresholdUpdater implements FlushThresholdUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentSizeBasedFlushThresholdUpdater.class);

  private static final long IDEAL_SEGMENT_SIZE_BYTES = 500 * 1024 * 1024;
  /** Below this size, we double the rows threshold */
  private static final double OPTIMAL_SEGMENT_SIZE_BYTES_MIN = IDEAL_SEGMENT_SIZE_BYTES / 2;
  /** Above this size we half the row threshold */
  private static final double OPTIMAL_SEGMENT_SIZE_BYTES_MAX = IDEAL_SEGMENT_SIZE_BYTES * 1.5;
  private static final int INITIAL_ROWS_THRESHOLD = 100_000;

  private static final double CURRENT_SEGMENT_RATIO_WEIGHT = 0.25;
  private static final double PREVIOUS_SEGMENT_RATIO_WEIGHT = 0.75;

  @VisibleForTesting
  long getIdealSegmentSizeBytes() {
    return IDEAL_SEGMENT_SIZE_BYTES;
  }

  @VisibleForTesting
  int getInitialRowsThreshold() {
    return INITIAL_ROWS_THRESHOLD;
  }

  // num rows to segment size ratio of last committed segment for this table
  private AtomicDouble _latestSegmentRowsToSizeRatio = new AtomicDouble();

  @Override
  public void updateFlushThreshold(@Nonnull LLCRealtimeSegmentZKMetadata newSegmentZKMetadata,
      @Nonnull FlushThresholdUpdaterParams params) {

    LLCRealtimeSegmentZKMetadata committingSegmentZkMetadata = params.getCommittingSegmentZkMetadata();
    if (committingSegmentZkMetadata == null) { // first segment of the partition, hence committing segment is null
      double prevRatio = _latestSegmentRowsToSizeRatio.get();
      if (prevRatio > 0) { // new partition added case
        LOGGER.info(
            "Committing segment zk metadata is not available, setting rows threshold for segment {} using previous segments ratio",
            newSegmentZKMetadata.getSegmentName());
        long targetSegmentNumRows = (long) (IDEAL_SEGMENT_SIZE_BYTES * prevRatio);
        targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
        newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
      } else {
        LOGGER.info("Committing segment zk metadata is not available, setting default rows threshold for segment {}",
            newSegmentZKMetadata.getSegmentName());
        newSegmentZKMetadata.setSizeThresholdToFlushSegment(INITIAL_ROWS_THRESHOLD);
      }
      return;
    }

    long committingSegmentSizeBytes = params.getCommittingSegmentSizeBytes();
    if (committingSegmentSizeBytes <= 0) { // repair segment case
      LOGGER.info(
          "Committing segment size is not available, setting thresholds for segment {} from previous segment {}",
          newSegmentZKMetadata.getSegmentName(), committingSegmentZkMetadata.getSegmentName());
      newSegmentZKMetadata.setSizeThresholdToFlushSegment(committingSegmentZkMetadata.getSizeThresholdToFlushSegment());
      return;
    }

    long timeConsumed = System.currentTimeMillis() - committingSegmentZkMetadata.getCreationTime();
    long numRowsConsumed = committingSegmentZkMetadata.getTotalRawDocs();
    int numRowsThreshold = committingSegmentZkMetadata.getSizeThresholdToFlushSegment();
    LOGGER.info("Time consumed:{}  Num rows consumed:{} Num rows threshold:{} Committing segment size bytes:{}",
        TimeUtils.convertMillisToPeriod(timeConsumed), numRowsConsumed, numRowsThreshold, committingSegmentSizeBytes);

    double currentRatio = (double) numRowsConsumed / committingSegmentSizeBytes;
    synchronized (_latestSegmentRowsToSizeRatio) {
      double prevRatio = _latestSegmentRowsToSizeRatio.get();
      if (prevRatio > 0) {
        _latestSegmentRowsToSizeRatio.set(CURRENT_SEGMENT_RATIO_WEIGHT * currentRatio + PREVIOUS_SEGMENT_RATIO_WEIGHT * prevRatio);
      } else {
        _latestSegmentRowsToSizeRatio.set(currentRatio);
      }
    }

    long targetSegmentNumRows;
    if (committingSegmentSizeBytes < OPTIMAL_SEGMENT_SIZE_BYTES_MIN) {
      targetSegmentNumRows = numRowsConsumed + numRowsConsumed / 2;
      LOGGER.info("Committing segment size is less than min segment size {}, doubling rows threshold to : {}",
          OPTIMAL_SEGMENT_SIZE_BYTES_MIN, newSegmentZKMetadata.getSizeThresholdToFlushSegment());
    } else if (committingSegmentSizeBytes > OPTIMAL_SEGMENT_SIZE_BYTES_MAX) {
      targetSegmentNumRows = numRowsConsumed / 2;
      LOGGER.info("Committing segment size is greater than max segment size {}, halving rows threshold {}",
          OPTIMAL_SEGMENT_SIZE_BYTES_MAX, newSegmentZKMetadata.getSizeThresholdToFlushSegment());
    } else {
      targetSegmentNumRows = (long) (IDEAL_SEGMENT_SIZE_BYTES * _latestSegmentRowsToSizeRatio.get());
      LOGGER.info("Setting new rows threshold : {}", newSegmentZKMetadata.getSizeThresholdToFlushSegment());
    }
    targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);

    newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
  }

  private long capNumRowsIfOverflow(long targetSegmentNumRows) {
    if (targetSegmentNumRows > Integer.MAX_VALUE) {
      targetSegmentNumRows = Integer.MAX_VALUE;
    }
    return targetSegmentNumRows;
  }
}
