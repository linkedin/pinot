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

package org.apache.pinot.core.data.manager.realtime;

import com.google.common.cache.LoadingCache;
import java.util.Optional;
import java.util.function.Function;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.ConsumptionRateLimiter;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class RealtimeConsumptionRateManagerTest {

  private static final int EPSILON = 50; // msec

  @Test
  public void testCreateRateLimiter()
      throws Exception {

    RealtimeConsumptionRateManager rateManager = RealtimeConsumptionRateManager.getInstance();

    // throttling is not enabled
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTopicConsumptionRateLimit()).thenReturn(Optional.of(2.0));
    ConsumptionRateLimiter rateLimiter = rateManager.createRateLimiterForSinglePartitionTopic(streamConfig);
    assertTiming(() -> {
      rateLimiter.throttle();
      rateLimiter.throttle();
      rateLimiter.throttle();
    }, totalTimeMs -> totalTimeMs < 1000);

    // consumption rate limit is set to 2 per second for a single partition topic
    rateManager.enableThrottling();
    when(streamConfig.getTopicConsumptionRateLimit()).thenReturn(Optional.of(2.0));
    ConsumptionRateLimiter rateLimiter2 = rateManager.createRateLimiterForSinglePartitionTopic(streamConfig);
    assertTiming(() -> {
      rateLimiter2.throttle();
      rateLimiter2.throttle();
      rateLimiter2.throttle();
    }, totalTimeMs -> totalTimeMs > 1000 - EPSILON);

    // consumption rate limit is set to 20 per second for a topic with 10 partitions
    when(streamConfig.getTopicConsumptionRateLimit()).thenReturn(Optional.of(20.0));
    LoadingCache<StreamConfig, Integer> mockLoadingCache = mock(LoadingCache.class);
    when(mockLoadingCache.get(any(StreamConfig.class))).thenReturn(10);
    rateManager._streamConfigToTopicPartitionCountMap = mockLoadingCache;
    ConsumptionRateLimiter rateLimiter3 = rateManager.createRateLimiterForMultiPartitionTopic(streamConfig);
    assertTiming(() -> {
      rateLimiter3.throttle();
      rateLimiter3.throttle();
      rateLimiter3.throttle();
    }, totalTimeMs -> totalTimeMs > 1000 - EPSILON);

    // no config for rate limiter means no throttling
    when(streamConfig.getTopicConsumptionRateLimit()).thenReturn(Optional.empty());
    ConsumptionRateLimiter rateLimiter4 = rateManager.createRateLimiterForSinglePartitionTopic(streamConfig);
    assertTiming(() -> {
      rateLimiter4.throttle();
      rateLimiter4.throttle();
      rateLimiter4.throttle();
    }, totalTimeMs -> totalTimeMs < 1000);

  }

  private void assertTiming(Runnable runnable, Function<Long, Boolean> assertFunc) {
    long start = System.currentTimeMillis();
    runnable.run();
    long totalTimeMs = System.currentTimeMillis() - start;
    assertTrue(assertFunc.apply(totalTimeMs));
  }
}
