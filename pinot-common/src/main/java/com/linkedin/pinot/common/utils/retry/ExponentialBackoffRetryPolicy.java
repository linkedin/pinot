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
package com.linkedin.pinot.common.utils.retry;

import java.util.concurrent.ThreadLocalRandom;


/**
 * Retry policy with exponential backoff delay between attempts.
 * <p>The delay between the i<sup>th</sup> and (i + 1)<sup>th</sup> attempts is between delayScaleFactor<sup>i - 1</sup>
 * * initialDelayMs and delayScaleFactor<sup>i</sup> * initialDelayMs.
 */
public class ExponentialBackoffRetryPolicy extends BaseRetryPolicy {
  private final ThreadLocalRandom _random = ThreadLocalRandom.current();
  private final double _delayScaleFactor;
  private final long _maxDelayMs;
  private long _minDelayMs;

  public ExponentialBackoffRetryPolicy(int maxNumAttempts, long initialDelayMs, double delayScaleFactor) {
    super(maxNumAttempts);
    _delayScaleFactor = delayScaleFactor;
    _minDelayMs = initialDelayMs;
    _maxDelayMs = (long) (_minDelayMs * Math.pow(delayScaleFactor, maxNumAttempts));
  }

  @Override
  protected long getNextDelayMs() {
    long maxDelayMs = Math.min((long) (_minDelayMs * _delayScaleFactor), _maxDelayMs);
    long nextDelayMs = _random.nextLong(_minDelayMs, maxDelayMs + 1);
    _minDelayMs = maxDelayMs;
    return nextDelayMs;
  }
}
