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
package org.apache.pinot.ingestion.common;

/**
 * PushJobSpec defines segment push job related configuration
 */
public class PushJobSpec {

  /**
   * retris for push job, default is 1, which means no retry.
   */
  int _retryCount = 1;

  /**
   * retry wait Ms, default to 1 second.
   */
  long _retryWaitMs = 1000;

  /**
   * Used in SegmentUriPushJobRunner, which is used to composite the segment uri to send to pinot controller.
   * The URI sends to controller is in the format ${segmentUriPrefix}${segmentPath}${segmentUriSuffix}
   */
  String _segmentUriPrefix;
  String _segmentUriSuffix;

  public String getSegmentUriPrefix() {
    return _segmentUriPrefix;
  }

  public void setSegmentUriPrefix(String segmentUriPrefix) {
    _segmentUriPrefix = segmentUriPrefix;
  }

  public String getSegmentUriSuffix() {
    return _segmentUriSuffix;
  }

  public void setSegmentUriSuffix(String segmentUriSuffix) {
    _segmentUriSuffix = segmentUriSuffix;
  }

  public int getRetryCount() {
    return _retryCount;
  }

  public void setRetryCount(int retryCount) {
    _retryCount = retryCount;
  }

  public long getRetryWaitMs() {
    return _retryWaitMs;
  }

  public void setRetryWaitMs(long retryWaitMs) {
    _retryWaitMs = retryWaitMs;
  }
}
