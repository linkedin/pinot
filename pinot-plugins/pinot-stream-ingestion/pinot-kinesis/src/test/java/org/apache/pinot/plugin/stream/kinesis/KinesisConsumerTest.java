package org.apache.pinot.plugin.stream.kinesis; /**
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


public class KinesisConsumerTest {

  private static final String STREAM_NAME = "kinesis-test";
  private static final String AWS_REGION = "us-west-2";

  public static void main(String[] args) {
    Map<String, String> props = new HashMap<>();
    props.put(KinesisConfig.STREAM, STREAM_NAME);
    props.put(KinesisConfig.AWS_REGION, AWS_REGION);
    props.put(KinesisConfig.MAX_RECORDS_TO_FETCH, "10");
    props.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    KinesisConfig kinesisConfig = new KinesisConfig(props);
    KinesisConnectionHandler kinesisConnectionHandler = new KinesisConnectionHandler(STREAM_NAME, AWS_REGION);
    List<Shard> shardList = kinesisConnectionHandler.getShards();
    for (Shard shard : shardList) {
      System.out.println("SHARD: " + shard.shardId());

      KinesisConsumer kinesisConsumer =
          new KinesisConsumer(kinesisConfig, new KinesisShardMetadata(shard.shardId(), STREAM_NAME, AWS_REGION));
      System.out.println(
          "Kinesis Checkpoint Range: < " + shard.sequenceNumberRange().startingSequenceNumber() + ", " + shard
              .sequenceNumberRange().endingSequenceNumber() + " >");
      KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shard.sequenceNumberRange().startingSequenceNumber());
      KinesisFetchResult fetchResult = kinesisConsumer.fetch(kinesisCheckpoint, null, 60 * 1000L);
      KinesisRecordsBatch list = fetchResult.getMessages();
      int n = list.getMessageCount();

      System.out.println("Found " + n + " messages ");
      for (int i = 0; i < n; i++) {
        System.out.println("SEQ-NO: " + list.getMessageOffsetAtIndex(i) + ", DATA: " + list.getMessageAtIndex(i));
      }
      kinesisConsumer.close();
    }
    kinesisConnectionHandler.close();
  }
}
