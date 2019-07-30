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
package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.LogQuerySrcImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


/**
 * TODO: This is test and will be deleted
 */
public class TunerTest extends TunerDriver {
  public static void main(String[] args) {
//    TunerDriver metaFetch = new TunerTest().setThreadPoolSize(3)
//        .setTuningStrategy(new AccumulateStats.Builder()
//            .setTableNamesWithoutType(new HashSet<String>() {{
//              add("scin_v2_additive");
//            }})
//            .setOutputDir("/Users/jiaguo/tmp3")
//            .build())
//        .setQuerySrc(new CompressedFilePathIter.Builder()
//            .set_directory("/Users/jiaguo/Workspace/pinot-tuna-script/data/segments")
//            .build())
//        .setMetaManager(null);
//    metaFetch.execute();

//    TunerDriver parserBased = new TunerTest().setThreadPoolSize(3)
//        .setTuningStrategy(new ParserBasedImpl.Builder().setAlgorithmOrder(ParserBasedImpl.THIRD_ORDER)
//            .setNumEntriesScannedThreshold(ParserBasedImpl.DEFAULT_NUM_ENTRIES_IN_FILTER_THRESHOLD)
//            .setNumQueriesThreshold(ParserBasedImpl.DEFAULT_NUM_QUERIES_THRESHOLD)
//            .build())
//        .setQuerySrc(new LogQuerySrcImpl.Builder().setValidLinePrefixRegex(LogQuerySrcImpl.REGEX_VALID_LINE_TIME)
//            .setParser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/finalTestData/broker.audienceCount.log")
//            .build())
//        .setMetaManager(
//            new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/finalTestData/meta/prodMetaV2/metadata.json")
//                .setUseExistingIndex(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX)
//                .build());
//    parserBased.execute();

    TunerDriver parserBased = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
        .setTuningStrategy(new ParserBasedImpl.Builder().setTableNamesWithoutType(null)
            .setNumQueriesThreshold(0)
            .setAlgorithmOrder(ParserBasedImpl.THIRD_ORDER)
            .setNumEntriesScannedThreshold(0)
            .build())
        .setQuerySrc(new LogQuerySrcImpl.Builder().setParser(new BrokerLogParserImpl())
            .setPath("/Users/jiaguo/finalTestData/broker.audienceCount.log")
            .build())
        .setMetaManager(new JsonFileMetaManagerImpl.Builder().setUseExistingIndex(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX) //Delete after demo
            .setPath("/Users/jiaguo/finalTestData/meta/prodMetaV2/metadata.json").build());
    parserBased.execute();

//    TunerDriver freqBased=new TunerTest()
//        .setThreadPoolSize(3)
//        .setStrategy(new FrequencyImpl.Builder()._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
//        .setQuerySrc(new LogFileSrcImpl.Builder()._parser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/scin_v2_additive.broker.log").build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/scin_v2_additive/col_meta").useExistingIndex(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
//    freqBased.execute();

//    TunerDriver fitModel = new TunerTest().setThreadPoolSize(3).setStrategy(new OLSAnalysisImpl.Builder().build())
//        .setQuerySrc(new LogQuerySrcImpl.Builder().setValidLinePrefixRegex(LogQuerySrcImpl.REGEX_VALID_LINE_TIME)
//            .setParser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/broker/pinot-broker.log.2019-07-25")
//            .build());
//    fitModel.execute();
  }
}