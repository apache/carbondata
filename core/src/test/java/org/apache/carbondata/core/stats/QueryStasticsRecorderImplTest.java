/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.stats;

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class QueryStasticsRecorderImplTest {

  private static QueryStatisticsRecorderImpl queryStasticsRecorderImpl = null;
  private static QueryStatistic queryStatistic = null;
  private static QueryStatistic queryStatisticWithLOAD_BLOCKS_EXECUTOR = null;
  private static QueryStatistic queryStatisticWithSCAN_BLOCKS_TIME = null;
  private static QueryStatistic queryStatisticWithSCAN_BLOCKS_NUM = null;
  private static QueryStatistic queryStatisticWithLOAD_DICTIONARY = null;
  private static QueryStatistic queryStatisticWithRESULT_SIZE = null;
  private static QueryStatistic queryStatisticWithEXECUTOR_PART = null;
  private static QueryStatistic queryStatisticWithTOTAL_BLOCKLET_NUM = null;
  private static QueryStatistic queryStatisticWithVALID_SCAN_BLOCKLET_NUM = null;

  @BeforeClass public static void setUp() {
    queryStatisticWithLOAD_BLOCKS_EXECUTOR = new QueryStatistic();
    queryStasticsRecorderImpl = new QueryStatisticsRecorderImpl(System.nanoTime() + "");
    queryStasticsRecorderImpl.logStatisticsAsTableDriver();
    queryStatisticWithLOAD_BLOCKS_EXECUTOR
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR, 5L);
    queryStatisticWithLOAD_BLOCKS_EXECUTOR
        .addCountStatistic(QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR, 5L);
    queryStatisticWithLOAD_BLOCKS_EXECUTOR
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithLOAD_BLOCKS_EXECUTOR);
    queryStatisticWithSCAN_BLOCKS_TIME = new QueryStatistic();
    queryStatisticWithSCAN_BLOCKS_TIME.addStatistics(QueryStatisticsConstants.SCAN_BLOCKlET_TIME, 5L);
    queryStatisticWithSCAN_BLOCKS_TIME
        .addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME, 5L);
    queryStatisticWithSCAN_BLOCKS_TIME.addStatistics(QueryStatisticsConstants.SCAN_BLOCKlET_TIME, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithSCAN_BLOCKS_TIME);
    queryStatisticWithSCAN_BLOCKS_NUM = new QueryStatistic();
    queryStatisticWithSCAN_BLOCKS_NUM.addStatistics(QueryStatisticsConstants.SCAN_BLOCKS_NUM, 5L);
    queryStatisticWithSCAN_BLOCKS_NUM
        .addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKS_NUM, 5L);
    queryStatisticWithSCAN_BLOCKS_NUM.addStatistics(QueryStatisticsConstants.SCAN_BLOCKS_NUM, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithSCAN_BLOCKS_NUM);
    queryStatisticWithLOAD_DICTIONARY = new QueryStatistic();
    queryStatisticWithLOAD_DICTIONARY.addStatistics(QueryStatisticsConstants.LOAD_DICTIONARY, 5L);
    queryStatisticWithLOAD_DICTIONARY
        .addCountStatistic(QueryStatisticsConstants.LOAD_DICTIONARY, 5L);
    queryStatisticWithLOAD_DICTIONARY.addStatistics(QueryStatisticsConstants.LOAD_DICTIONARY, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithLOAD_DICTIONARY);
    queryStatisticWithRESULT_SIZE = new QueryStatistic();
    queryStatisticWithRESULT_SIZE.addStatistics(QueryStatisticsConstants.RESULT_SIZE, 5L);
    queryStatisticWithRESULT_SIZE.addCountStatistic(QueryStatisticsConstants.RESULT_SIZE, 5L);
    queryStatisticWithRESULT_SIZE.addStatistics(QueryStatisticsConstants.RESULT_SIZE, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithRESULT_SIZE);
    queryStatisticWithEXECUTOR_PART = new QueryStatistic();
    queryStatisticWithEXECUTOR_PART.addStatistics(QueryStatisticsConstants.EXECUTOR_PART, 5L);
    queryStatisticWithEXECUTOR_PART.addCountStatistic(QueryStatisticsConstants.EXECUTOR_PART, 5L);
    queryStatisticWithEXECUTOR_PART.addStatistics(QueryStatisticsConstants.EXECUTOR_PART, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithEXECUTOR_PART);
    queryStatisticWithTOTAL_BLOCKLET_NUM = new QueryStatistic();
    queryStatisticWithTOTAL_BLOCKLET_NUM
        .addStatistics(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, 5L);
    queryStatisticWithTOTAL_BLOCKLET_NUM
        .addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, 5L);
    queryStatisticWithTOTAL_BLOCKLET_NUM
        .addStatistics(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithTOTAL_BLOCKLET_NUM);
    queryStatisticWithVALID_SCAN_BLOCKLET_NUM = new QueryStatistic();
    queryStatisticWithVALID_SCAN_BLOCKLET_NUM
        .addStatistics(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM, 5L);
    queryStatisticWithVALID_SCAN_BLOCKLET_NUM
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM, 5L);
    queryStatisticWithVALID_SCAN_BLOCKLET_NUM
        .addStatistics(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM, 5L);
    queryStasticsRecorderImpl.logStatistics();
    queryStasticsRecorderImpl.recordStatistics(queryStatisticWithVALID_SCAN_BLOCKLET_NUM);
  }

  @Test public void testcollectExecutorStatistics() {
    assertNotNull(queryStasticsRecorderImpl.statisticsForTask(1, 1));
  }

}
