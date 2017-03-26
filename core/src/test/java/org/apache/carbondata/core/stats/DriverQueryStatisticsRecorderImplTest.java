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

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertNull;

public class DriverQueryStatisticsRecorderImplTest {
  private static DriverQueryStatisticsRecorderImpl driverQueryStatisticsRecorderImpl = null;
  private static QueryStatistic queryStasticsWithSQL_PARSE = null;
  private static QueryStatistic queryStasticsWithLoadMetaData = null;
  private static QueryStatistic queryStasticsWithLOAD_BLOCKS_DRIVER = null;
  private static QueryStatistic queryStasticsWithBLOCK_ALLOCATION = null;
  private static QueryStatistic queryStasticsWithBLOCK_IDENTIFICATION = null;

  private static List<QueryStatistic> queryStatisticList;

  @BeforeClass public static void setUp() {
    driverQueryStatisticsRecorderImpl = DriverQueryStatisticsRecorderImpl.getInstance();
    queryStasticsWithSQL_PARSE = new QueryStatistic();
    queryStasticsWithLoadMetaData = new QueryStatistic();
    queryStasticsWithLOAD_BLOCKS_DRIVER = new QueryStatistic();
    queryStasticsWithBLOCK_ALLOCATION = new QueryStatistic();
    queryStasticsWithBLOCK_IDENTIFICATION = new QueryStatistic();
    queryStatisticList = new ArrayList<>();

  }

  @Test public void testCollectDriverStatistics() {
    queryStasticsWithLOAD_BLOCKS_DRIVER
        .addCountStatistic(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, 3);
    queryStasticsWithLOAD_BLOCKS_DRIVER
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, 5);
    queryStasticsWithLOAD_BLOCKS_DRIVER
        .addFixedTimeStatistic(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, 5);
    queryStatisticList.add(queryStasticsWithLOAD_BLOCKS_DRIVER);
    queryStasticsWithBLOCK_ALLOCATION
        .addCountStatistic(QueryStatisticsConstants.BLOCK_ALLOCATION, 3);
    queryStasticsWithBLOCK_ALLOCATION.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, 5);
    queryStasticsWithLOAD_BLOCKS_DRIVER
        .addFixedTimeStatistic(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, 5);
    queryStatisticList.add(queryStasticsWithBLOCK_ALLOCATION);
    queryStasticsWithBLOCK_IDENTIFICATION
        .addCountStatistic(QueryStatisticsConstants.BLOCK_IDENTIFICATION, 3);
    queryStasticsWithBLOCK_IDENTIFICATION
        .addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION, 5);
    queryStatisticList.add(queryStasticsWithBLOCK_IDENTIFICATION);
    queryStasticsWithLoadMetaData.addCountStatistic(QueryStatisticsConstants.LOAD_META, 3);
    queryStasticsWithLoadMetaData.addStatistics(QueryStatisticsConstants.LOAD_META, 5);
    queryStatisticList.add(queryStasticsWithLoadMetaData);
    queryStasticsWithSQL_PARSE.addCountStatistic(QueryStatisticsConstants.SQL_PARSE, 3);
    queryStasticsWithSQL_PARSE.addStatistics(QueryStatisticsConstants.SQL_PARSE, 5);
    queryStatisticList.add(queryStasticsWithSQL_PARSE);

    String result =
        driverQueryStatisticsRecorderImpl.collectDriverStatistics(queryStatisticList, "query1");
    assertNotNull(result);
  }

  @Test public void testCollectDriverStatisticsWithBlock_Allocation() {
    queryStatisticList.clear();
    queryStasticsWithBLOCK_ALLOCATION
        .addCountStatistic(QueryStatisticsConstants.BLOCK_ALLOCATION, 3);
    queryStasticsWithBLOCK_ALLOCATION.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, 5);
    queryStatisticList.add(queryStasticsWithBLOCK_ALLOCATION);
    String result =
        driverQueryStatisticsRecorderImpl.collectDriverStatistics(queryStatisticList, "query1");
    assertNull(result);
  }

  @Test public void testCollectDriverStatisticsWithBlock_AllocationAndBlock_Identification() {
    queryStatisticList.clear();
    queryStasticsWithBLOCK_ALLOCATION
        .addCountStatistic(QueryStatisticsConstants.BLOCK_ALLOCATION, 3);
    queryStasticsWithBLOCK_ALLOCATION.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, 5);
    queryStatisticList.add(queryStasticsWithBLOCK_ALLOCATION);
    queryStasticsWithBLOCK_IDENTIFICATION
        .addCountStatistic(QueryStatisticsConstants.BLOCK_IDENTIFICATION, 3);
    queryStasticsWithBLOCK_IDENTIFICATION
        .addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION, 5);
    queryStatisticList.add(queryStasticsWithBLOCK_IDENTIFICATION);
    String result =
        driverQueryStatisticsRecorderImpl.collectDriverStatistics(queryStatisticList, "query1");
    assertNotNull(result);
  }

  @Test
  public void testCollectDriverStatisticsWithBlock_AllocationAndBlock_IdentificationForException() {
    queryStatisticList.clear();

    driverQueryStatisticsRecorderImpl = DriverQueryStatisticsRecorderImpl.getInstance();

    String result =
        driverQueryStatisticsRecorderImpl.collectDriverStatistics(queryStatisticList, "query1");
    assertNull(result);
  }
}

