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

package org.apache.carbondata.core.util;

public class CarbonLoadStatisticsDummy implements LoadStatistics {
  private CarbonLoadStatisticsDummy() {

  }

  private static CarbonLoadStatisticsDummy carbonLoadStatisticsDummyInstance =
      new CarbonLoadStatisticsDummy();

  public static CarbonLoadStatisticsDummy getInstance() {
    return carbonLoadStatisticsDummyInstance;
  }

  @Override
  public void initPartitionInfo(String PartitionId) {

  }

  @Override
  public void recordDicShuffleAndWriteTime() {

  }

  @Override
  public void recordLoadCsvFilesToDfTime() {

  }

  @Override
  public void recordDictionaryValuesTotalTime(String partitionID,
      Long dictionaryValuesTotalTimeTimePoint) {

  }

  @Override
  public void recordCsvInputStepTime(String partitionID, Long csvInputStepTimePoint) {

  }

  @Override
  public void recordLruCacheLoadTime(double lruCacheLoadTime) {

  }

  @Override
  public void recordGeneratingDictionaryValuesTime(String partitionID,
      Long generatingDictionaryValuesTimePoint) {

  }

  @Override
  public void recordSortRowsStepTotalTime(String partitionID, Long sortRowsStepTotalTimePoint) {

  }

  @Override
  public void recordMdkGenerateTotalTime(String partitionID, Long mdkGenerateTotalTimePoint) {

  }

  @Override
  public void recordDictionaryValue2MdkAdd2FileTime(String partitionID,
      Long dictionaryValue2MdkAdd2FileTimePoint) {

  }

  @Override
  public void recordTotalRecords(long totalRecords) {

  }

  @Override
  public void recordHostBlockMap(String host, Integer numBlocks) {

  }

  @Override
  public void recordPartitionBlockMap(String partitionID, Integer numBlocks) {

  }

  @Override
  public void printStatisticsInfo(String partitionID) {

  }
}
