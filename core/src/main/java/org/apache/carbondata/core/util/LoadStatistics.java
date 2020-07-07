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

public interface LoadStatistics {
  //Init PartitionInfo
  void initPartitionInfo(String PartitionId);

  //Record the time
  void recordDicShuffleAndWriteTime();

  void recordLoadCsvFilesToDfTime();

  void recordDictionaryValuesTotalTime(String partitionID,
      Long dictionaryValuesTotalTimeTimePoint);

  void recordCsvInputStepTime(String partitionID,
      Long csvInputStepTimePoint);

  void recordLruCacheLoadTime(double lruCacheLoadTime);

  void recordGeneratingDictionaryValuesTime(String partitionID,
      Long generatingDictionaryValuesTimePoint);

  void recordSortRowsStepTotalTime(String partitionID,
      Long sortRowsStepTotalTimePoint);

  void recordMdkGenerateTotalTime(String partitionID,
      Long mdkGenerateTotalTimePoint);

  void recordDictionaryValue2MdkAdd2FileTime(String partitionID,
      Long dictionaryValue2MdkAdd2FileTimePoint);

  //Record the node blocks information map
  void recordHostBlockMap(String host, Integer numBlocks);

  //Record the partition blocks information map
  void recordPartitionBlockMap(String partitionID, Integer numBlocks);

  //Record total num of records processed
  void recordTotalRecords(long totalRecords);

  //Print the statistics information
  void printStatisticsInfo(String partitionID);

}
