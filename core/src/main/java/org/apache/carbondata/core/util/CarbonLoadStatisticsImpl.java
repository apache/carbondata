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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;

/**
 * A util which provide methods used to record time information druing data loading.
 */
public class CarbonLoadStatisticsImpl implements LoadStatistics {
  private CarbonLoadStatisticsImpl() {

  }

  private static CarbonLoadStatisticsImpl carbonLoadStatisticsImplInstance =
          new CarbonLoadStatisticsImpl();

  public static CarbonLoadStatisticsImpl getInstance() {
    return carbonLoadStatisticsImplInstance;
  }

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(CarbonLoadStatisticsImpl.class.getName());

  /*
   *We only care about the earliest start time(EST) and the latest end time(LET) of different
   *threads, who does the same thing, LET - EST is the cost time of doing one thing using
   *multiple thread.
 */
  private long loadCsvfilesToDfStartTime = 0;
  private long loadCsvfilesToDfCostTime = 0;
  private long dicShuffleAndWriteFileTotalStartTime = 0;

  //LRU cache load one time
  private double lruCacheLoadTime = 0;

  //Generate surrogate keys total time for each partition:
  private ConcurrentHashMap<String, Long[]> parDictionaryValuesTotalTimeMap =
          new ConcurrentHashMap<String, Long[]>();
  private ConcurrentHashMap<String, Long[]> parCsvInputStepTimeMap =
          new ConcurrentHashMap<String, Long[]>();
  private ConcurrentHashMap<String, Long[]> parGeneratingDictionaryValuesTimeMap =
          new ConcurrentHashMap<String, Long[]>();

  //Sort rows step total time for each partition:
  private ConcurrentHashMap<String, Long[]> parSortRowsStepTotalTimeMap =
          new ConcurrentHashMap<String, Long[]>();

  //MDK generate total time for each partition:
  private ConcurrentHashMap<String, Long[]> parMdkGenerateTotalTimeMap =
          new ConcurrentHashMap<String, Long[]>();
  private ConcurrentHashMap<String, Long[]> parDictionaryValue2MdkAdd2FileTime =
          new ConcurrentHashMap<String, Long[]>();

  //Node block process information
  private ConcurrentHashMap<String, Integer> hostBlockMap =
          new ConcurrentHashMap<String, Integer>();

  //Partition block process information
  private ConcurrentHashMap<String, Integer> partitionBlockMap =
          new ConcurrentHashMap<String, Integer>();

  private long totalRecords = 0;
  private double totalTime = 0;

  @Override
  public void initPartitionInfo(String PartitionId) {
    parDictionaryValuesTotalTimeMap.put(PartitionId, new Long[2]);
    parCsvInputStepTimeMap.put(PartitionId, new Long[2]);
    parSortRowsStepTotalTimeMap.put(PartitionId, new Long[2]);
    parGeneratingDictionaryValuesTimeMap.put(PartitionId, new Long[2]);
    parMdkGenerateTotalTimeMap.put(PartitionId, new Long[2]);
    parDictionaryValue2MdkAdd2FileTime.put(PartitionId, new Long[2]);
  }

  //Record the time
  public void recordDicShuffleAndWriteTime() {
    long dicShuffleAndWriteTimePoint = System.currentTimeMillis();
    if (0 == dicShuffleAndWriteFileTotalStartTime) {
      dicShuffleAndWriteFileTotalStartTime = dicShuffleAndWriteTimePoint;
    }
  }

  public void recordLoadCsvfilesToDfTime() {
    long loadCsvfilesToDfTimePoint = System.currentTimeMillis();
    if (0 == loadCsvfilesToDfStartTime) {
      loadCsvfilesToDfStartTime = loadCsvfilesToDfTimePoint;
    }
    if (loadCsvfilesToDfTimePoint - loadCsvfilesToDfStartTime > loadCsvfilesToDfCostTime) {
      loadCsvfilesToDfCostTime = loadCsvfilesToDfTimePoint - loadCsvfilesToDfStartTime;
    }
  }

  public double getLruCacheLoadTime() {
    return lruCacheLoadTime;
  }

  public void recordDictionaryValuesTotalTime(String partitionID,
      Long dictionaryValuesTotalTimeTimePoint) {
    if (null != parDictionaryValuesTotalTimeMap.get(partitionID)) {
      if (null == parDictionaryValuesTotalTimeMap.get(partitionID)[0]) {
        parDictionaryValuesTotalTimeMap.get(partitionID)[0] = dictionaryValuesTotalTimeTimePoint;
      }
      if (null == parDictionaryValuesTotalTimeMap.get(partitionID)[1] ||
          dictionaryValuesTotalTimeTimePoint - parDictionaryValuesTotalTimeMap.get(partitionID)[0] >
              parDictionaryValuesTotalTimeMap.get(partitionID)[1]) {
        parDictionaryValuesTotalTimeMap.get(partitionID)[1] = dictionaryValuesTotalTimeTimePoint -
            parDictionaryValuesTotalTimeMap.get(partitionID)[0];
      }
    }
  }

  public void recordCsvInputStepTime(String partitionID,
      Long csvInputStepTimePoint) {
    if (null != parCsvInputStepTimeMap.get(partitionID)) {
      if (null == parCsvInputStepTimeMap.get(partitionID)[0]) {
        parCsvInputStepTimeMap.get(partitionID)[0] = csvInputStepTimePoint;
      }
      if (null == parCsvInputStepTimeMap.get(partitionID)[1] ||
              csvInputStepTimePoint - parCsvInputStepTimeMap.get(partitionID)[0] >
                      parCsvInputStepTimeMap.get(partitionID)[1]) {
        parCsvInputStepTimeMap.get(partitionID)[1] = csvInputStepTimePoint -
                parCsvInputStepTimeMap.get(partitionID)[0];
      }
    }
  }

  public void recordLruCacheLoadTime(double lruCacheLoadTime) {
    this.lruCacheLoadTime = lruCacheLoadTime;
  }

  public void recordGeneratingDictionaryValuesTime(String partitionID,
      Long generatingDictionaryValuesTimePoint) {
    if (null != parGeneratingDictionaryValuesTimeMap.get(partitionID)) {
      if (null == parGeneratingDictionaryValuesTimeMap.get(partitionID)[0]) {
        parGeneratingDictionaryValuesTimeMap.get(partitionID)[0] =
                generatingDictionaryValuesTimePoint;
      }
      if (null == parGeneratingDictionaryValuesTimeMap.get(partitionID)[1] ||
              generatingDictionaryValuesTimePoint - parGeneratingDictionaryValuesTimeMap
                      .get(partitionID)[0] > parGeneratingDictionaryValuesTimeMap
                      .get(partitionID)[1]) {
        parGeneratingDictionaryValuesTimeMap.get(partitionID)[1] =
                generatingDictionaryValuesTimePoint - parGeneratingDictionaryValuesTimeMap
                        .get(partitionID)[0];
      }
    }
  }

  public void recordSortRowsStepTotalTime(String partitionID,
                                          Long sortRowsStepTotalTimePoint) {
    if (null != parSortRowsStepTotalTimeMap.get(partitionID)) {
      if (null == parSortRowsStepTotalTimeMap.get(partitionID)[0]) {
        parSortRowsStepTotalTimeMap.get(partitionID)[0] = sortRowsStepTotalTimePoint;
      }
      if (null == parSortRowsStepTotalTimeMap.get(partitionID)[1] ||
              sortRowsStepTotalTimePoint - parSortRowsStepTotalTimeMap.get(partitionID)[0] >
                      parSortRowsStepTotalTimeMap.get(partitionID)[1]) {
        parSortRowsStepTotalTimeMap.get(partitionID)[1] = sortRowsStepTotalTimePoint -
                parSortRowsStepTotalTimeMap.get(partitionID)[0];
      }
    }
  }

  public void recordMdkGenerateTotalTime(String partitionID,
                                         Long mdkGenerateTotalTimePoint) {
    if (null != parMdkGenerateTotalTimeMap.get(partitionID)) {
      if (null == parMdkGenerateTotalTimeMap.get(partitionID)[0]) {
        parMdkGenerateTotalTimeMap.get(partitionID)[0] = mdkGenerateTotalTimePoint;
      }
      if (null == parMdkGenerateTotalTimeMap.get(partitionID)[1] ||
              mdkGenerateTotalTimePoint - parMdkGenerateTotalTimeMap.get(partitionID)[0] >
                      parMdkGenerateTotalTimeMap.get(partitionID)[1]) {
        parMdkGenerateTotalTimeMap.get(partitionID)[1] = mdkGenerateTotalTimePoint -
                parMdkGenerateTotalTimeMap.get(partitionID)[0];
      }
    }
  }

  public void recordDictionaryValue2MdkAdd2FileTime(String partitionID,
      Long dictionaryValue2MdkAdd2FileTimePoint) {
    if (null != parDictionaryValue2MdkAdd2FileTime.get(partitionID)) {
      if (null == parDictionaryValue2MdkAdd2FileTime.get(partitionID)[0]) {
        parDictionaryValue2MdkAdd2FileTime.get(partitionID)[0] =
                dictionaryValue2MdkAdd2FileTimePoint;
      }
      if (null == parDictionaryValue2MdkAdd2FileTime.get(partitionID)[1] ||
              dictionaryValue2MdkAdd2FileTimePoint - parDictionaryValue2MdkAdd2FileTime
                      .get(partitionID)[0] > parDictionaryValue2MdkAdd2FileTime
                      .get(partitionID)[1]) {
        parDictionaryValue2MdkAdd2FileTime.get(partitionID)[1] =
                dictionaryValue2MdkAdd2FileTimePoint - parDictionaryValue2MdkAdd2FileTime
                        .get(partitionID)[0];
      }
    }
  }

  //Record the node blocks information map
  public void recordHostBlockMap(String host, Integer numBlocks) {
    hostBlockMap.put(host, numBlocks);
  }

  //Record the partition blocks information map
  public void recordPartitionBlockMap(String partitionID, Integer numBlocks) {
    partitionBlockMap.put(partitionID, numBlocks);
  }

  public void recordTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  private double getLoadCsvfilesToDfTime() {
    return loadCsvfilesToDfCostTime / 1000.0;
  }

  private double getDictionaryValuesTotalTime(String partitionID) {
    return parDictionaryValuesTotalTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getCsvInputStepTime(String partitionID) {
    return parCsvInputStepTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getGeneratingDictionaryValuesTime(String partitionID) {
    return parGeneratingDictionaryValuesTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getSortRowsStepTotalTime(String partitionID) {
    return parSortRowsStepTotalTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getDictionaryValue2MdkAdd2FileTime(String partitionID) {
    return parDictionaryValue2MdkAdd2FileTime.get(partitionID)[1] / 1000.0;
  }

  //Get the hostBlockMap
  private ConcurrentHashMap<String, Integer> getHostBlockMap() {
    return hostBlockMap;
  }

  //Get the partitionBlockMap
  private ConcurrentHashMap<String, Integer> getPartitionBlockMap() {
    return partitionBlockMap;
  }

  //Speed calculate
  private long getTotalRecords() {
    return this.totalRecords;
  }

  private int getLoadSpeed() {
    return (int)(totalRecords / totalTime);
  }

  private int getReadCSVSpeed(String partitionID) {
    return (int)(totalRecords / getCsvInputStepTime(partitionID));
  }

  private int getGenSurKeySpeed(String partitionID) {
    return (int)(totalRecords / getGeneratingDictionaryValuesTime(partitionID));
  }

  private int getSortKeySpeed(String partitionID) {
    return (int)(totalRecords / getSortRowsStepTotalTime(partitionID));
  }

  private int getMDKSpeed(String partitionID) {
    return (int)(totalRecords / getDictionaryValue2MdkAdd2FileTime(partitionID));
  }

  private double getTotalTime(String partitionID) {
    this.totalTime = getLoadCsvfilesToDfTime() +
        getLruCacheLoadTime() + getDictionaryValuesTotalTime(partitionID) +
        getDictionaryValue2MdkAdd2FileTime(partitionID);
    return totalTime;
  }

  //Print the statistics information
  private void printDicGenStatisticsInfo() {
    double loadCsvfilesToDfTime = getLoadCsvfilesToDfTime();
    LOGGER.info("STAGE 1 ->Load csv to DataFrame and generate" +
            " block distinct values: " + loadCsvfilesToDfTime + "(s)");
  }

  private void printLruCacheLoadTimeInfo() {
    LOGGER.info("STAGE 2 ->LRU cache load: " + getLruCacheLoadTime() + "(s)");
  }

  private void printDictionaryValuesGenStatisticsInfo(String partitionID) {
    double dictionaryValuesTotalTime = getDictionaryValuesTotalTime(partitionID);
    LOGGER.info("STAGE 3 ->Total cost of gen dictionary values, sort and write to temp files: "
            + dictionaryValuesTotalTime + "(s)");
    double csvInputStepTime = getCsvInputStepTime(partitionID);
    double generatingDictionaryValuesTime = getGeneratingDictionaryValuesTime(partitionID);
    LOGGER.info("STAGE 3.1 ->  |_read csv file: " + csvInputStepTime + "(s)");
    LOGGER.info("STAGE 3.2 ->  |_transform to surrogate key: "
            + generatingDictionaryValuesTime + "(s)");
  }

  private void printSortRowsStepStatisticsInfo(String partitionID) {
    double sortRowsStepTotalTime = getSortRowsStepTotalTime(partitionID);
    LOGGER.info("STAGE 3.3 ->  |_sort rows and write to temp file: "
            + sortRowsStepTotalTime + "(s)");
  }

  private void printGenMdkStatisticsInfo(String partitionID) {
    double dictionaryValue2MdkAdd2FileTime = getDictionaryValue2MdkAdd2FileTime(partitionID);
    LOGGER.info("STAGE 4 ->Transform to MDK, compress and write fact files: "
            + dictionaryValue2MdkAdd2FileTime + "(s)");
  }

  //Print the node blocks information
  private void printHostBlockMapInfo() {
    LOGGER.info("========== BLOCK_INFO ==========");
    if (getHostBlockMap().size() > 0) {
      for (String host: getHostBlockMap().keySet()) {
        LOGGER.info("BLOCK_INFO ->Node host: " + host);
        LOGGER.info("BLOCK_INFO ->The block count in this node: " + getHostBlockMap().get(host));
      }
    } else if (getPartitionBlockMap().size() > 0) {
      for (String parID: getPartitionBlockMap().keySet()) {
        LOGGER.info("BLOCK_INFO ->Partition ID: " + parID);
        LOGGER.info("BLOCK_INFO ->The block count in this partition: " +
                getPartitionBlockMap().get(parID));
      }
    }
  }

  //Print the speed information
  private void printLoadSpeedInfo(String partitionID) {
    LOGGER.info("===============Load_Speed_Info===============");
    LOGGER.info("Total Num of Records Processed: " + getTotalRecords());
    LOGGER.info("Total Time Cost: " + getTotalTime(partitionID) + "(s)");
    LOGGER.info("Total Load Speed: " + getLoadSpeed() + "records/s");
    LOGGER.info("Read CSV Speed: " + getReadCSVSpeed(partitionID) + " records/s");
    LOGGER.info("Generate Surrogate Key Speed: " + getGenSurKeySpeed(partitionID) + " records/s");
    LOGGER.info("Sort Key/Write Temp Files Speed: " + getSortKeySpeed(partitionID) + " records/s");
    LOGGER.info("MDK Step Speed: " + getMDKSpeed(partitionID) + " records/s");
    LOGGER.info("=============================================");
  }

  public void printStatisticsInfo(String partitionID) {
    try {
      LOGGER.info("========== TIME_STATISTICS PartitionID: " + partitionID + "==========");
      printDicGenStatisticsInfo();
      printLruCacheLoadTimeInfo();
      printDictionaryValuesGenStatisticsInfo(partitionID);
      printSortRowsStepStatisticsInfo(partitionID);
      printGenMdkStatisticsInfo(partitionID);
      printHostBlockMapInfo();
      printLoadSpeedInfo(partitionID);
    } catch (Exception e) {
      LOGGER.error("Can't print Statistics Information");
    } finally {
      resetLoadStatistics();
    }
  }

  //Reset the load statistics values
  private void resetLoadStatistics() {
    loadCsvfilesToDfStartTime = 0;
    loadCsvfilesToDfCostTime = 0;
    dicShuffleAndWriteFileTotalStartTime = 0;
    lruCacheLoadTime = 0;
    totalRecords = 0;
    totalTime = 0;
    parDictionaryValuesTotalTimeMap.clear();
    parCsvInputStepTimeMap.clear();
    parSortRowsStepTotalTimeMap.clear();
    parGeneratingDictionaryValuesTimeMap.clear();
    parMdkGenerateTotalTimeMap.clear();
    parDictionaryValue2MdkAdd2FileTime.clear();
  }

}
