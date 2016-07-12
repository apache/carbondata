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

package org.carbondata.core.util;

import java.util.concurrent.ConcurrentHashMap;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;

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

  private final LogService LOGGER =
          LogServiceFactory.getLogService(CarbonLoadStatisticsImpl.class.getName());

  /*
   *We only care about the earliest start time(EST) and the latest end time(LET) of different
   *threads, who does the same thing, LET - EST is the cost time of doing one thing using
   *multiple thread.
 */
  private long loadCsvfilesToDfStartTime = 0;
  private long loadCsvfilesToDfCostTime = 0;
  private long dicShuffleAndWriteFileTotalStartTime = 0;
  private long dicShuffleAndWriteFileTotalCostTime = 0;

  //Due to thread thread blocking in each task, we only record the max
  //csvlDicShuffle Time of each single thread
  private long csvlDicShuffleCostTime = 0;
  //Due to thread thread blocking in each task, we only record the max
  //dicWriteFile Time of each single thread
  private long dicWriteFileCostTime = 0;

  //LRU cache load one time
  private double lruCacheLoadTime = 0;

  //Generate surrogate keys total time for each partition:
  private ConcurrentHashMap<String, Long[]> parSurrogatekeysTotalTimeMap =
          new ConcurrentHashMap<String, Long[]>();
  private ConcurrentHashMap<String, Long[]> parCsvInputStepTimeMap =
          new ConcurrentHashMap<String, Long[]>();
  private ConcurrentHashMap<String, Long[]> parGeneratingSurrogatekeysTimeMap =
          new ConcurrentHashMap<String, Long[]>();

  //Sort rows step total time for each partition:
  private ConcurrentHashMap<String, Long[]> parSortRowsStepTotalTimeMap =
          new ConcurrentHashMap<String, Long[]>();

  //MDK generate total time for each partition:
  private ConcurrentHashMap<String, Long[]> parMdkGenerateTotalTimeMap =
          new ConcurrentHashMap<String, Long[]>();
  private ConcurrentHashMap<String, Long[]> parSurrogateKey2MdkAdd2FileTime =
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
  public void  initPartitonInfo(String PartitionId) {
    parSurrogatekeysTotalTimeMap.put(PartitionId, new Long[2]);
    parCsvInputStepTimeMap.put(PartitionId, new Long[2]);
    parSortRowsStepTotalTimeMap.put(PartitionId, new Long[2]);
    parGeneratingSurrogatekeysTimeMap.put(PartitionId, new Long[2]);
    parMdkGenerateTotalTimeMap.put(PartitionId, new Long[2]);
    parSurrogateKey2MdkAdd2FileTime.put(PartitionId, new Long[2]);
  }

  //Record the time
  public void recordGlobalDicGenTotalTime(Long glblDicTimePoint) {
    if (0 == dicShuffleAndWriteFileTotalStartTime) {
      dicShuffleAndWriteFileTotalStartTime = glblDicTimePoint;
    }
    if (glblDicTimePoint - dicShuffleAndWriteFileTotalStartTime >
            dicShuffleAndWriteFileTotalCostTime) {
      dicShuffleAndWriteFileTotalCostTime = glblDicTimePoint - dicShuffleAndWriteFileTotalStartTime;
    }
  }

  public void recordLoadCsvfilesToDfTime() {
    Long loadCsvfilesToDfTimePoint = System.currentTimeMillis();
    if (0 == loadCsvfilesToDfStartTime) {
      loadCsvfilesToDfStartTime = loadCsvfilesToDfTimePoint;
    }
    if (loadCsvfilesToDfTimePoint - loadCsvfilesToDfStartTime > loadCsvfilesToDfCostTime) {
      loadCsvfilesToDfCostTime = loadCsvfilesToDfTimePoint - loadCsvfilesToDfStartTime;
    }
  }

  public void recordCsvlDicShuffleMaxTime(Long csvlDicShuffleTimePart) {
    if (csvlDicShuffleTimePart > csvlDicShuffleCostTime) {
      csvlDicShuffleCostTime = csvlDicShuffleTimePart;
    }
  }

  public void recordDicWriteFileMaxTime(Long dicWriteFileTimePart) {
    if (dicWriteFileTimePart > dicWriteFileCostTime) {
      dicWriteFileCostTime = dicWriteFileTimePart;
    }
  }


  public double getLruCacheLoadTime() {
    return lruCacheLoadTime;
  }

  public void recordSurrogatekeysTotalTime(String partitionID,
      Long surrogatekeysTotalTimeTimePoint) {
    if (null != parSurrogatekeysTotalTimeMap.get(partitionID)) {
      if (null == parSurrogatekeysTotalTimeMap.get(partitionID)[0]) {
        parSurrogatekeysTotalTimeMap.get(partitionID)[0] = surrogatekeysTotalTimeTimePoint;
      }
      if (null == parSurrogatekeysTotalTimeMap.get(partitionID)[1] ||
          surrogatekeysTotalTimeTimePoint - parSurrogatekeysTotalTimeMap.get(partitionID)[0] >
              parSurrogatekeysTotalTimeMap.get(partitionID)[1]) {
        parSurrogatekeysTotalTimeMap.get(partitionID)[1] = surrogatekeysTotalTimeTimePoint -
            parSurrogatekeysTotalTimeMap.get(partitionID)[0];
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

  public void recordGeneratingSurrogatekeysTime(String partitionID,
                                                Long generatingSurrogatekeysTimePoint) {
    if (null != parGeneratingSurrogatekeysTimeMap.get(partitionID)) {
      if (null == parGeneratingSurrogatekeysTimeMap.get(partitionID)[0]) {
        parGeneratingSurrogatekeysTimeMap.get(partitionID)[0] = generatingSurrogatekeysTimePoint;
      }
      if (null == parGeneratingSurrogatekeysTimeMap.get(partitionID)[1] ||
              generatingSurrogatekeysTimePoint - parGeneratingSurrogatekeysTimeMap
                      .get(partitionID)[0] > parGeneratingSurrogatekeysTimeMap
                      .get(partitionID)[1]) {
        parGeneratingSurrogatekeysTimeMap.get(partitionID)[1] = generatingSurrogatekeysTimePoint -
                parGeneratingSurrogatekeysTimeMap.get(partitionID)[0];
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

  public void recordSurrogateKey2MdkAdd2FileTime(String partitionID,
                                                 Long surrogateKey2MdkAdd2FileTimePoint) {
    if (null != parSurrogateKey2MdkAdd2FileTime.get(partitionID)) {
      if (null == parSurrogateKey2MdkAdd2FileTime.get(partitionID)[0]) {
        parSurrogateKey2MdkAdd2FileTime.get(partitionID)[0] = surrogateKey2MdkAdd2FileTimePoint;
      }
      if (null == parSurrogateKey2MdkAdd2FileTime.get(partitionID)[1] ||
              surrogateKey2MdkAdd2FileTimePoint - parSurrogateKey2MdkAdd2FileTime
                      .get(partitionID)[0] > parSurrogateKey2MdkAdd2FileTime.get(partitionID)[1]) {
        parSurrogateKey2MdkAdd2FileTime.get(partitionID)[1] = surrogateKey2MdkAdd2FileTimePoint -
                parSurrogateKey2MdkAdd2FileTime.get(partitionID)[0];
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

  //Get the time
  private double getDicShuffleAndWriteFileTotalTime() {
    return dicShuffleAndWriteFileTotalCostTime / 1000.0;
  }

  private double getLoadCsvfilesToDfTime() {
    return loadCsvfilesToDfCostTime / 1000.0;
  }

  private double getCsvlDicShuffleMaxTime() {
    return csvlDicShuffleCostTime / 1000.0;
  }

  private double getDicWriteFileMaxTime() {
    return dicWriteFileCostTime / 1000.0;
  }

  private double getSurrogatekeysTotalTime(String partitionID) {
    return parSurrogatekeysTotalTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getCsvInputStepTime(String partitionID) {
    return parCsvInputStepTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getGeneratingSurrogatekeysTime(String partitionID) {
    return parGeneratingSurrogatekeysTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getSortRowsStepTotalTime(String partitionID) {
    return parSortRowsStepTotalTimeMap.get(partitionID)[1] / 1000.0;
  }

  private double getSurrogateKey2MdkAdd2FileTime(String partitionID) {
    return parSurrogateKey2MdkAdd2FileTime.get(partitionID)[1] / 1000.0;
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

  private int getGenDicSpeed() {
    return (int)(totalRecords / getLoadCsvfilesToDfTime() + getDicShuffleAndWriteFileTotalTime());
  }

  private int getReadCSVSpeed(String partitionID) {
    return (int)(totalRecords / getCsvInputStepTime(partitionID));
  }

  private int getGenSurKeySpeed(String partitionID) {
    return (int)(totalRecords / getGeneratingSurrogatekeysTime(partitionID));
  }

  private int getSortKeySpeed(String partitionID) {
    return (int)(totalRecords / getSortRowsStepTotalTime(partitionID));
  }

  private int getMDKSpeed(String partitionID) {
    return (int)(totalRecords / getSurrogateKey2MdkAdd2FileTime(partitionID));
  }

  private double getTotalTime(String partitionID) {
    this.totalTime = getLoadCsvfilesToDfTime() + getDicShuffleAndWriteFileTotalTime() +
        getLruCacheLoadTime() + getSurrogatekeysTotalTime(partitionID) +
        getSurrogateKey2MdkAdd2FileTime(partitionID);
    return totalTime;
  }

  //Print the statistics information
  private void printDicGenStatisticsInfo() {
    double loadCsvfilesToDfTime = getLoadCsvfilesToDfTime();
    LOGGER.audit("STAGE 1 ->Load csv to DataFrame and generate" +
            " block distinct values: " + loadCsvfilesToDfTime + "(s)");
    double dicShuffleAndWriteFileTotalTime = getDicShuffleAndWriteFileTotalTime();
    LOGGER.audit("STAGE 2 ->Global dict shuffle and write dict file: " +
            + dicShuffleAndWriteFileTotalTime + "(s)");
    double csvShuffleMaxTime = getCsvlDicShuffleMaxTime();
    LOGGER.audit("STAGE 2.1 ->  |_maximum distinct column shuffle time: "
            + csvShuffleMaxTime + "(s)");
    double dicWriteFileMaxTime = getDicWriteFileMaxTime();
    LOGGER.audit("STAGE 2.2 ->  |_maximum distinct column write dict file time: "
            + dicWriteFileMaxTime + "(s)");
  }

  private void printLruCacheLoadTimeInfo() {
    LOGGER.audit("STAGE 3 ->LRU cache load: " + getLruCacheLoadTime() + "(s)");
  }

  private void printSurrogatekeysGenStatisticsInfo(String partitionID) {
    double surrogatekeysTotalTime = getSurrogatekeysTotalTime(partitionID);
    LOGGER.audit("STAGE 4 ->Total cost of gen surrogate key, sort and write to temp files: "
            + surrogatekeysTotalTime + "(s)");
    double csvInputStepTime = getCsvInputStepTime(partitionID);
    double generatingSurrogatekeysTime = getGeneratingSurrogatekeysTime(partitionID);
    LOGGER.audit("STAGE 4.1 ->  |_read csv file: " + csvInputStepTime + "(s)");
    LOGGER.audit("STAGE 4.2 ->  |_transform to surrogate key: "
            + generatingSurrogatekeysTime + "(s)");
  }

  private void printSortRowsStepStatisticsInfo(String partitionID) {
    double sortRowsStepTotalTime = getSortRowsStepTotalTime(partitionID);
    LOGGER.audit("STAGE 4.3 ->  |_sort rows and write to temp file: "
            + sortRowsStepTotalTime + "(s)");
  }

  private void printGenMdkStatisticsInfo(String partitionID) {
    double surrogateKey2MdkAdd2FileTime = getSurrogateKey2MdkAdd2FileTime(partitionID);
    LOGGER.audit("STAGE 5 ->Tansform to MDK, compress and write fact files: "
            + surrogateKey2MdkAdd2FileTime + "(s)");
  }

  //Print the node blocks information
  private void printHostBlockMapInfo() {
    LOGGER.audit("========== BLOCK_INFO ==========");
    if (getHostBlockMap().size() > 0) {
      for (String host: getHostBlockMap().keySet()) {
        LOGGER.audit("BLOCK_INFO ->Node host: " + host);
        LOGGER.audit("BLOCK_INFO ->The block count in this node: " + getHostBlockMap().get(host));
      }
    } else if (getPartitionBlockMap().size() > 0) {
      for (String parID: getPartitionBlockMap().keySet()) {
        LOGGER.audit("BLOCK_INFO ->Partition ID: " + parID);
        LOGGER.audit("BLOCK_INFO ->The block count in this partition: " +
                getPartitionBlockMap().get(parID));
      }
    }
  }

  //Print the speed information
  private void printLoadSpeedInfo(String partitionID) {
    LOGGER.audit("===============Load_Speed_Info===============");
    LOGGER.audit("Total Num of Records Processed: " + getTotalRecords());
    LOGGER.audit("Total Time Cost: " + getTotalTime(partitionID) + "(s)");
    LOGGER.audit("Total Load Speed: " + getLoadSpeed() + "records/s");
    LOGGER.audit("Generate Dictionaries Speed: " + getGenDicSpeed() + "records/s");
    LOGGER.audit("Read CSV Speed: " + getReadCSVSpeed(partitionID) + " records/s");
    LOGGER.audit("Generate Surrogate Key Speed: " + getGenSurKeySpeed(partitionID) + " records/s");
    LOGGER.audit("Sort Key/Write Temp Files Speed: " + getSortKeySpeed(partitionID) + " records/s");
    LOGGER.audit("MDK Step Speed: " + getMDKSpeed(partitionID) + " records/s");
    LOGGER.audit("=============================================");
  }

  public void printStatisticsInfo(String partitionID) {
    try {
      LOGGER.audit("========== TIME_STATISTICS PartitionID: " + partitionID + "==========");
      printDicGenStatisticsInfo();
      printLruCacheLoadTimeInfo();
      printSurrogatekeysGenStatisticsInfo(partitionID);
      printSortRowsStepStatisticsInfo(partitionID);
      printGenMdkStatisticsInfo(partitionID);
      printHostBlockMapInfo();
      printLoadSpeedInfo(partitionID);
    } catch (Exception e) {
      LOGGER.audit("Can't print Statistics Information");
    }
  }

}
