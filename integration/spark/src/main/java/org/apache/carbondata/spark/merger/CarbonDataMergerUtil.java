/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.merger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.integration.spark.merger.CompactionType;
import org.apache.carbondata.lcm.locks.ICarbonLock;
import org.apache.carbondata.lcm.status.SegmentStatusManager;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.spark.load.CarbonLoaderUtil;

/**
 * utility class for load merging.
 */
public final class CarbonDataMergerUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataMergerUtil.class.getName());

  private CarbonDataMergerUtil() {

  }

  /**
   * Returns the size of all the carbondata files present in the segment.
   * @param carbonFile
   * @return
   */
  private static long getSizeOfFactFileInLoad(CarbonFile carbonFile) {
    long factSize = 0;

    // carbon data file case.
    CarbonFile[] factFile = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        return CarbonTablePath.isCarbonDataFile(file.getName());
      }
    });

    for (CarbonFile fact : factFile) {
      factSize += fact.getSize();
    }

    return factSize;
  }

  /**
   * To check whether the merge property is enabled or not.
   *
   * @return
   */

  public static boolean checkIfAutoLoadMergingRequired() {
    // load merge is not supported as per new store format
    // moving the load merge check in early to avoid unnecessary load listing causing IOException
    // check whether carbons segment merging operation is enabled or not.
    // default will be false.
    String isLoadMergeEnabled = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
            CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE);
    if (isLoadMergeEnabled.equalsIgnoreCase("false")) {
      return false;
    }
    return true;
  }

  /**
   * Form the Name of the New Merge Folder
   *
   * @param segmentsToBeMergedList
   * @return
   */
  public static String getMergedLoadName(List<LoadMetadataDetails> segmentsToBeMergedList) {
    String firstSegmentName = segmentsToBeMergedList.get(0).getLoadName();
    if (firstSegmentName.contains(".")) {
      String beforeDecimal = firstSegmentName.substring(0, firstSegmentName.indexOf("."));
      String afterDecimal = firstSegmentName.substring(firstSegmentName.indexOf(".") + 1);
      int fraction = Integer.parseInt(afterDecimal) + 1;
      String mergedSegmentName = beforeDecimal + "." + fraction;
      return CarbonCommonConstants.LOAD_FOLDER + mergedSegmentName;
    } else {
      String mergeName = firstSegmentName + "." + 1;
      return CarbonCommonConstants.LOAD_FOLDER + mergeName;
    }

  }

  public static boolean updateLoadMetadataWithMergeStatus(List<LoadMetadataDetails> loadsToMerge,
      String metaDataFilepath, String MergedLoadName, CarbonLoadModel carbonLoadModel,
      String mergeLoadStartTime, CompactionType compactionType) {

    boolean tableStatusUpdationStatus = false;
    AbsoluteTableIdentifier absoluteTableIdentifier =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();
    ICarbonLock carbonLock = SegmentStatusManager.getTableStatusLock(absoluteTableIdentifier);

    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for the table " + carbonLoadModel.getDatabaseName() + "."
            + carbonLoadModel.getTableName() + " for table status updation ");

        CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
                absoluteTableIdentifier.getCarbonTableIdentifier());

        String statusFilePath = carbonTablePath.getTableStatusFilePath();

        LoadMetadataDetails[] loadDetails = SegmentStatusManager.readLoadMetadata(metaDataFilepath);

        String mergedLoadNumber = MergedLoadName.substring(
            MergedLoadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER)
                + CarbonCommonConstants.LOAD_FOLDER.length(), MergedLoadName.length());

        String modificationOrDeletionTimeStamp = CarbonLoaderUtil.readCurrentTime();
        for (LoadMetadataDetails loadDetail : loadDetails) {
          // check if this segment is merged.
          if (loadsToMerge.contains(loadDetail)) {
            // if the compacted load is deleted after the start of the compaction process,
            // then need to discard the compaction process and treat it as failed compaction.
            if (loadDetail.getLoadStatus()
                .equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE)) {
              LOGGER.error("Compaction is aborted as the segment " + loadDetail.getLoadName()
                  + " is deleted after the compaction is started.");
              return tableStatusUpdationStatus;
            }
            loadDetail.setLoadStatus(CarbonCommonConstants.SEGMENT_COMPACTED);
            loadDetail.setModificationOrdeletionTimesStamp(modificationOrDeletionTimeStamp);
            loadDetail.setMergedLoadName(mergedLoadNumber);
          }
        }

        // create entry for merged one.
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        loadMetadataDetails.setPartitionCount(carbonLoadModel.getPartitionId());
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS);
        String loadEnddate = CarbonLoaderUtil.readCurrentTime();
        loadMetadataDetails.setTimestamp(loadEnddate);
        loadMetadataDetails.setLoadName(mergedLoadNumber);
        loadMetadataDetails.setLoadStartTime(mergeLoadStartTime);
        loadMetadataDetails.setPartitionCount("0");
        // if this is a major compaction then set the segment as major compaction.
        if (compactionType == CompactionType.MAJOR_COMPACTION) {
          loadMetadataDetails.setMajorCompacted("true");
        }

        List<LoadMetadataDetails> updatedDetailsList = new ArrayList<>(Arrays.asList(loadDetails));

        // put the merged folder entry
        updatedDetailsList.add(loadMetadataDetails);

        try {
          SegmentStatusManager.writeLoadDetailsIntoFile(statusFilePath,
              updatedDetailsList.toArray(new LoadMetadataDetails[updatedDetailsList.size()]));
          tableStatusUpdationStatus = true;
        } catch (IOException e) {
          LOGGER.error("Error while writing metadata");
        }
      } else {
        LOGGER.error(
            "Could not able to obtain lock for table" + carbonLoadModel.getDatabaseName() + "."
                + carbonLoadModel.getTableName() + "for table status updation");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + carbonLoadModel
            .getDatabaseName() + "." + carbonLoadModel.getTableName());
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + carbonLoadModel.getDatabaseName() + "."
                + carbonLoadModel.getTableName() + " during table status updation");
      }
    }
    return tableStatusUpdationStatus;
  }

  /**
   * To identify which all segments can be merged.
   *
   * @param storeLocation
   * @param carbonLoadModel
   * @param compactionSize
   * @return
   */
  public static List<LoadMetadataDetails> identifySegmentsToBeMerged(String storeLocation,
      CarbonLoadModel carbonLoadModel, long compactionSize,
      List<LoadMetadataDetails> segments, CompactionType compactionType) {

    List sortedSegments = new ArrayList(segments);

    sortSegments(sortedSegments);

    // check preserve property and preserve the configured number of latest loads.

    List<LoadMetadataDetails> listOfSegmentsAfterPreserve =
        checkPreserveSegmentsPropertyReturnRemaining(sortedSegments);

    // filter the segments if the compaction based on days is configured.

    List<LoadMetadataDetails> listOfSegmentsLoadedInSameDateInterval =
        identifySegmentsToBeMergedBasedOnLoadedDate(listOfSegmentsAfterPreserve);
    List<LoadMetadataDetails> listOfSegmentsToBeMerged;
    // identify the segments to merge based on the Size of the segments across partition.
    if (compactionType.equals(CompactionType.MAJOR_COMPACTION)) {

      listOfSegmentsToBeMerged = identifySegmentsToBeMergedBasedOnSize(compactionSize,
          listOfSegmentsLoadedInSameDateInterval, carbonLoadModel, storeLocation);
    } else {

      listOfSegmentsToBeMerged =
          identifySegmentsToBeMergedBasedOnSegCount(listOfSegmentsLoadedInSameDateInterval);
    }

    return listOfSegmentsToBeMerged;
  }

  /**
   * Sorting of the segments.
   * @param segments
   */
  public static void sortSegments(List segments) {
    // sort the segment details.
    Collections.sort(segments, new Comparator<LoadMetadataDetails>() {
      @Override public int compare(LoadMetadataDetails seg1, LoadMetadataDetails seg2) {
        double seg1Id = Double.parseDouble(seg1.getLoadName());
        double seg2Id = Double.parseDouble(seg2.getLoadName());
        if (seg1Id - seg2Id < 0) {
          return -1;
        }
        if (seg1Id - seg2Id > 0) {
          return 1;
        }
        return 0;
      }
    });
  }

  /**
   * This method will return the list of loads which are loaded at the same interval.
   * This property is configurable.
   *
   * @param listOfSegmentsBelowThresholdSize
   * @return
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnLoadedDate(
      List<LoadMetadataDetails> listOfSegmentsBelowThresholdSize) {

    List<LoadMetadataDetails> loadsOfSameDate =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    long numberOfDaysAllowedToMerge = 0;
    try {
      numberOfDaysAllowedToMerge = Long.parseLong(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
              CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT));
      if (numberOfDaysAllowedToMerge < 0 || numberOfDaysAllowedToMerge > 100) {
        LOGGER.error(
            "The specified value for property " + CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT
                + " is incorrect."
                + " Correct value should be in range of 0 -100. Taking the default value.");
        numberOfDaysAllowedToMerge =
            Long.parseLong(CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT);
      }

    } catch (NumberFormatException e) {
      numberOfDaysAllowedToMerge =
          Long.parseLong(CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT);
    }
    // if true then process loads according to the load date.
    if (numberOfDaysAllowedToMerge > 0) {

      // filter loads based on the loaded date
      boolean first = true;
      Date segDate1 = null;
      SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
      for (LoadMetadataDetails segment : listOfSegmentsBelowThresholdSize) {

        if (first) {
          segDate1 = initializeFirstSegment(loadsOfSameDate, segment, sdf);
          first = false;
          continue;
        }
        String segmentDate = segment.getLoadStartTime();
        Date segDate2 = null;
        try {
          segDate2 = sdf.parse(segmentDate);
        } catch (ParseException e) {
          LOGGER.error("Error while parsing segment start time" + e.getMessage());
        }

        if (isTwoDatesPresentInRequiredRange(segDate1, segDate2, numberOfDaysAllowedToMerge)) {
          loadsOfSameDate.add(segment);
        }
        // if the load is beyond merged date.
        // then reset everything and continue search for loads.
        else if (loadsOfSameDate.size() < 2) {
          loadsOfSameDate.clear();
          // need to add the next segment as first and  to check further
          segDate1 = initializeFirstSegment(loadsOfSameDate, segment, sdf);
        } else { // case where a load is beyond merge date and there is at least 2 loads to merge.
          break;
        }
      }
    } else {
      return listOfSegmentsBelowThresholdSize;
    }

    return loadsOfSameDate;
  }

  /**
   * @param loadsOfSameDate
   * @param segment
   * @return
   */
  private static Date initializeFirstSegment(List<LoadMetadataDetails> loadsOfSameDate,
      LoadMetadataDetails segment, SimpleDateFormat sdf) {
    String baselineLoadStartTime = segment.getLoadStartTime();
    Date segDate1 = null;
    try {
      segDate1 = sdf.parse(baselineLoadStartTime);
    } catch (ParseException e) {
      LOGGER.error("Error while parsing segment start time" + e.getMessage());
    }
    loadsOfSameDate.add(segment);
    return segDate1;
  }

  /**
   * Method to check if the load dates are complied to the configured dates.
   *
   * @param segDate1
   * @param segDate2
   * @return
   */
  private static boolean isTwoDatesPresentInRequiredRange(Date segDate1, Date segDate2,
      long numberOfDaysAllowedToMerge) {
    if(segDate1 == null || segDate2 == null) {
      return false;
    }
    // take 1 st date add the configured days .
    Calendar cal1 = Calendar.getInstance();
    cal1.set(segDate1.getYear(), segDate1.getMonth(), segDate1.getDate());
    Calendar cal2 = Calendar.getInstance();
    cal2.set(segDate2.getYear(), segDate2.getMonth(), segDate2.getDate());

    long diff = cal2.getTimeInMillis() - cal1.getTimeInMillis();

    if ((diff / (24 * 60 * 60 * 1000)) < numberOfDaysAllowedToMerge) {
      return true;
    }
    return false;
  }

  /**
   * Identify the segments to be merged based on the Size in case of Major compaction.
   *
   * @param compactionSize
   * @param listOfSegmentsAfterPreserve
   * @param carbonLoadModel
   * @param storeLocation
   * @return
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnSize(
      long compactionSize, List<LoadMetadataDetails> listOfSegmentsAfterPreserve,
      CarbonLoadModel carbonLoadModel, String storeLocation) {

    List<LoadMetadataDetails> segmentsToBeMerged =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    CarbonTableIdentifier tableIdentifier =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getCarbonTableIdentifier();


    // total length
    long totalLength = 0;

    // check size of each segment , sum it up across partitions
    for (LoadMetadataDetails segment : listOfSegmentsAfterPreserve) {

      String segId = segment.getLoadName();
      // variable to store one  segment size across partition.
      long sizeOfOneSegmentAcrossPartition =
          getSizeOfSegment(storeLocation, tableIdentifier, segId);

      // if size of a segment is greater than the Major compaction size. then ignore it.
      if (sizeOfOneSegmentAcrossPartition > (compactionSize * 1024 * 1024)) {
        // if already 2 segments have been found for merging then stop scan here and merge.
        if (segmentsToBeMerged.size() > 1) {
          break;
        } else { // if only one segment is found then remove the earlier one in list.
          // reset the total length to 0.
          segmentsToBeMerged = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
          totalLength = 0;
          continue;
        }
      }

      totalLength += sizeOfOneSegmentAcrossPartition;

      // in case of major compaction the size doesnt matter. all the segments will be merged.
      if (totalLength < (compactionSize * 1024 * 1024)) {
        segmentsToBeMerged.add(segment);
      } else { // if already 2 segments have been found for merging then stop scan here and merge.
        if (segmentsToBeMerged.size() > 1) {
          break;
        } else { // if only one segment is found then remove the earlier one in list and put this.
          // reset the total length to the current identified segment.
          segmentsToBeMerged = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
          segmentsToBeMerged.add(segment);
          totalLength = sizeOfOneSegmentAcrossPartition;
        }
      }

    }

    return segmentsToBeMerged;
  }

  /**
   * For calculating the size of the specified segment
   * @param storeLocation
   * @param tableIdentifier
   * @param segId
   * @return
   */
  private static long getSizeOfSegment(String storeLocation,
      CarbonTableIdentifier tableIdentifier, String segId) {
    String loadPath = CarbonLoaderUtil
        .getStoreLocation(storeLocation, tableIdentifier, segId, "0");
    CarbonFile segmentFolder =
        FileFactory.getCarbonFile(loadPath, FileFactory.getFileType(loadPath));
    return getSizeOfFactFileInLoad(segmentFolder);
  }

  /**
   * Identify the segments to be merged based on the segment count
   *
   * @param listOfSegmentsAfterPreserve
   * @return
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnSegCount(
      List<LoadMetadataDetails> listOfSegmentsAfterPreserve) {

    List<LoadMetadataDetails> mergedSegments =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<LoadMetadataDetails> unMergedSegments =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    int[] noOfSegmentLevelsCount =
        CarbonProperties.getInstance().getCompactionSegmentLevelCount();

    int level1Size = 0;
    int level2Size = 0;
    int size = noOfSegmentLevelsCount.length;

    if (size >= 2) {
      level1Size = noOfSegmentLevelsCount[0];
      level2Size = noOfSegmentLevelsCount[1];
    } else if (size == 1) {
      level1Size = noOfSegmentLevelsCount[0];
    }

    int unMergeCounter = 0;
    int mergeCounter = 0;

    // check size of each segment , sum it up across partitions
    for (LoadMetadataDetails segment : listOfSegmentsAfterPreserve) {

      String segName = segment.getLoadName();

      // if a segment is already merged 2 levels then it s name will become .2
      // need to exclude those segments from minor compaction.
      // if a segment is major compacted then should not be considered for minor.
      if (segName.endsWith(CarbonCommonConstants.LEVEL2_COMPACTION_INDEX) || (
          segment.isMajorCompacted() != null && segment.isMajorCompacted()
              .equalsIgnoreCase("true"))) {
        continue;
      }

      // check if the segment is merged or not

      if (!isMergedSegment(segName)) {
        //if it is an unmerged segment then increment counter
        unMergeCounter++;
        unMergedSegments.add(segment);
        if (unMergeCounter == (level1Size)) {
          return unMergedSegments;
        }
      } else {
        mergeCounter++;
        mergedSegments.add(segment);
        if (mergeCounter == (level2Size)) {
          return mergedSegments;
        }
      }
    }
    return new ArrayList<>(0);
  }

  /**
   * To check if the segment is merged or not.
   * @param segName
   * @return
   */
  private static boolean isMergedSegment(String segName) {
    if(segName.contains(".")){
      return true;
    }
    return false;
  }

  /**
   * checks number of loads to be preserved and returns remaining valid segments
   *
   * @param segments
   * @return
   */
  private static List<LoadMetadataDetails> checkPreserveSegmentsPropertyReturnRemaining(
      List<LoadMetadataDetails> segments) {

    int numberOfSegmentsToBePreserved = 0;
    // check whether the preserving of the segments from merging is enabled or not.
    // get the number of loads to be preserved.
    numberOfSegmentsToBePreserved =
        CarbonProperties.getInstance().getNumberOfSegmentsToBePreserved();
    // get the number of valid segments and retain the latest loads from merging.
    return CarbonDataMergerUtil
        .getValidLoadDetailsWithRetaining(segments, numberOfSegmentsToBePreserved);
  }

  /**
   * Retain the number of segments which are to be preserved and return the remaining list of
   * segments.
   *
   * @param loadMetadataDetails
   * @param numberOfSegToBeRetained
   * @return
   */
  private static List<LoadMetadataDetails> getValidLoadDetailsWithRetaining(
      List<LoadMetadataDetails> loadMetadataDetails, int numberOfSegToBeRetained) {

    List<LoadMetadataDetails> validList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails segment : loadMetadataDetails) {
      if (segment.getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
          || segment.getLoadStatus()
          .equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) || segment
          .getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_UPDATE)) {
        validList.add(segment);
      }
    }

    // check if valid list is big enough for removing the number of seg to be retained.
    // last element
    int removingIndex = validList.size() - 1;

    for (int i = validList.size(); i > 0; i--) {
      if (numberOfSegToBeRetained == 0) {
        break;
      }
      // remove last segment
      validList.remove(removingIndex--);
      numberOfSegToBeRetained--;
    }
    return validList;

  }

  /**
   * This will give the compaction sizes configured based on compaction type.
   *
   * @param compactionType
   * @return
   */
  public static long getCompactionSize(CompactionType compactionType) {

    long compactionSize = 0;
    switch (compactionType) {
      case MAJOR_COMPACTION:
        compactionSize = CarbonProperties.getInstance().getMajorCompactionSize();
        break;
      default: // this case can not come.
    }
    return compactionSize;
  }

  /**
   * For getting the comma separated valid segments for merging.
   *
   * @param loadMetadataDetails
   * @return
   */
  public static String getValidSegments(List<LoadMetadataDetails> loadMetadataDetails) {
    StringBuilder builder = new StringBuilder();
    for (LoadMetadataDetails segment : loadMetadataDetails) {
      //check if this load is an already merged load.
      if (null != segment.getMergedLoadName()) {
        builder.append(segment.getMergedLoadName() + ",");
      } else {
        builder.append(segment.getLoadName() + ",");
      }
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  /**
   * Combining the list of maps to one map.
   *
   * @param mapsOfNodeBlockMapping
   * @return
   */
  public static Map<String, List<TableBlockInfo>> combineNodeBlockMaps(
      List<Map<String, List<TableBlockInfo>>> mapsOfNodeBlockMapping) {

    Map<String, List<TableBlockInfo>> combinedMap =
        new HashMap<String, List<TableBlockInfo>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // traverse list of maps.
    for (Map<String, List<TableBlockInfo>> eachMap : mapsOfNodeBlockMapping) {
      // traverse inside each map.
      for (Map.Entry<String, List<TableBlockInfo>> eachEntry : eachMap.entrySet()) {

        String node = eachEntry.getKey();
        List<TableBlockInfo> blocks = eachEntry.getValue();

        // if already that node detail exist in the combined map.
        if (null != combinedMap.get(node)) {
          List<TableBlockInfo> blocksAlreadyPresent = combinedMap.get(node);
          blocksAlreadyPresent.addAll(blocks);
        } else { // if its not present in map then put to map.
          combinedMap.put(node, blocks);
        }
      }
    }
    return combinedMap;
  }

  /**
   * Removing the already merged segments from list.
   */
  public static List<LoadMetadataDetails> filterOutNewlyAddedSegments(
      List<LoadMetadataDetails> segments,
      LoadMetadataDetails lastSeg) {

    // take complete list of segments.
    List<LoadMetadataDetails> list = new ArrayList<>(segments);
    // sort list
    CarbonDataMergerUtil.sortSegments(list);

    // first filter out newly added segments.
    return list.subList(0, list.indexOf(lastSeg) + 1);

  }

}
