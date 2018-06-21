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

package org.apache.carbondata.processing.merger;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.reader.CarbonDeleteFilesDataReader;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

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

      @Override
      public boolean accept(CarbonFile file) {
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
   * @Params carbonTable
   * @return
   */
  public static boolean checkIfAutoLoadMergingRequired(CarbonTable carbonTable) {
    // load merge is not supported as per new store format
    // moving the load merge check in early to avoid unnecessary load listing causing IOException
    // check whether carbons segment merging operation is enabled or not.
    // default will be false.
    Map<String, String> tblProps = carbonTable.getTableInfo().getFactTable().getTableProperties();

    String isLoadMergeEnabled = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
                    CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE);
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE)) {
      isLoadMergeEnabled = tblProps.get(CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE);
    }
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


  /**
   * Update Both Segment Update Status and Table Status for the case of IUD Delete
   * delta compaction.
   *
   * @param loadsToMerge
   * @param metaDataFilepath
   * @param carbonLoadModel
   * @return
   */
  public static boolean updateLoadMetadataIUDUpdateDeltaMergeStatus(
      List<LoadMetadataDetails> loadsToMerge, String metaDataFilepath,
      CarbonLoadModel carbonLoadModel, List<Segment> segmentFilesToBeUpdated) {

    boolean status = false;
    boolean updateLockStatus = false;
    boolean tableLockStatus = false;

    String timestamp = "" + carbonLoadModel.getFactTimeStamp();

    List<String> updatedDeltaFilesList = null;

    // This routine updateLoadMetadataIUDCompactionMergeStatus is suppose to update
    // two files as it is only called during IUD_UPDDEL_DELTA_COMPACTION. Along with
    // Table Status Metadata file (For Update Block Compaction) it has to update the
    // Table Update Status Metadata File (For corresponding Delete Delta File).
    // As the IUD_UPDDEL_DELTA_COMPACTION going to write in the same segment therefore in
    // A) Table Update Status Metadata File (Block Level)
    //      * For each blocks which is being compacted Mark 'Compacted' as the Status.
    // B) Table Status Metadata file (Segment Level)
    //      * loadStatus won't be changed to "compacted'
    //      * UpdateDeltaStartTime and UpdateDeltaEndTime will be both set to current
    //        timestamp (which is being passed from driver)
    // First the Table Update Status Metadata File should be updated as we need to get
    // the updated blocks for the segment from Table Status Metadata Update Delta Start and
    // End Timestamp.

    // Table Update Status Metadata Update.
    AbsoluteTableIdentifier identifier =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();

    SegmentUpdateStatusManager segmentUpdateStatusManager =
        new SegmentUpdateStatusManager(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable());

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);

    ICarbonLock updateLock = segmentUpdateStatusManager.getTableUpdateStatusLock();
    ICarbonLock statusLock = segmentStatusManager.getTableStatusLock();

    // Update the Compacted Blocks with Compacted Status.
    try {
      updatedDeltaFilesList = segmentUpdateStatusManager
          .getUpdateDeltaFiles(loadsToMerge.get(0).getLoadName());
    } catch (Exception e) {
      LOGGER.error("Error while getting the Update Delta Blocks.");
      status = false;
      return status;
    }

    if (updatedDeltaFilesList.size() > 0) {
      try {
        updateLockStatus = updateLock.lockWithRetries();
        tableLockStatus = statusLock.lockWithRetries();

        List<String> blockNames = new ArrayList<>(updatedDeltaFilesList.size());

        for (String compactedBlocks : updatedDeltaFilesList) {
          // Try to BlockName
          int endIndex = compactedBlocks.lastIndexOf(File.separator);
          String blkNoExt =
              compactedBlocks.substring(endIndex + 1, compactedBlocks.lastIndexOf("-"));
          blockNames.add(blkNoExt);
        }

        if (updateLockStatus && tableLockStatus) {

          SegmentUpdateDetails[] updateLists = segmentUpdateStatusManager
              .readLoadMetadata();

          for (String compactedBlocks : blockNames) {
            // Check is the compactedBlocks name matches with oldDetails
            for (int i = 0; i < updateLists.length; i++) {
              if (updateLists[i].getBlockName().equalsIgnoreCase(compactedBlocks)
                  && updateLists[i].getSegmentStatus() != SegmentStatus.COMPACTED
                  && updateLists[i].getSegmentStatus() != SegmentStatus.MARKED_FOR_DELETE) {
                updateLists[i].setSegmentStatus(SegmentStatus.COMPACTED);
              }
            }
          }

          LoadMetadataDetails[] loadDetails =
              SegmentStatusManager.readLoadMetadata(metaDataFilepath);

          for (LoadMetadataDetails loadDetail : loadDetails) {
            if (loadsToMerge.contains(loadDetail)) {
              loadDetail.setUpdateDeltaStartTimestamp(timestamp);
              loadDetail.setUpdateDeltaEndTimestamp(timestamp);
              if (loadDetail.getLoadName().equalsIgnoreCase("0")) {
                loadDetail
                    .setUpdateStatusFileName(CarbonUpdateUtil.getUpdateStatusFileName(timestamp));
              }
              // Update segement file name to status file
              int segmentFileIndex = segmentFilesToBeUpdated
                  .indexOf(Segment.toSegment(loadDetail.getLoadName(), null));
              if (segmentFileIndex > -1) {
                loadDetail.setSegmentFile(
                    segmentFilesToBeUpdated.get(segmentFileIndex).getSegmentFileName());
              }
            }
          }

          segmentUpdateStatusManager.writeLoadDetailsIntoFile(
              Arrays.asList(updateLists), timestamp);
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()), loadDetails);
          status = true;
        } else {
          LOGGER.error("Not able to acquire the lock.");
          status = false;
        }
      } catch (IOException e) {
        LOGGER.error("Error while updating metadata. The metadata file path is " +
            CarbonTablePath.getMetadataPath(identifier.getTablePath()));
        status = false;

      } finally {
        if (updateLockStatus) {
          if (updateLock.unlock()) {
            LOGGER.info("Unlock the segment update lock successfully.");
          } else {
            LOGGER.error("Not able to unlock the segment update lock.");
          }
        }
        if (tableLockStatus) {
          if (statusLock.unlock()) {
            LOGGER.info("Unlock the table status lock successfully.");
          } else {
            LOGGER.error("Not able to unlock the table status lock.");
          }
        }
      }
    }
    return status;
  }

  /**
   * method to update table status in case of IUD Update Delta Compaction.
   * @param loadsToMerge
   * @param metaDataFilepath
   * @param mergedLoadNumber
   * @param carbonLoadModel
   * @param compactionType
   * @return
   */
  public static boolean updateLoadMetadataWithMergeStatus(List<LoadMetadataDetails> loadsToMerge,
      String metaDataFilepath, String mergedLoadNumber, CarbonLoadModel carbonLoadModel,
      CompactionType compactionType, String segmentFile) throws IOException {
    boolean tableStatusUpdationStatus = false;
    AbsoluteTableIdentifier identifier =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();

    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for the table " + carbonLoadModel.getDatabaseName() + "."
            + carbonLoadModel.getTableName() + " for table status updation ");

        String statusFilePath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());

        LoadMetadataDetails[] loadDetails = SegmentStatusManager.readLoadMetadata(metaDataFilepath);

        long modificationOrDeletionTimeStamp = CarbonUpdateUtil.readCurrentTime();
        for (LoadMetadataDetails loadDetail : loadDetails) {
          // check if this segment is merged.
          if (loadsToMerge.contains(loadDetail)) {
            // if the compacted load is deleted after the start of the compaction process,
            // then need to discard the compaction process and treat it as failed compaction.
            if (loadDetail.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE) {
              LOGGER.error("Compaction is aborted as the segment " + loadDetail.getLoadName()
                  + " is deleted after the compaction is started.");
              return false;
            }
            loadDetail.setSegmentStatus(SegmentStatus.COMPACTED);
            loadDetail.setModificationOrdeletionTimesStamp(modificationOrDeletionTimeStamp);
            loadDetail.setMergedLoadName(mergedLoadNumber);
          }
        }

        // create entry for merged one.
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS);
        long loadEnddate = CarbonUpdateUtil.readCurrentTime();
        loadMetadataDetails.setLoadEndTime(loadEnddate);
        CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
        loadMetadataDetails.setLoadName(mergedLoadNumber);
        loadMetadataDetails.setSegmentFile(segmentFile);
        CarbonLoaderUtil
            .addDataIndexSizeIntoMetaEntry(loadMetadataDetails, mergedLoadNumber, carbonTable);
        loadMetadataDetails.setLoadStartTime(carbonLoadModel.getFactTimeStamp());
        // if this is a major compaction then set the segment as major compaction.
        if (CompactionType.MAJOR == compactionType) {
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
          tableStatusUpdationStatus = false;
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
   * Get the load number from load name.
   * @param loadName
   * @return
   */
  public static String getLoadNumberFromLoadName(String loadName) {
    return loadName.substring(
        loadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER) + CarbonCommonConstants.LOAD_FOLDER
            .length(), loadName.length());
  }

  /**
   * To identify which all segments can be merged.
   *
   * @param carbonLoadModel
   * @param compactionSize
   * @return
   */
  public static List<LoadMetadataDetails> identifySegmentsToBeMerged(
          CarbonLoadModel carbonLoadModel, long compactionSize,
          List<LoadMetadataDetails> segments, CompactionType compactionType,
          List<String> customSegmentIds) throws IOException, MalformedCarbonCommandException {
    String tablePath = carbonLoadModel.getTablePath();
    Map<String, String> tableLevelProperties = carbonLoadModel.getCarbonDataLoadSchema()
            .getCarbonTable().getTableInfo().getFactTable().getTableProperties();
    List<LoadMetadataDetails> sortedSegments = new ArrayList<LoadMetadataDetails>(segments);

    sortSegments(sortedSegments);

    if (CompactionType.CUSTOM == compactionType) {
      return identitySegmentsToBeMergedBasedOnSpecifiedSegments(sortedSegments,
              new HashSet<>(customSegmentIds));
    }

    // Check for segments which are qualified for IUD compaction.
    if (CompactionType.IUD_UPDDEL_DELTA == compactionType) {

      return identifySegmentsToBeMergedBasedOnIUD(sortedSegments, carbonLoadModel);
    }

    // check preserve property and preserve the configured number of latest loads.

    List<LoadMetadataDetails> listOfSegmentsAfterPreserve =
            checkPreserveSegmentsPropertyReturnRemaining(sortedSegments, tableLevelProperties);

    // filter the segments if the compaction based on days is configured.

    List<LoadMetadataDetails> listOfSegmentsLoadedInSameDateInterval =
            identifySegmentsToBeMergedBasedOnLoadedDate(listOfSegmentsAfterPreserve,
                    tableLevelProperties);
    List<LoadMetadataDetails> listOfSegmentsToBeMerged;
    // identify the segments to merge based on the Size of the segments across partition.
    if (CompactionType.MAJOR == compactionType) {

      listOfSegmentsToBeMerged = identifySegmentsToBeMergedBasedOnSize(compactionSize,
              listOfSegmentsLoadedInSameDateInterval, carbonLoadModel, tablePath);
    } else {

      listOfSegmentsToBeMerged =
              identifySegmentsToBeMergedBasedOnSegCount(listOfSegmentsLoadedInSameDateInterval,
                      tableLevelProperties);
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
      @Override
      public int compare(LoadMetadataDetails seg1, LoadMetadataDetails seg2) {
        double seg1Id = Double.parseDouble(seg1.getLoadName());
        double seg2Id = Double.parseDouble(seg2.getLoadName());
        return Double.compare(seg1Id, seg2Id);
      }
    });
  }

  /**
   * This method will return the list of loads which are specified by user in SQL.
   *
   * @param listOfSegments
   * @param segmentIds
   * @return
   */
  private static List<LoadMetadataDetails> identitySegmentsToBeMergedBasedOnSpecifiedSegments(
          List<LoadMetadataDetails> listOfSegments,
          Set<String> segmentIds) throws MalformedCarbonCommandException {
    Map<String, LoadMetadataDetails> specifiedSegments =
            new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails detail : listOfSegments) {
      if (segmentIds.contains(detail.getLoadName())) {
        specifiedSegments.put(detail.getLoadName(), detail);
      }
    }
    // all requested segments should exist and be valid
    for (String segmentId : segmentIds) {
      if (!specifiedSegments.containsKey(segmentId) ||
              !isSegmentValid(specifiedSegments.get(segmentId))) {
        String errMsg = String.format("Segment %s does not exist or is not valid", segmentId);
        LOGGER.error(errMsg);
        throw new MalformedCarbonCommandException(errMsg);
      }
    }
    return new ArrayList<>(specifiedSegments.values());
  }

  /**
   * This method will return the list of loads which are loaded at the same interval.
   * This property is configurable.
   *
   * @param listOfSegmentsBelowThresholdSize
   * @param tblProps
   * @return
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnLoadedDate(
      List<LoadMetadataDetails> listOfSegmentsBelowThresholdSize, Map<String, String> tblProps) {

    List<LoadMetadataDetails> loadsOfSameDate =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    long numberOfDaysAllowedToMerge = 0;
    try {
      // overwrite system level option by table level option if exists
      numberOfDaysAllowedToMerge = Long.parseLong(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
                      CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT));
      if (tblProps.containsKey(CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS)) {
        numberOfDaysAllowedToMerge = Long.parseLong(
                tblProps.get(CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS));
      }

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
        // compaction should skip streaming segments
        if (segment.getSegmentStatus() == SegmentStatus.STREAMING ||
            segment.getSegmentStatus() == SegmentStatus.STREAMING_FINISH) {
          continue;
        }

        if (first) {
          segDate1 = initializeFirstSegment(loadsOfSameDate, segment, sdf);
          first = false;
          continue;
        }
        long segmentDate = segment.getLoadStartTime();
        Date segDate2 = null;
        try {
          segDate2 = sdf.parse(sdf.format(segmentDate));
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
      for (LoadMetadataDetails segment : listOfSegmentsBelowThresholdSize) {
        // compaction should skip streaming segments
        if (segment.getSegmentStatus() == SegmentStatus.STREAMING ||
            segment.getSegmentStatus() == SegmentStatus.STREAMING_FINISH) {
          continue;
        }
        loadsOfSameDate.add(segment);
      }
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
    long baselineLoadStartTime = segment.getLoadStartTime();
    Date segDate1 = null;
    try {
      segDate1 = sdf.parse(sdf.format(baselineLoadStartTime));
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
    if (segDate1 == null || segDate2 == null) {
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
   * @param compactionSize compaction size in MB format
   * @param listOfSegmentsAfterPreserve  the segments list after
   *        preserving the configured number of latest loads
   * @param carbonLoadModel carbon load model
   * @param tablePath the store location of the segment
   * @return the list of segments that need to be merged
   *         based on the Size in case of Major compaction
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnSize(
      long compactionSize, List<LoadMetadataDetails> listOfSegmentsAfterPreserve,
      CarbonLoadModel carbonLoadModel, String tablePath) throws IOException {

    List<LoadMetadataDetails> segmentsToBeMerged =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();

    // total length
    long totalLength = 0;

    // check size of each segment , sum it up across partitions
    for (LoadMetadataDetails segment : listOfSegmentsAfterPreserve) {
      // compaction should skip streaming segments
      if (segment.getSegmentStatus() == SegmentStatus.STREAMING ||
          segment.getSegmentStatus() == SegmentStatus.STREAMING_FINISH) {
        continue;
      }

      String segId = segment.getLoadName();
      // variable to store one  segment size across partition.
      long sizeOfOneSegmentAcrossPartition;
      if (segment.getSegmentFile() != null) {
        sizeOfOneSegmentAcrossPartition = CarbonUtil.getSizeOfSegment(
            carbonTable.getTablePath(), new Segment(segId, segment.getSegmentFile()));
      } else {
        sizeOfOneSegmentAcrossPartition = getSizeOfSegment(carbonTable.getTablePath(), segId);
      }

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
   * @param tablePath the store path of the segment
   * @param segId segment id
   * @return the data size of the segment
   */
  private static long getSizeOfSegment(String tablePath, String segId) {
    String loadPath = CarbonTablePath.getSegmentPath(tablePath, segId);
    CarbonFile segmentFolder =
        FileFactory.getCarbonFile(loadPath, FileFactory.getFileType(loadPath));
    return getSizeOfFactFileInLoad(segmentFolder);
  }

  /**
   * Identify the segments to be merged based on the segment count
   *
   * @param listOfSegmentsAfterPreserve the list of segments after
   *        preserve and before filtering by minor compaction level
   * @param tblProps
   * @return the list of segments to be merged after filtering by minor compaction level
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnSegCount(
          List<LoadMetadataDetails> listOfSegmentsAfterPreserve, Map<String, String> tblProps) {

    List<LoadMetadataDetails> mergedSegments =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<LoadMetadataDetails> unMergedSegments =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    int[] noOfSegmentLevelsCount = CarbonProperties.getInstance()
            .getCompactionSegmentLevelCount();
    // overwrite system level option by table level option if exists
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD)) {
      noOfSegmentLevelsCount = CarbonProperties.getInstance()
              .getIntArray(tblProps.get(CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD));
      if (0 == noOfSegmentLevelsCount.length) {
        noOfSegmentLevelsCount = CarbonProperties.getInstance().getCompactionSegmentLevelCount();
      }
    }

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
      // compaction should skip streaming segments
      if (segment.getSegmentStatus() == SegmentStatus.STREAMING ||
          segment.getSegmentStatus() == SegmentStatus.STREAMING_FINISH) {
        continue;
      }
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
   * @param segName segment name
   * @return the status whether the segment has been Merged
   */
  private static boolean isMergedSegment(String segName) {
    if (segName.contains(".")) {
      return true;
    }
    return false;
  }

  /**
   * checks number of loads to be preserved and returns remaining valid segments
   *
   * @param segments
   * @param tblProps
   * @return
   */
  private static List<LoadMetadataDetails> checkPreserveSegmentsPropertyReturnRemaining(
      List<LoadMetadataDetails> segments, Map<String, String> tblProps) {
    // check whether the preserving of the segments from merging is enabled or not.
    // get the number of loads to be preserved. default value is system level option
    // overwrite system level option by table level option if exists
    int numberOfSegmentsToBePreserved = CarbonProperties.getInstance()
            .getNumberOfSegmentsToBePreserved();
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS)) {
      numberOfSegmentsToBePreserved = Integer.parseInt(
              tblProps.get(CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS));
    }

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
      if (isSegmentValid(segment)) {
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
   * @param carbonLoadModel
   * @return
   */
  public static long getCompactionSize(CompactionType compactionType,
                                       CarbonLoadModel carbonLoadModel) {
    long compactionSize = 0;
    switch (compactionType) {
      case MAJOR:
        // default value is system level option
        compactionSize = CarbonProperties.getInstance().getMajorCompactionSize();
        // if table level option is identified, use it to overwrite system level option
        Map<String, String> tblProps = carbonLoadModel.getCarbonDataLoadSchema()
                .getCarbonTable().getTableInfo().getFactTable().getTableProperties();
        if (tblProps.containsKey(CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE)) {
          compactionSize = Long.parseLong(
                  tblProps.get(CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE));
        }
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
  public static List<Segment> getValidSegments(List<LoadMetadataDetails> loadMetadataDetails) {
    List<Segment> segments = new ArrayList<>();
    for (LoadMetadataDetails segment : loadMetadataDetails) {
      //check if this load is an already merged load.
      if (null != segment.getMergedLoadName()) {

        segments.add(Segment.toSegment(segment.getMergedLoadName(), null));
      } else {
        segments.add(Segment.toSegment(segment.getLoadName(), null));
      }
    }
    return segments;
  }

  /**
   * This method returns the valid segments attached to the table Identifier.
   *
   * @param absoluteTableIdentifier
   * @return
   */
  public static List<Segment> getValidSegmentList(AbsoluteTableIdentifier absoluteTableIdentifier)
          throws IOException {

    SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegments = null;
    try {
      validAndInvalidSegments =
              new SegmentStatusManager(absoluteTableIdentifier).getValidAndInvalidSegments();
    } catch (IOException e) {
      LOGGER.error("Error while getting valid segment list for a table identifier");
      throw new IOException();
    }
    return validAndInvalidSegments.getValidSegments();
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

  /**
   * method to identify the segments qualified for merging in case of IUD Compaction.
   *
   * @param segments
   * @param carbonLoadModel
   * @return
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnIUD(
      List<LoadMetadataDetails> segments, CarbonLoadModel carbonLoadModel) {

    List<LoadMetadataDetails> validSegments = new ArrayList<>(segments.size());

    AbsoluteTableIdentifier absoluteTableIdentifier =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();

    int numberUpdateDeltaFilesThreshold =
        CarbonProperties.getInstance().getNoUpdateDeltaFilesThresholdForIUDCompaction();
    for (LoadMetadataDetails seg : segments) {
      if ((isSegmentValid(seg)) && checkUpdateDeltaFilesInSeg(
          new Segment(seg.getLoadName(), seg.getSegmentFile()),
          absoluteTableIdentifier, carbonLoadModel.getSegmentUpdateStatusManager(),
          numberUpdateDeltaFilesThreshold)) {
        validSegments.add(seg);
      }
    }
    return validSegments;
  }

  private static boolean isSegmentValid(LoadMetadataDetails seg) {
    return seg.getSegmentStatus() == SegmentStatus.SUCCESS ||
        seg.getSegmentStatus() == SegmentStatus.LOAD_PARTIAL_SUCCESS ||
        seg.getSegmentStatus() == SegmentStatus.MARKED_FOR_UPDATE;
  }

  /**
   * method gets the segments list which get qualified for IUD compaction.
   * @param segments
   * @param absoluteTableIdentifier
   * @param compactionTypeIUD
   * @return
   */
  public static List<String> getSegListIUDCompactionQualified(List<Segment> segments,
      AbsoluteTableIdentifier absoluteTableIdentifier,
      SegmentUpdateStatusManager segmentUpdateStatusManager, CompactionType compactionTypeIUD) {

    List<String> validSegments = new ArrayList<>();

    if (CompactionType.IUD_DELETE_DELTA == compactionTypeIUD) {
      int numberDeleteDeltaFilesThreshold =
          CarbonProperties.getInstance().getNoDeleteDeltaFilesThresholdForIUDCompaction();
      List<Segment> deleteSegments = new ArrayList<>();
      for (Segment seg : segments) {
        if (checkDeleteDeltaFilesInSeg(seg, segmentUpdateStatusManager,
            numberDeleteDeltaFilesThreshold)) {
          deleteSegments.add(seg);
        }
      }
      if (deleteSegments.size() > 0) {
        // This Code Block Append the Segname along with the Blocks selected for Merge instead of
        // only taking the segment name. This will help to parallelize better for each block
        // in case of Delete Horizontal Compaction.
        for (Segment segName : deleteSegments) {
          List<String> tempSegments = getDeleteDeltaFilesInSeg(segName, segmentUpdateStatusManager,
              numberDeleteDeltaFilesThreshold);
          validSegments.addAll(tempSegments);
        }
      }
    } else if (CompactionType.IUD_UPDDEL_DELTA == compactionTypeIUD) {
      int numberUpdateDeltaFilesThreshold =
          CarbonProperties.getInstance().getNoUpdateDeltaFilesThresholdForIUDCompaction();
      for (Segment seg : segments) {
        if (checkUpdateDeltaFilesInSeg(seg, absoluteTableIdentifier, segmentUpdateStatusManager,
            numberUpdateDeltaFilesThreshold)) {
          validSegments.add(seg.getSegmentNo());
        }
      }
    }
    return validSegments;
  }

  /**
   * Check if the blockname of the segment belongs to the Valid Update Delta List or not.
   * @param seg
   * @param blkName
   * @param segmentUpdateStatusManager
   * @return
   */
  public static Boolean checkUpdateDeltaMatchBlock(final String seg, final String blkName,
      SegmentUpdateStatusManager segmentUpdateStatusManager) {

    List<String> list = segmentUpdateStatusManager.getUpdateDeltaFiles(seg);

    String[] FileParts = blkName.split(CarbonCommonConstants.FILE_SEPARATOR);
    String blockName = FileParts[FileParts.length - 1];

    for (String str : list) {
      if (str.contains(blockName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method traverses Update Delta Files inside the seg and return true
   * if UpdateDelta Files are more than IUD Compaction threshold.
   */
  private static Boolean checkUpdateDeltaFilesInSeg(Segment seg,
      AbsoluteTableIdentifier identifier, SegmentUpdateStatusManager segmentUpdateStatusManager,
      int numberDeltaFilesThreshold) {

    CarbonFile[] updateDeltaFiles = null;
    Set<String> uniqueBlocks = new HashSet<String>();

    String segmentPath = CarbonTablePath.getSegmentPath(
        identifier.getTablePath(), seg.getSegmentNo());
    CarbonFile segDir =
        FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));
    CarbonFile[] allSegmentFiles = segDir.listFiles();

    updateDeltaFiles = segmentUpdateStatusManager
        .getUpdateDeltaFilesForSegment(seg.getSegmentNo(), true,
            CarbonCommonConstants.UPDATE_DELTA_FILE_EXT, false, allSegmentFiles);

    if (updateDeltaFiles == null) {
      return false;
    }

    // The update Delta files may have Spill over blocks. Will consider multiple spill over
    // blocks as one. Currently updateDeltaFiles array contains Update Delta Block name which
    // lies within UpdateDelta Start TimeStamp and End TimeStamp. In order to eliminate
    // Spill Over Blocks will choose files with unique taskID.
    for (CarbonFile blocks : updateDeltaFiles) {
      // Get Task ID and the Timestamp from the Block name for e.g.
      // part-0-3-1481084721319.carbondata => "3-1481084721319"
      String task = CarbonTablePath.DataFileUtil.getTaskNo(blocks.getName());
      String timestamp =
          CarbonTablePath.DataFileUtil.getTimeStampFromDeleteDeltaFile(blocks.getName());
      String taskAndTimeStamp = task + "-" + timestamp;
      uniqueBlocks.add(taskAndTimeStamp);
    }
    if (uniqueBlocks.size() > numberDeltaFilesThreshold) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check is the segment passed qualifies for IUD delete delta compaction or not i.e.
   * if the number of delete delta files present in the segment is more than
   * numberDeltaFilesThreshold.
   *
   * @param seg
   * @param segmentUpdateStatusManager
   * @param numberDeltaFilesThreshold
   * @return
   */
  private static boolean checkDeleteDeltaFilesInSeg(Segment seg,
      SegmentUpdateStatusManager segmentUpdateStatusManager, int numberDeltaFilesThreshold) {

    Set<String> uniqueBlocks = new HashSet<String>();
    List<String> blockNameList =
        segmentUpdateStatusManager.getBlockNameFromSegment(seg.getSegmentNo());

    for (final String blockName : blockNameList) {

      CarbonFile[] deleteDeltaFiles =
          segmentUpdateStatusManager.getDeleteDeltaFilesList(seg, blockName);
      if (null != deleteDeltaFiles) {
        // The Delete Delta files may have Spill over blocks. Will consider multiple spill over
        // blocks as one. Currently DeleteDeltaFiles array contains Delete Delta Block name which
        // lies within Delete Delta Start TimeStamp and End TimeStamp. In order to eliminate
        // Spill Over Blocks will choose files with unique taskID.
        for (CarbonFile blocks : deleteDeltaFiles) {
          // Get Task ID and the Timestamp from the Block name for e.g.
          // part-0-3-1481084721319.carbondata => "3-1481084721319"
          String task = CarbonTablePath.DataFileUtil.getTaskNo(blocks.getName());
          String timestamp =
              CarbonTablePath.DataFileUtil.getTimeStampFromDeleteDeltaFile(blocks.getName());
          String taskAndTimeStamp = task + "-" + timestamp;
          uniqueBlocks.add(taskAndTimeStamp);
        }

        if (uniqueBlocks.size() > numberDeltaFilesThreshold) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check is the segment passed qualifies for IUD delete delta compaction or not i.e.
   * if the number of delete delta files present in the segment is more than
   * numberDeltaFilesThreshold.
   * @param seg
   * @param segmentUpdateStatusManager
   * @param numberDeltaFilesThreshold
   * @return
   */

  private static List<String> getDeleteDeltaFilesInSeg(Segment seg,
      SegmentUpdateStatusManager segmentUpdateStatusManager, int numberDeltaFilesThreshold) {

    List<String> blockLists = new ArrayList<>();
    List<String> blockNameList =
        segmentUpdateStatusManager.getBlockNameFromSegment(seg.getSegmentNo());

    for (final String blockName : blockNameList) {

      CarbonFile[] deleteDeltaFiles =
          segmentUpdateStatusManager.getDeleteDeltaFilesList(seg, blockName);

      if (null != deleteDeltaFiles && (deleteDeltaFiles.length > numberDeltaFilesThreshold)) {
        blockLists.add(seg.getSegmentNo() + "/" + blockName);
      }
    }
    return blockLists;
  }

  /**
   * Returns true is horizontal compaction is enabled.
   * @return
   */
  public static boolean isHorizontalCompactionEnabled() {
    if ((CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.isHorizontalCompactionEnabled,
            CarbonCommonConstants.defaultIsHorizontalCompactionEnabled)).equalsIgnoreCase("true")) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * method to compact Delete Delta files in case of IUD Compaction.
   *
   * @param seg
   * @param blockName
   * @param segmentUpdateDetails
   * @param timestamp
   * @return
   * @throws IOException
   */
  public static List<CarbonDataMergerUtilResult> compactBlockDeleteDeltaFiles(String seg,
      String blockName, CarbonTable table, SegmentUpdateDetails[] segmentUpdateDetails,
      Long timestamp) throws IOException {

    SegmentUpdateStatusManager segmentUpdateStatusManager = new SegmentUpdateStatusManager(table);

    List<CarbonDataMergerUtilResult> resultList = new ArrayList<CarbonDataMergerUtilResult>(1);

    // set the update status.
    segmentUpdateStatusManager.setUpdateStatusDetails(segmentUpdateDetails);

    CarbonFile[] deleteDeltaFiles =
        segmentUpdateStatusManager.getDeleteDeltaFilesList(new Segment(seg, null), blockName);

    String destFileName =
        blockName + "-" + timestamp.toString() + CarbonCommonConstants.DELETE_DELTA_FILE_EXT;
    List<String> deleteFilePathList = new ArrayList<>();
    if (null != deleteDeltaFiles && deleteDeltaFiles.length > 0 && null != deleteDeltaFiles[0]
        .getParentFile()) {
      String fullBlockFilePath = deleteDeltaFiles[0].getParentFile().getCanonicalPath()
          + CarbonCommonConstants.FILE_SEPARATOR + destFileName;

      for (CarbonFile cFile : deleteDeltaFiles) {
        deleteFilePathList.add(cFile.getCanonicalPath());
      }

      CarbonDataMergerUtilResult blockDetails = new CarbonDataMergerUtilResult();
      blockDetails.setBlockName(blockName);
      blockDetails.setSegmentName(seg);
      blockDetails.setDeleteDeltaStartTimestamp(timestamp.toString());
      blockDetails.setDeleteDeltaEndTimestamp(timestamp.toString());

      try {
        startCompactionDeleteDeltaFiles(deleteFilePathList, blockName, fullBlockFilePath);
        blockDetails.setCompactionStatus(true);
        resultList.add(blockDetails);
      } catch (IOException e) {
        LOGGER.error("Compaction of Delete Delta Files failed. The complete file path is "
            + fullBlockFilePath);
        throw new IOException();
      }
    }
    return resultList;
  }

  /**
   * this method compact the delete delta files.
   * @param deleteDeltaFiles
   * @param blockName
   * @param fullBlockFilePath
   * @return
   */
  public static void startCompactionDeleteDeltaFiles(List<String> deleteDeltaFiles,
      String blockName, String fullBlockFilePath) throws IOException {

    DeleteDeltaBlockDetails deleteDeltaBlockDetails = null;
    CarbonDeleteFilesDataReader dataReader = new CarbonDeleteFilesDataReader();
    try {
      deleteDeltaBlockDetails =
              dataReader.getCompactedDeleteDeltaFileFromBlock(deleteDeltaFiles, blockName);
    } catch (Exception e) {
      String blockFilePath = fullBlockFilePath
              .substring(0, fullBlockFilePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR));
      LOGGER.error("Error while getting the delete delta blocks in path " + blockFilePath);
      throw new IOException();
    }
    CarbonDeleteDeltaWriterImpl carbonDeleteWriter =
            new CarbonDeleteDeltaWriterImpl(fullBlockFilePath,
                    FileFactory.getFileType(fullBlockFilePath));
    try {
      carbonDeleteWriter.write(deleteDeltaBlockDetails);
    } catch (IOException e) {
      LOGGER.error("Error while writing compacted delete delta file " + fullBlockFilePath);
      throw new IOException();
    }
  }

  public static Boolean updateStatusFile(
          List<CarbonDataMergerUtilResult> updateDataMergerDetailsList, CarbonTable table,
          String timestamp, SegmentUpdateStatusManager segmentUpdateStatusManager) {

    List<SegmentUpdateDetails> segmentUpdateDetails =
            new ArrayList<SegmentUpdateDetails>(updateDataMergerDetailsList.size());


    // Check the list output.
    for (CarbonDataMergerUtilResult carbonDataMergerUtilResult : updateDataMergerDetailsList) {
      if (carbonDataMergerUtilResult.getCompactionStatus()) {
        SegmentUpdateDetails tempSegmentUpdateDetails = new SegmentUpdateDetails();
        tempSegmentUpdateDetails.setSegmentName(carbonDataMergerUtilResult.getSegmentName());
        tempSegmentUpdateDetails.setBlockName(carbonDataMergerUtilResult.getBlockName());

        for (SegmentUpdateDetails origDetails : segmentUpdateStatusManager
                .getUpdateStatusDetails()) {
          if (origDetails.getBlockName().equalsIgnoreCase(carbonDataMergerUtilResult.getBlockName())
                  && origDetails.getSegmentName()
                  .equalsIgnoreCase(carbonDataMergerUtilResult.getSegmentName())) {

            tempSegmentUpdateDetails.setDeletedRowsInBlock(origDetails.getDeletedRowsInBlock());
            tempSegmentUpdateDetails.setSegmentStatus(origDetails.getSegmentStatus());
            break;
          }
        }

        tempSegmentUpdateDetails.setDeleteDeltaStartTimestamp(
                carbonDataMergerUtilResult.getDeleteDeltaStartTimestamp());
        tempSegmentUpdateDetails
              .setDeleteDeltaEndTimestamp(carbonDataMergerUtilResult.getDeleteDeltaEndTimestamp());

        segmentUpdateDetails.add(tempSegmentUpdateDetails);
      } else return false;
    }

    CarbonUpdateUtil.updateSegmentStatus(segmentUpdateDetails, table, timestamp, true);

    // Update the Table Status.
    String metaDataFilepath = table.getMetadataPath();
    AbsoluteTableIdentifier identifier = table.getAbsoluteTableIdentifier();

    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();

    boolean lockStatus = false;

    try {
      lockStatus = carbonLock.lockWithRetries();
      if (lockStatus) {
        LOGGER.info(
                "Acquired lock for table" + table.getDatabaseName() + "." + table.getTableName()
                        + " for table status updation");

        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
                SegmentStatusManager.readLoadMetadata(metaDataFilepath);

        for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {
          if (loadMetadata.getLoadName().equalsIgnoreCase("0")) {
            loadMetadata.setUpdateStatusFileName(
                    CarbonUpdateUtil.getUpdateStatusFileName(timestamp));
          }
        }
        try {
          SegmentStatusManager
                  .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
        } catch (IOException e) {
          return false;
        }
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + table
                .getDatabaseName() + "." + table.getTableName());
      }
    } finally {
      if (lockStatus) {
        if (carbonLock.unlock()) {
          LOGGER.info(
                 "Table unlocked successfully after table status updation" + table.getDatabaseName()
                          + "." + table.getTableName());
        } else {
          LOGGER.error(
                  "Unable to unlock Table lock for table" + table.getDatabaseName() + "." + table
                          .getTableName() + " during table status updation");
        }
      }
    }
    return true;
  }

}
