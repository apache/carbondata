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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.IndexStoreManager;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.TableIndex;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.log4j.Logger;

public final class DeleteLoadFolders {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DeleteLoadFolders.class.getName());

  private DeleteLoadFolders() {

  }

  /**
   * returns segment path
   *
   * @param identifier
   * @param oneLoad
   * @return
   */
  private static String getSegmentPath(AbsoluteTableIdentifier identifier,
      LoadMetadataDetails oneLoad) {
    String segmentId = oneLoad.getLoadName();
    return CarbonTablePath.getSegmentPath(identifier.getTablePath(), segmentId);
  }

  public static void physicalFactAndMeasureMetadataDeletion(CarbonTable carbonTable,
      LoadMetadataDetails[] newAddedLoadHistoryList,
      boolean isForceDelete,
      List<PartitionSpec> specs,
      boolean cleanStaleInProgress,
      Set<String> loadsToDelete,
      String tblStatusVersion) {
    LoadMetadataDetails[] currentDetails =
        SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath(),
            tblStatusVersion);
    physicalFactAndMeasureMetadataDeletion(carbonTable,
        currentDetails,
        isForceDelete,
        specs,
        currentDetails,
        cleanStaleInProgress,
        loadsToDelete);
    if (newAddedLoadHistoryList != null && newAddedLoadHistoryList.length > 0) {
      physicalFactAndMeasureMetadataDeletion(carbonTable,
          newAddedLoadHistoryList,
          isForceDelete,
          specs,
          currentDetails,
          cleanStaleInProgress,
          loadsToDelete);
    }
  }

  /**
   * Delete the invalid data physically from table.
   * @param carbonTable table
   * @param loadDetails Load details which need clean up
   * @param isForceDelete Force delete Compacted and MFD segments. it will empty the trash folder
   * @param specs Partition specs
   * @param currLoadDetails Current table status load details which are required for update manager.
   */
  private static void physicalFactAndMeasureMetadataDeletion(CarbonTable carbonTable,
      LoadMetadataDetails[] loadDetails, boolean isForceDelete, List<PartitionSpec> specs,
      LoadMetadataDetails[] currLoadDetails, boolean cleanStaleInProgress,
      Set<String> loadsToDelete) {
    List<TableIndex> indexes = new ArrayList<>();
    try {
      for (TableIndex index : IndexStoreManager.getInstance().getAllCGAndFGIndexes(carbonTable)) {
        if (index.getIndexSchema().isIndex()) {
          indexes.add(index);
        }
      }
    } catch (IOException e) {
      LOGGER.warn(String.format(
          "Failed to get indexes for %s.%s, therefore the index files could not be cleaned.",
          carbonTable.getAbsoluteTableIdentifier().getDatabaseName(),
          carbonTable.getAbsoluteTableIdentifier().getTableName()));
    }
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(carbonTable, currLoadDetails);
    for (final LoadMetadataDetails oneLoad : loadDetails) {
      if (loadsToDelete.contains(oneLoad.getLoadName())) {
        try {
          if (oneLoad.getSegmentFile() != null) {
            String tablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
            Segment segment = new Segment(oneLoad.getLoadName(), oneLoad.getSegmentFile());
            // No need to delete physical data for external segments.
            if (oneLoad.getPath() == null || oneLoad.getPath().equalsIgnoreCase("NA")) {
              SegmentFileStore.deleteSegment(tablePath, segment, specs, updateStatusManager);
            }
            // delete segment files for all segments.
            SegmentFileStore.deleteSegmentFile(tablePath, segment);
          } else {
            String path = getSegmentPath(carbonTable.getAbsoluteTableIdentifier(), oneLoad);
            boolean status = false;
            if (FileFactory.isFileExist(path)) {
              CarbonFile file = FileFactory.getCarbonFile(path);
              CarbonFile[] filesToBeDeleted = file.listFiles(new CarbonFileFilter() {

                @Override
                public boolean accept(CarbonFile file) {
                  return (CarbonTablePath.isCarbonDataFile(file.getName()) ||
                      CarbonTablePath.isCarbonIndexFile(file.getName()));
                }
              });

              //if there are no fact and msr metadata files present then no need to keep
              //entry in metadata.
              if (filesToBeDeleted.length == 0) {
                status = true;
              } else {

                for (CarbonFile eachFile : filesToBeDeleted) {
                  if (!eachFile.delete()) {
                    LOGGER.warn("Unable to delete the file as per delete command " + eachFile
                        .getAbsolutePath());
                    status = false;
                  } else {
                    status = true;
                  }
                }
              }
              // need to delete the complete folder.
              if (status) {
                if (!file.delete()) {
                  LOGGER.warn("Unable to delete the folder as per delete command " + file
                      .getAbsolutePath());
                }
              }

            }
          }
          List<Segment> segments = new ArrayList<>(1);
          for (TableIndex index : indexes) {
            segments.clear();
            segments.add(new Segment(oneLoad.getLoadName()));
            index.deleteIndexData(segments);
          }
        } catch (Exception e) {
          LOGGER.warn("Unable to delete the file as per delete command " + oneLoad.getLoadName());
        }
      }
    }
  }

  private static boolean checkIfLoadCanBeDeleted(LoadMetadataDetails oneLoad,
      boolean isForceDelete, boolean cleanStaleInProgress, AbsoluteTableIdentifier
      absoluteTableIdentifier) {
    if (oneLoad.getVisibility().equalsIgnoreCase("true")) {
      return canDeleteThisLoad(oneLoad, isForceDelete, cleanStaleInProgress,
          absoluteTableIdentifier);
    }
    return false;
  }

  public static Boolean canDeleteThisLoad(LoadMetadataDetails oneLoad, boolean
      isForceDelete, boolean cleanStaleInProgress, AbsoluteTableIdentifier
      absoluteTableIdentifier) {
    /*
     * if cleanStaleInProgress == false and  isForceDelete == false, clean MFD and Compacted
     *  segments will depend on query timeout(1 hr) and trashRetentionTimeout(7 days, default).
     *  For example:
     *  If trashRetentionTimeout is 7 days and query timeout is 1 hr--> Delete after 7 days
     *  If trashRetentionTimeout is 0 days and query timeout is 1 hr--> Delete after 1 hr
     *
     * if cleanStaleInProgress == false and  isForceDelete == true, clean MFD and Compacted
     *  segments immediately(Do not check for any timeout)
     *
     * if cleanStaleInProgress == true and  isForceDelete == false, clean Stale Inprogress, MFD and
     *  compacted segments after 7 days(taking carbon.trash.retention.time value)
     *
     * if cleanStaleInProgress == true and  isForceDelete == true, clean MFD, Compacted and
     *  stale inprogress segments immediately.(Do not check for any timeout)
     */
    boolean canDelete = isForceDelete || TrashUtil.isDataOutsideTrashIsExpired(
        oneLoad.getModificationOrDeletionTimestamp());
    switch (oneLoad.getSegmentStatus()) {
      case COMPACTED:
      case MARKED_FOR_DELETE:
        return canDelete;
      case INSERT_IN_PROGRESS:
      case INSERT_OVERWRITE_IN_PROGRESS:
        return canDelete && cleanStaleInProgress && canSegmentLockBeAcquired(oneLoad,
            absoluteTableIdentifier);
      default:
        return false;
    }
  }

  private static LoadMetadataDetails getCurrentLoadStatusOfSegment(String segmentId,
      String metadataPath, String version) {
    LoadMetadataDetails[] currentDetails =
        SegmentStatusManager.readLoadMetadata(metadataPath, version);
    for (LoadMetadataDetails oneLoad : currentDetails) {
      if (oneLoad.getLoadName().equalsIgnoreCase(segmentId)) {
        return oneLoad;
      }
    }
    return null;
  }

  public static Set<String> deleteLoadFoldersFromFileSystem(
      AbsoluteTableIdentifier absoluteTableIdentifier, boolean isForceDelete, LoadMetadataDetails[]
      details, String metadataPath, boolean cleanStaleInProgress, String version) {
    Set<String> loadsToDelete = new HashSet<>();
    if (details != null && details.length != 0) {
      for (LoadMetadataDetails oneLoad : details) {
        if (checkIfLoadCanBeDeleted(oneLoad, isForceDelete, cleanStaleInProgress,
            absoluteTableIdentifier)) {
          if (oneLoad.getSegmentStatus() == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS
              || oneLoad.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS) {
            LoadMetadataDetails currentDetails =
                getCurrentLoadStatusOfSegment(oneLoad.getLoadName(), metadataPath, version);
            if (currentDetails != null && checkIfLoadCanBeDeleted(currentDetails,
                isForceDelete, cleanStaleInProgress, absoluteTableIdentifier)) {
              oneLoad.setVisibility("false");
              loadsToDelete.add(oneLoad.getLoadName());
              LOGGER.info("Deleted the load " + oneLoad.getLoadName());
            }
          } else {
            oneLoad.setVisibility("false");
            loadsToDelete.add(oneLoad.getLoadName());
            LOGGER.info("Deleted the load " + oneLoad.getLoadName());
          }
        }
      }
    }
    return loadsToDelete;
  }

  private static boolean canSegmentLockBeAcquired(LoadMetadataDetails oneLoad,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
        CarbonTablePath.addSegmentPrefix(oneLoad.getLoadName()) + LockUsage.LOCK);
    if (segmentLock.lockWithRetries()) {
      LOGGER.info("Segment Lock on segment: " + oneLoad.getLoadName() + "can be acquired.");
      return segmentLock.unlock();
    } else {
      LOGGER.info("Segment Lock on segment: " + oneLoad.getLoadName() + "can not be" +
          " acquired. Load going on for that load");
    }
    return false;
  }
}
