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
import java.util.List;

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

  /**
   * Delete the invalid data physically from table.
   * @param carbonTable table
   * @param loadDetails Load details which need clean up
   * @param specs Partition specs
   */
  public static void physicalFactAndMeasureMetadataDeletion(CarbonTable carbonTable,
      LoadMetadataDetails[] loadDetails, List<PartitionSpec> specs) {
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
    LoadMetadataDetails[] currLoadDetails =
        SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(carbonTable, currLoadDetails);
    for (final LoadMetadataDetails oneLoad : loadDetails) {
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

  private static boolean checkIfLoadCanBeDeleted(LoadMetadataDetails oneLoad,
      boolean isForceDelete, boolean cleanStaleInProgress, AbsoluteTableIdentifier
      absoluteTableIdentifier) {
    if (oneLoad.getVisibility().equalsIgnoreCase("true")) {
      return canDeleteThisLoad(oneLoad, isForceDelete, cleanStaleInProgress,
          absoluteTableIdentifier);
    }
    return false;
  }

  /**
   * Used for clean files with specific segment ids, when segment ids are specified,
   * only all specified segments can be deleted, then we continue the clean files operation,
   * otherwise, throw exception and show the segment which cannot be deleted.
   * @return segment id list which contains all the segments which cannot be deleted
   */
  public static List<String> loadsCannotBeDeleted(
      AbsoluteTableIdentifier absoluteTableIdentifier, List<LoadMetadataDetails> details) {
    List<String> loadsCannotBeDeleted = new ArrayList<>();
    if (details != null && !details.isEmpty()) {
      for (LoadMetadataDetails oneLoad : details) {
        if (checkIfLoadCanBeDeleted(oneLoad, true, true, absoluteTableIdentifier)) {
          oneLoad.setVisibility("false");
          LOGGER.info("Deleted the load " + oneLoad.getLoadName());
        } else {
          loadsCannotBeDeleted.add(oneLoad.getLoadName());
          LOGGER.info("Segment " + oneLoad.getLoadName() + " cannot be deleted at this moment, its"
              + " status is " + oneLoad.getSegmentStatus());
        }
      }
    }
    return loadsCannotBeDeleted;
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

  public static boolean deleteLoadFoldersFromFileSystem(
      AbsoluteTableIdentifier absoluteTableIdentifier, boolean isForceDelete,
      LoadMetadataDetails[] details, boolean cleanStaleInProgress) {
    boolean isDeleted = false;
    if (details != null && details.length != 0) {
      for (LoadMetadataDetails oneLoad : details) {
        if (checkIfLoadCanBeDeleted(oneLoad, isForceDelete, cleanStaleInProgress,
            absoluteTableIdentifier)) {
          oneLoad.setVisibility("false");
          isDeleted = true;
          LOGGER.info("Deleted the load " + oneLoad.getLoadName());
        }
      }
    }
    return isDeleted;
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
