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
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
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
      List<PartitionSpec> specs) {
    LoadMetadataDetails[] currentDetails =
        SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
    physicalFactAndMeasureMetadataDeletion(carbonTable,
        currentDetails,
        isForceDelete,
        specs,
        currentDetails);
    if (newAddedLoadHistoryList != null && newAddedLoadHistoryList.length > 0) {
      physicalFactAndMeasureMetadataDeletion(carbonTable,
          newAddedLoadHistoryList,
          isForceDelete,
          specs,
          currentDetails);
    }
  }

  /**
   * Delete the invalid data physically from table.
   * @param carbonTable table
   * @param loadDetails Load details which need clean up
   * @param isForceDelete is Force delete requested by user
   * @param specs Partition specs
   * @param currLoadDetails Current table status load details which are required for update manager.
   */
  private static void physicalFactAndMeasureMetadataDeletion(CarbonTable carbonTable,
      LoadMetadataDetails[] loadDetails, boolean isForceDelete, List<PartitionSpec> specs,
      LoadMetadataDetails[] currLoadDetails) {
    List<TableIndex> indexes = new ArrayList<>();
    try {
      for (TableIndex index : IndexStoreManager.getInstance().getAllIndexes(carbonTable)) {
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
      if (checkIfLoadCanBeDeletedPhysically(oneLoad, isForceDelete)) {
        try {
          if (oneLoad.getSegmentFile() != null) {
            SegmentFileStore.deleteSegment(carbonTable.getAbsoluteTableIdentifier().getTablePath(),
                new Segment(oneLoad.getLoadName(), oneLoad.getSegmentFile()),
                specs, updateStatusManager);
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
      boolean isForceDelete) {
    if ((SegmentStatus.MARKED_FOR_DELETE == oneLoad.getSegmentStatus() ||
        SegmentStatus.COMPACTED == oneLoad.getSegmentStatus() ||
        SegmentStatus.INSERT_IN_PROGRESS == oneLoad.getSegmentStatus() ||
        SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == oneLoad.getSegmentStatus())
        && oneLoad.getVisibility().equalsIgnoreCase("true")) {
      if (isForceDelete) {
        return true;
      }
      long deletionTime = oneLoad.getModificationOrdeletionTimesStamp();

      return CarbonUpdateUtil.isMaxQueryTimeoutExceeded(deletionTime);

    }

    return false;
  }

  private static boolean checkIfLoadCanBeDeletedPhysically(LoadMetadataDetails oneLoad,
      boolean isForceDelete) {
    // Check if the segment is added externally and path is set then do not delete it
    if ((SegmentStatus.MARKED_FOR_DELETE == oneLoad.getSegmentStatus()
        || SegmentStatus.COMPACTED == oneLoad.getSegmentStatus()) && (oneLoad.getPath() == null
        || oneLoad.getPath().equalsIgnoreCase("NA"))) {
      if (isForceDelete) {
        return true;
      }
      long deletionTime = oneLoad.getModificationOrdeletionTimesStamp();

      return CarbonUpdateUtil.isMaxQueryTimeoutExceeded(deletionTime);

    }

    return false;
  }

  private static LoadMetadataDetails getCurrentLoadStatusOfSegment(String segmentId,
      String metadataPath) {
    LoadMetadataDetails[] currentDetails = SegmentStatusManager.readLoadMetadata(metadataPath);
    for (LoadMetadataDetails oneLoad : currentDetails) {
      if (oneLoad.getLoadName().equalsIgnoreCase(segmentId)) {
        return oneLoad;
      }
    }
    return null;
  }

  public static boolean deleteLoadFoldersFromFileSystem(
      AbsoluteTableIdentifier absoluteTableIdentifier, boolean isForceDelete,
      LoadMetadataDetails[] details, String metadataPath) {
    boolean isDeleted = false;
    if (details != null && details.length != 0) {
      for (LoadMetadataDetails oneLoad : details) {
        if (checkIfLoadCanBeDeleted(oneLoad, isForceDelete)) {
          ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
              CarbonTablePath.addSegmentPrefix(oneLoad.getLoadName()) + LockUsage.LOCK);
          try {
            if (oneLoad.getSegmentStatus() == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS
                || oneLoad.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS) {
              if (segmentLock.lockWithRetries(1, 5)) {
                LOGGER.info("Info: Acquired segment lock on segment:" + oneLoad.getLoadName());
                LoadMetadataDetails currentDetails =
                    getCurrentLoadStatusOfSegment(oneLoad.getLoadName(), metadataPath);
                if (currentDetails != null && checkIfLoadCanBeDeleted(currentDetails,
                    isForceDelete)) {
                  oneLoad.setVisibility("false");
                  isDeleted = true;
                  LOGGER.info("Info: Deleted the load " + oneLoad.getLoadName());
                }
              } else {
                LOGGER.info("Info: Load in progress for segment" + oneLoad.getLoadName());
                return isDeleted;
              }
            } else {
              oneLoad.setVisibility("false");
              isDeleted = true;
              LOGGER.info("Info: Deleted the load " + oneLoad.getLoadName());
            }
          } finally {
            segmentLock.unlock();
            LOGGER.info("Info: Segment lock on segment:" + oneLoad.getLoadName() + " is released");
          }
        }
      }
    }
    return isDeleted;
  }

}
