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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentDetailVO;
import org.apache.carbondata.core.statusmanager.SegmentManager;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

public final class DeleteLoadFolders {

  private static final LogService LOGGER =
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

  public static void physicalFactAndMeasureMetadataDeletion(
      AbsoluteTableIdentifier absoluteTableIdentifier,
      String metadataPath,
      LoadMetadataDetails[] newAddedLoadHistoryList,
      boolean isForceDelete,
      List<PartitionSpec> specs) {
    LoadMetadataDetails[] currentDetails = SegmentStatusManager.readLoadMetadata(metadataPath);
    physicalFactAndMeasureMetadataDeletion(
        absoluteTableIdentifier,
        currentDetails,
        isForceDelete,
        specs);
    if (newAddedLoadHistoryList != null && newAddedLoadHistoryList.length > 0) {
      physicalFactAndMeasureMetadataDeletion(
          absoluteTableIdentifier,
          newAddedLoadHistoryList,
          isForceDelete,
          specs);
    }
  }

  public static void physicalFactAndMeasureMetadataDeletion(
      AbsoluteTableIdentifier absoluteTableIdentifier,
      LoadMetadataDetails[] loadDetails,
      boolean isForceDelete,
      List<PartitionSpec> specs) {
    CarbonTable carbonTable = DataMapStoreManager.getInstance().getCarbonTable(
        absoluteTableIdentifier);
    List<TableDataMap> indexDataMaps = new ArrayList<>();
    try {
      for (TableDataMap dataMap : DataMapStoreManager.getInstance().getAllDataMap(carbonTable)) {
        if (dataMap.getDataMapSchema().isIndexDataMap()) {
          indexDataMaps.add(dataMap);
        }
      }
    } catch (IOException e) {
      LOGGER.warn(String.format(
          "Failed to get datamaps for %s.%s, therefore the datamap files could not be cleaned.",
          absoluteTableIdentifier.getDatabaseName(), absoluteTableIdentifier.getTableName()));
    }

    for (final LoadMetadataDetails oneLoad : loadDetails) {
      if (checkIfLoadCanBeDeletedPhysically(oneLoad, isForceDelete)) {
        try {
          if (oneLoad.getSegmentFile() != null) {
            SegmentFileStore
                .deleteSegment(absoluteTableIdentifier.getTablePath(), oneLoad.getSegmentFile(),
                    specs);
          } else {
            String path = getSegmentPath(absoluteTableIdentifier, oneLoad);
            boolean status = false;
            if (FileFactory.isFileExist(path, FileFactory.getFileType(path))) {
              CarbonFile file = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));
              CarbonFile[] filesToBeDeleted = file.listFiles(new CarbonFileFilter() {

                @Override public boolean accept(CarbonFile file) {
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

            } else {
              LOGGER.warn("Files are not found in segment " + path
                  + " it seems, files are already being deleted");
            }

          }
          List<Segment> segments = new ArrayList<>(1);
          for (TableDataMap dataMap : indexDataMaps) {
            segments.clear();
            segments.add(new Segment(oneLoad.getLoadName()));
            dataMap.deleteDatamapData(segments);
          }
        } catch (IOException e) {
          LOGGER.warn("Unable to delete the file as per delete command " + oneLoad.getLoadName());
        }
      }
    }
  }

  private static boolean checkIfLoadCanBeDeleted(SegmentDetailVO oneLoad,
      boolean isForceDelete) {
    if ((SegmentStatus.MARKED_FOR_DELETE.toString().equals(oneLoad.getStatus()) ||
        SegmentStatus.COMPACTED.toString().equals(oneLoad.getStatus()) ||
        SegmentStatus.INSERT_IN_PROGRESS.toString().equals(oneLoad.getStatus()) ||
        SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString().equals(oneLoad.getStatus()))
        && oneLoad.getVisibility()) {
      if (isForceDelete) {
        return true;
      }
      long deletionTime = oneLoad.getModificationOrDeletionTimestamp();

      return CarbonUpdateUtil.isMaxQueryTimeoutExceeded(deletionTime);

    }

    return false;
  }

  private static boolean checkIfLoadCanBeDeletedPhysically(LoadMetadataDetails oneLoad,
      boolean isForceDelete) {
    if ((SegmentStatus.MARKED_FOR_DELETE == oneLoad.getSegmentStatus() ||
        SegmentStatus.COMPACTED == oneLoad.getSegmentStatus())) {
      if (isForceDelete) {
        return true;
      }
      long deletionTime = oneLoad.getModificationOrdeletionTimesStamp();

      return CarbonUpdateUtil.isMaxQueryTimeoutExceeded(deletionTime);

    }

    return false;
  }

  public static boolean deleteLoadFoldersFromFileSystem(
      AbsoluteTableIdentifier absoluteTableIdentifier, boolean isForceDelete,
      List<SegmentDetailVO> details) {
    boolean isDeleted = false;
    if (details != null && details.size() != 0) {
      for (SegmentDetailVO oneLoad : details) {
        if (checkIfLoadCanBeDeleted(oneLoad, isForceDelete)) {
          ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
              CarbonTablePath.addSegmentPrefix(oneLoad.getSegmentId()) + LockUsage.LOCK);
          try {
            if (oneLoad.getStatus().equals(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString())
                || oneLoad.getStatus().equals(SegmentStatus.INSERT_IN_PROGRESS.toString())) {
              if (segmentLock.lockWithRetries(1, 5)) {
                LOGGER.info("Info: Acquired segment lock on segment:" + oneLoad.getSegmentId());
                SegmentDetailVO currentDetails =
                    new SegmentManager().getSegment(absoluteTableIdentifier, oneLoad.getSegmentId());
                if (currentDetails != null && checkIfLoadCanBeDeleted(currentDetails,
                    isForceDelete)) {
                  oneLoad.setVisibility(false);
                  isDeleted = true;
                  LOGGER.info("Info: Deleted the load " + oneLoad.getSegmentId());
                }
              } else {
                LOGGER.info("Info: Load in progress for segment" + oneLoad.getSegmentId());
                return isDeleted;
              }
            } else {
              oneLoad.setVisibility(false);
              isDeleted = true;
              LOGGER.info("Info: Deleted the load " + oneLoad.getSegmentId());
            }
          } finally {
            segmentLock.unlock();
            LOGGER.info("Info: Segment lock on segment:" + oneLoad.getSegmentId() + " is released");
          }
        }
      }
    }
    return isDeleted;
  }

}
