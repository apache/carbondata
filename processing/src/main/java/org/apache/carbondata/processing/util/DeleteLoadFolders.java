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

package org.apache.carbondata.processing.util;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;

public final class DeleteLoadFolders {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DeleteLoadFolders.class.getName());

  private DeleteLoadFolders() {

  }

  /**
   * returns segment path
   *
   * @param absoluteTableIdentifier
   * @param oneLoad
   * @return
   */
  private static String getSegmentPath(AbsoluteTableIdentifier absoluteTableIdentifier,
      LoadMetadataDetails oneLoad) {
    CarbonTablePath carbon = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier);
    String segmentId = oneLoad.getLoadName();
    return carbon.getCarbonDataDirectoryPath(segmentId);
  }

  public static void physicalFactAndMeasureMetadataDeletion(
      AbsoluteTableIdentifier absoluteTableIdentifier, String metadataPath, boolean isForceDelete) {
    LoadMetadataDetails[] currentDetails = SegmentStatusManager.readLoadMetadata(metadataPath);
    for (LoadMetadataDetails oneLoad : currentDetails) {
      if (checkIfLoadCanBeDeletedPhysically(oneLoad, isForceDelete)) {
        String path = getSegmentPath(absoluteTableIdentifier, oneLoad);
        boolean status = false;
        try {
          if (FileFactory.isFileExist(path, FileFactory.getFileType(path))) {
            CarbonFile file = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));
            CarbonFile[] filesToBeDeleted = file.listFiles(new CarbonFileFilter() {

              @Override public boolean accept(CarbonFile file) {
                return (CarbonTablePath.isCarbonDataFile(file.getName())
                    || CarbonTablePath.isCarbonIndexFile(file.getName())
                    || CarbonTablePath.isPartitionMapFile(file.getName()));
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
                LOGGER.warn(
                    "Unable to delete the folder as per delete command " + file.getAbsolutePath());
              }
            }

          } else {
            LOGGER.warn("Files are not found in segment " + path
                + " it seems, files are already being deleted");
          }
        } catch (IOException e) {
          LOGGER.warn("Unable to delete the file as per delete command " + path);
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
