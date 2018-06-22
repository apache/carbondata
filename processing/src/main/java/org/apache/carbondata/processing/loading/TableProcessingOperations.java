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

package org.apache.carbondata.processing.loading;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentDetailVO;
import org.apache.carbondata.core.statusmanager.SegmentManager;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.commons.lang3.StringUtils;

public class TableProcessingOperations {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonLoaderUtil.class.getName());

  /**
   *
   * @param carbonTable
   * @param isCompactionFlow
   * @throws IOException
   */
  public static void deletePartialLoadDataIfExist(CarbonTable carbonTable,
      final boolean isCompactionFlow) throws IOException {
    final List<SegmentDetailVO> details =
        new SegmentManager().getAllSegments(carbonTable.getAbsoluteTableIdentifier())
            .getAllSegments();

    //delete folder which metadata no exist in tablestatus
    String partitionPath = CarbonTablePath.getPartitionDir(carbonTable.getTablePath());
    FileFactory.FileType fileType = FileFactory.getFileType(partitionPath);
    if (FileFactory.isFileExist(partitionPath, fileType)) {
      CarbonFile carbonFile = FileFactory.getCarbonFile(partitionPath, fileType);
      CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile path) {
          String segmentId =
              CarbonTablePath.DataFileUtil.getSegmentIdFromPath(path.getAbsolutePath() + "/dummy");
          boolean found = false;
          for (SegmentDetailVO detail : details) {
            if (detail.getSegmentId().equals(segmentId)) {
              found = true;
              break;
            }
          }
          return !found;
        }
      });
      for (int k = 0; k < listFiles.length; k++) {
        String segmentId = CarbonTablePath.DataFileUtil
            .getSegmentIdFromPath(listFiles[k].getAbsolutePath() + "/dummy");
        if (isCompactionFlow) {
          if (segmentId.contains(".")) {
            CarbonLoaderUtil.deleteStorePath(listFiles[k].getAbsolutePath());
          }
        } else {
          if (!segmentId.contains(".")) {
            CarbonLoaderUtil.deleteStorePath(listFiles[k].getAbsolutePath());
          }
        }
      }
    }
  }

  /**
   *
   * This method will delete the local data load folder location after data load is complete
   *
   * @param loadModel
   * @param isCompactionFlow COMPACTION keyword will be added to path to make path unique if true
   * @param isAltPartitionFlow Alter_Partition keyword will be added to path to make path unique if
   *                           true
   */
  public static void deleteLocalDataLoadFolderLocation(CarbonLoadModel loadModel,
      boolean isCompactionFlow, boolean isAltPartitionFlow) {
    String tableName = loadModel.getTableName();
    String databaseName = loadModel.getDatabaseName();
    String tempLocationKey = CarbonDataProcessorUtil
        .getTempStoreLocationKey(databaseName, tableName, loadModel.getSegmentId(),
            loadModel.getTaskNo(), isCompactionFlow, isAltPartitionFlow);
    deleteLocalDataLoadFolderLocation(tempLocationKey, tableName);
  }

  /**
   *
   * This method will delete the local data load folder location after data load is complete
   *
   * @param tempLocationKey temporary location set in carbon properties
   * @param tableName
   */
  public static void deleteLocalDataLoadFolderLocation(String tempLocationKey, String tableName) {

    // form local store location
    final String localStoreLocations = CarbonProperties.getInstance().getProperty(tempLocationKey);
    if (localStoreLocations == null) {
      throw new RuntimeException("Store location not set for the key " + tempLocationKey);
    }
    // submit local folder clean up in another thread so that main thread execution is not blocked
    ExecutorService localFolderDeletionService = Executors
        .newFixedThreadPool(1, new CarbonThreadFactory("LocalFolderDeletionPool:" + tableName));
    try {
      localFolderDeletionService.submit(new Callable<Void>() {
        @Override public Void call() throws Exception {
          long startTime = System.currentTimeMillis();
          String[] locArray = StringUtils.split(localStoreLocations, File.pathSeparator);
          for (String loc : locArray) {
            try {
              CarbonUtil.deleteFoldersAndFiles(new File(loc));
            } catch (IOException | InterruptedException e) {
              LOGGER.error(e, "Failed to delete local data load folder location: " + loc);
            }
          }
          LOGGER.info(
              "Deleted the local store location: " + localStoreLocations + " : Time taken: " + (
                  System.currentTimeMillis() - startTime));
          return null;
        }
      });
    } finally {
      if (null != localFolderDeletionService) {
        localFolderDeletionService.shutdown();
      }
    }

  }
}
