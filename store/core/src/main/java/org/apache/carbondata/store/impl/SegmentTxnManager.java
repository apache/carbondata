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

package org.apache.carbondata.store.impl;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

class SegmentTxnManager {

  private LogService LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());
  private static final SegmentTxnManager instance = new SegmentTxnManager();

  static SegmentTxnManager getInstance() {
    return instance;
  }

  void openSegment(CarbonLoadModel loadModel, boolean isOverwriteTable) throws IOException {
    try {
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, isOverwriteTable);
    } catch (IOException e) {
      LOGGER.error(e, "Failed to handle load data");
      throw e;
    }
  }

  void closeSegment(CarbonLoadModel loadModel) throws IOException {
    try {
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel, "");
    } catch (IOException e) {
      LOGGER.error(e, "Failed to close segment");
      throw e;
    }
  }

  void commitSegment(CarbonLoadModel loadModel) throws IOException {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    String segmentId = loadModel.getSegmentId();
    String segmentFileName = SegmentFileStore
        .writeSegmentFile(carbonTable, segmentId, String.valueOf(loadModel.getFactTimeStamp()));

    AbsoluteTableIdentifier absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
    String tablePath = absoluteTableIdentifier.getTablePath();
    String metadataPath = CarbonTablePath.getMetadataPath(tablePath);
    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(tablePath);

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    int retryCount = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
    int maxTimeout = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info("Acquired lock for tablepath" + tablePath + " for table status updation");
        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(metadataPath);
        LoadMetadataDetails loadMetadataDetails = null;
        for (LoadMetadataDetails detail : listOfLoadFolderDetailsArray) {
          // if the segments is in the list of marked for delete then update the status.
          if (segmentId.equals(detail.getLoadName())) {
            loadMetadataDetails = detail;
            detail.setSegmentFile(segmentFileName);
            break;
          }
        }
        if (loadMetadataDetails == null) {
          throw new IOException("can not find segment: " + segmentId);
        }

        CarbonLoaderUtil.populateNewLoadMetaEntry(loadMetadataDetails, SegmentStatus.SUCCESS,
            loadModel.getFactTimeStamp(), true);
        CarbonLoaderUtil
            .addDataIndexSizeIntoMetaEntry(loadMetadataDetails, segmentId, carbonTable);

        SegmentStatusManager
            .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table path " + tablePath);
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + tablePath);
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + tablePath + " during table status updation");
      }
    }
  }
}
