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

package org.apache.carbondata.core.mutate.data;

import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;

import org.apache.hadoop.conf.Configuration;

/**
 * This class is responsible for loading delete delta file cache based on
 * blocklet id of a particular block
 */
public class BlockletDeleteDeltaCacheLoader implements DeleteDeltaCacheLoaderIntf {
  private String blockletID;
  private DataRefNode blockletNode;
  private AbsoluteTableIdentifier absoluteIdentifier;
  private Configuration configuration;
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockletDeleteDeltaCacheLoader.class.getName());

  public BlockletDeleteDeltaCacheLoader(String blockletID, DataRefNode blockletNode,
      AbsoluteTableIdentifier absoluteIdentifier, Configuration configuration) {
    this.blockletID = blockletID;
    this.blockletNode = blockletNode;
    this.absoluteIdentifier = absoluteIdentifier;
    this.configuration = configuration;
  }

  /**
   * This method will load the delete delta cache based on blocklet id of particular block with
   * the help of SegmentUpdateStatusManager.
   */
  public void loadDeleteDeltaFileDataToCache() {
    SegmentUpdateStatusManager segmentUpdateStatusManager =
        new SegmentUpdateStatusManager(absoluteIdentifier, configuration);
    Map<Integer, Integer[]> deleteDeltaFileData = null;
    BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache = null;
    if (null == blockletNode.getDeleteDeltaDataCache()) {
      try {
        deleteDeltaFileData =
            segmentUpdateStatusManager.getDeleteDeltaDataFromAllFiles(blockletID);
        deleteDeltaDataCache = new BlockletLevelDeleteDeltaDataCache(deleteDeltaFileData,
            segmentUpdateStatusManager.getTimestampForRefreshCache(blockletID, null));
      } catch (Exception e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Unable to retrieve delete delta files");
        }
      }
    } else {
      deleteDeltaDataCache = blockletNode.getDeleteDeltaDataCache();
      // if already cache is present then validate the cache using timestamp
      String cacheTimeStamp = segmentUpdateStatusManager
          .getTimestampForRefreshCache(blockletID, deleteDeltaDataCache.getCacheTimeStamp());
      if (null != cacheTimeStamp) {
        try {
          deleteDeltaFileData =
              segmentUpdateStatusManager.getDeleteDeltaDataFromAllFiles(blockletID);
          deleteDeltaDataCache = new BlockletLevelDeleteDeltaDataCache(deleteDeltaFileData,
              segmentUpdateStatusManager.getTimestampForRefreshCache(blockletID, cacheTimeStamp));
        } catch (Exception e) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Unable to retrieve delete delta files");
          }
        }
      }
    }
    blockletNode.setDeleteDeltaDataCache(deleteDeltaDataCache);
  }
}
