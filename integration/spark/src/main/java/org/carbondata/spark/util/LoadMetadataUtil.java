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

/**
 * Project Name  : Carbon
 * Module Name   : CARBON Data Processor
 * Author    : R00903928
 * Created Date  : 15-Sep-2015
 * FileName   : LoadMetadataUtil.java
 * Description   : Kettle step to generate MD Key
 * Class Version  : 1.0
 */
package org.carbondata.spark.util;

import java.io.File;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.lcm.status.SegmentStatusManager;
import org.carbondata.spark.load.CarbonLoadModel;

public final class LoadMetadataUtil {
  private LoadMetadataUtil() {

  }

  public static boolean isLoadDeletionRequired(CarbonLoadModel loadModel) {
    CarbonTable cube = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
        .getCarbonTable(loadModel.getDatabaseName() + '_' + loadModel.getTableName());

    String metaDataLocation = cube.getMetaDataFilepath();
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(
        new AbsoluteTableIdentifier(
            CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier(loadModel.getDatabaseName(),
                loadModel.getTableName())));
    LoadMetadataDetails[] details = segmentStatusManager.readLoadMetadata(metaDataLocation);
    if (details != null && details.length != 0) {
      for (LoadMetadataDetails oneRow : details) {
        if ((CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(oneRow.getLoadStatus())
            || CarbonCommonConstants.SEGMENT_COMPACTED.equalsIgnoreCase(oneRow.getLoadStatus()))
            && oneRow.getVisibility().equalsIgnoreCase("true")) {
          return true;
        }
      }
    }

    return false;

  }

  public static String createLoadFolderPath(CarbonLoadModel model, String hdfsStoreLocation,
      int partitionId, int currentRestructNumber) {
    hdfsStoreLocation =
        hdfsStoreLocation + File.separator + model.getDatabaseName() + '_' + partitionId
            + File.separator + model.getTableName() + '_' + partitionId;
    int rsCounter = currentRestructNumber;
    if (rsCounter == -1) {
      rsCounter = 0;
    }
    String hdfsLoadedTable =
        hdfsStoreLocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER + rsCounter
            + File.separator + model.getTableName();
    hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");
    return hdfsLoadedTable;
  }

  public static CarbonFile[] getAggregateTableList(final CarbonLoadModel model,
      String hdfsStoreLocation, int partitionId, int currentRestructNumber) {
    hdfsStoreLocation =
        hdfsStoreLocation + File.separator + model.getDatabaseName() + '_' + partitionId
            + File.separator + model.getTableName() + '_' + partitionId;

    int rsCounter = currentRestructNumber;
    if (rsCounter == -1) {
      rsCounter = 0;
    }

    String hdfsLoadedTable =
        hdfsStoreLocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER + rsCounter;

    CarbonFile rsFile =
        FileFactory.getCarbonFile(hdfsLoadedTable, FileFactory.getFileType(hdfsLoadedTable));

    CarbonFile[] aggFiles = rsFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        return file.getName().startsWith(
            CarbonCommonConstants.AGGREGATE_TABLE_START_TAG + CarbonCommonConstants.UNDERSCORE
                + model.getTableName());
      }
    });

    return aggFiles;
  }
}
