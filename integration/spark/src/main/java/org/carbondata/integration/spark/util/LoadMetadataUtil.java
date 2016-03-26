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
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928
 * Created Date  : 15-Sep-2015
 * FileName   : LoadMetadataUtil.java
 * Description   : Kettle step to generate MD Key
 * Class Version  : 1.0
 */
package org.carbondata.integration.spark.util;

import java.io.File;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.filesystem.MolapFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.integration.spark.load.MolapLoadModel;
import org.carbondata.integration.spark.load.MolapLoaderUtil;

public final class LoadMetadataUtil {
    private LoadMetadataUtil() {

    }

    public static boolean isLoadDeletionRequired(MolapLoadModel loadModel) {
        String metaDataLocation = MolapLoaderUtil
                .extractLoadMetadataFileLocation(loadModel.getSchema(), loadModel.getSchemaName(),
                        loadModel.getCubeName());
        LoadMetadataDetails[] details = MolapUtil.readLoadMetadata(metaDataLocation);
        if (details != null && details.length != 0) {
            for (LoadMetadataDetails oneRow : details) {
                if (MolapCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(oneRow.getLoadStatus())
                        && oneRow.getVisibility().equalsIgnoreCase("true")) {
                    return true;
                }
            }
        }

        return false;

    }

    public static String createLoadFolderPath(MolapLoadModel model, String hdfsStoreLocation,
            int partitionId, int currentRestructNumber) {
        hdfsStoreLocation =
                hdfsStoreLocation + File.separator + model.getSchemaName() + '_' + partitionId
                        + File.separator + model.getCubeName() + '_' + partitionId;
        int rsCounter = currentRestructNumber;
        if (rsCounter == -1) {
            rsCounter = 0;
        }
        String hdfsLoadedTable =
                hdfsStoreLocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                        + rsCounter + File.separator + model.getTableName();
        hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");
        return hdfsLoadedTable;
    }

    public static MolapFile[] getAggregateTableList(final MolapLoadModel model,
            String hdfsStoreLocation, int partitionId, int currentRestructNumber) {
        hdfsStoreLocation =
                hdfsStoreLocation + File.separator + model.getSchemaName() + '_' + partitionId
                        + File.separator + model.getCubeName() + '_' + partitionId;

        int rsCounter = currentRestructNumber;
        if (rsCounter == -1) {
            rsCounter = 0;
        }

        String hdfsLoadedTable =
                hdfsStoreLocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                        + rsCounter;

        MolapFile rsFile =
                FileFactory.getMolapFile(hdfsLoadedTable, FileFactory.getFileType(hdfsLoadedTable));

        MolapFile[] aggFiles = rsFile.listFiles(new MolapFileFilter() {

            @Override
            public boolean accept(MolapFile file) {
                return file.getName().startsWith(MolapCommonConstants.AGGREGATE_TABLE_START_TAG
                        + MolapCommonConstants.UNDERSCORE + model.getTableName());
            }
        });

        return aggFiles;
    }
}
