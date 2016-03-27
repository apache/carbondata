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

package org.carbondata.core.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.pentaho.di.core.exception.KettleException;

/**
 * Util class for merge activities of 2 loads.
 */
public class CarbonMergerUtil {

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonMergerUtil.class.getName());

    public static List<CarbonSliceAndFiles> getSliceAndFilesList(String storeLocation,
            String tableName, FileType fileType, List<String> loadsToBeMerged) {
        try {
            if (!FileFactory.isFileExist(storeLocation, fileType)) {
                return new ArrayList<CarbonSliceAndFiles>(0);
            }
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error occurred :: " + e.getMessage());
        }
        CarbonFile file = FileFactory.getMolapFile(storeLocation, fileType);

        CarbonFile[] listFiles = CarbonUtil.listFiles(file);
        if (null == listFiles || listFiles.length < 0) {
            return new ArrayList<CarbonSliceAndFiles>(0);
        }
        Arrays.sort(listFiles, new CarbonFileFolderComparator());
        listFiles = getMergeFilesList(loadsToBeMerged, listFiles);

        return CarbonUtil.getSliceAndFilesList(tableName, listFiles, fileType);
    }

    private static CarbonFile[] getMergeFilesList(List<String> loadsToBeMerged,
            CarbonFile[] listFiles) {
        CarbonFile[] carbonFile = new CarbonFile[loadsToBeMerged.size()];
        int i = 0;
        for (CarbonFile listFile : listFiles) {
            String loadName = listFile.getName();
            for (String load : loadsToBeMerged) {
                if ((CarbonCommonConstants.LOAD_FOLDER + load).equalsIgnoreCase(loadName)) {
                    carbonFile[i++] = listFile;
                }
            }
        }
        return carbonFile;
    }

    public static int[] mergeLevelMetadata(String[] sliceLocation, String tableName,
            String destinationLocation) {
        int[][] cardinalityOfLoads = new int[sliceLocation.length][];
        int i = 0;
        for (String loadFolderLoacation : sliceLocation) {
            try {
                cardinalityOfLoads[i++] = CarbonUtil.getCardinalityFromLevelMetadataFile(
                        loadFolderLoacation + '/' + CarbonCommonConstants.LEVEL_METADATA_FILE
                                + tableName + ".metadata");
            } catch (CarbonUtilException e) {
                LOGGER.error(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "Error occurred :: " + e.getMessage());
            }
        }
        int[] MaxCardinality = new int[cardinalityOfLoads[0].length];

        for (int k = 0; k < cardinalityOfLoads[0].length; k++) {
            MaxCardinality[k] = Math.max(cardinalityOfLoads[0][k], cardinalityOfLoads[1][k]);
        }

        try {
            CarbonUtil.writeLevelCardinalityFile(destinationLocation, tableName, MaxCardinality);
        } catch (KettleException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error occurred :: " + e.getMessage());
        }

        return MaxCardinality;
    }

    public static int[] getCardinalityFromLevelMetadata(String path, String tableName) {
        int[] localCardinality = null;
        try {
            localCardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(
                    path + '/' + CarbonCommonConstants.LEVEL_METADATA_FILE + tableName
                            + ".metadata");
        } catch (CarbonUtilException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error occurred :: " + e.getMessage());
        }

        return localCardinality;
    }

}
