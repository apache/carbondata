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
 * 
 */
package com.huawei.unibi.molap.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.Math;

import org.pentaho.di.core.exception.KettleException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;

/**
 * @author R00903928
 * Util class for merge activities of 2 loads.
 */
public class MolapMergerUtil
{

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapMergerUtil.class.getName());

    /**
     * 
     * @param storeLocation
     * @param tableName
     * @param fileType
     * @param loadsToBeMerged
     * @return
     */
    public static List<MolapSliceAndFiles> getSliceAndFilesList(String storeLocation, String tableName,
            FileType fileType, List<String> loadsToBeMerged)
    {
        try
        {
            if(!FileFactory.isFileExist(storeLocation, fileType))
            {
                return new ArrayList<MolapSliceAndFiles>(0);
            }
        }
        catch(IOException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Error occurred :: " + e.getMessage());
        }
        MolapFile file = FileFactory.getMolapFile(storeLocation, fileType);

        MolapFile[] listFiles = MolapUtil.listFiles(file);
        if(null == listFiles || listFiles.length < 0)
        {
            return new ArrayList<MolapSliceAndFiles>(0);
        }
        Arrays.sort(listFiles, new MolapFileFolderComparator());
        listFiles = getMergeFilesList(loadsToBeMerged, listFiles);

        return MolapUtil.getSliceAndFilesList(tableName, listFiles, fileType);
    }

    /**
     * 
     * @param loadsToBeMerged
     * @param listFiles
     * @return
     */
    private static MolapFile[] getMergeFilesList(List<String> loadsToBeMerged, MolapFile[] listFiles)
    {
        MolapFile[] molapFile = new MolapFile[loadsToBeMerged.size()];
        int i = 0;
        for(MolapFile listFile : listFiles)
        {
            String loadName = listFile.getName();
            for(String load : loadsToBeMerged)
            {
                if((MolapCommonConstants.LOAD_FOLDER + load).equalsIgnoreCase(loadName))
                {
                    molapFile[i++] = listFile;
                }
            }
        }
        return molapFile;
    }

    /**
     * 
     * @param sliceLocation
     * @param tableName
     * @param destinationLocation
     */
    public static int[] mergeLevelMetadata(String[] sliceLocation, String tableName, String destinationLocation)
    {
        int[][] cardinalityOfLoads = new int[sliceLocation.length][];
        int i = 0;
        for(String loadFolderLoacation : sliceLocation)
        {
            try
            {
                cardinalityOfLoads[i++] = MolapUtil.getCardinalityFromLevelMetadataFile(loadFolderLoacation + '/'
                        + MolapCommonConstants.LEVEL_METADATA_FILE + tableName + ".metadata");
            }
            catch(MolapUtilException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Error occurred :: " + e.getMessage());
            }
        }
        int[] MaxCardinality = new int[cardinalityOfLoads[0].length];

        for(int k = 0;k < cardinalityOfLoads[0].length;k++)
        {
            MaxCardinality[k] = Math.max(cardinalityOfLoads[0][k], cardinalityOfLoads[1][k]);
        }

        try
        {
            MolapUtil.writeLevelCardinalityFile(destinationLocation, tableName, MaxCardinality);
        }
        catch(KettleException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Error occurred :: " + e.getMessage());
        }

        return MaxCardinality;
    }

    /**
     * 
     * @param path
     * @param tableName
     * @return
     */
    public static int[] getCardinalityFromLevelMetadata(String path, String tableName)
    {
        int[] localCardinality = null;
        try
        {
            localCardinality = MolapUtil.getCardinalityFromLevelMetadataFile(path + '/'
                    + MolapCommonConstants.LEVEL_METADATA_FILE + tableName + ".metadata");
        }
        catch(MolapUtilException e)
        {
            LOGGER.error(
                    MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG ,
                    "Error occurred :: " + e.getMessage());
        }

        return localCardinality;
    }

}
