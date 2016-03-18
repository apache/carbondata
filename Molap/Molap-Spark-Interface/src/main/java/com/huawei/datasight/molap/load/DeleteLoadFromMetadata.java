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
 * Created Date  : 21-Sep-2015
 * FileName   : DeleteLoadFromMetadata.java
 * Description   : Kettle step to generate MD Key
 * Class Version  : 1.0
 */
package com.huawei.datasight.molap.load;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperations;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperationsImpl;
import com.huawei.unibi.molap.datastorage.store.fileperations.FileWriteOperation;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.locks.MetadataLock;
import com.huawei.unibi.molap.locks.MolapLock;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name : Carbon Module Name : MOLAP Data Processor Author : R00903928
 * Created Date : 21-Sep-2015 FileName : DeleteLoadFromMetadata.java Description
 * : Class Version : 1.0
 */
public final class DeleteLoadFromMetadata
{

    private static final LogService LOGGER = LogServiceFactory
            .getLogService(DeleteLoadFromMetadata.class.getName());
    private DeleteLoadFromMetadata()
    {
    	
    }

    public static List<String> updateDeletionStatus(List<String> loadIds,
            String cubeFolderPath)
    {
        MolapLock molapLock = new MetadataLock(cubeFolderPath);
        BufferedWriter brWriter = null;
        List<String> invalidLoadIds = new ArrayList<String>(0);
        try
        {
//            if(molapLock
//                    .lock(MolapCommonConstants.NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK,
//                            MolapCommonConstants.MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK))
//            {
        	if (molapLock.lockWithRetries()) {
                LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "Metadata lock has been successfully acquired");
                
                String dataLoadLocation = cubeFolderPath
                        + MolapCommonConstants.FILE_SEPARATOR + MolapCommonConstants.LOADMETADATA_FILENAME
                        + MolapCommonConstants.MOLAP_METADATA_EXTENSION;
                
                DataOutputStream dataOutputStream = null;
                Gson gsonObjectToWrite = new Gson();
                LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;
                
                if(!FileFactory.isFileExist(dataLoadLocation,
                        FileFactory.getFileType(dataLoadLocation)))
                {
                    // log error.
                    LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                            "Load metadata file is not present.");
                    return loadIds;
                }
                // read existing metadata details in load metadata.
                listOfLoadFolderDetailsArray = MolapUtil
                        .readLoadMetadata(cubeFolderPath);
                if(listOfLoadFolderDetailsArray!=null&&listOfLoadFolderDetailsArray.length != 0)
                {
                    updateDeletionStatusInDetails(loadIds,
                            listOfLoadFolderDetailsArray, invalidLoadIds);
                    if(!invalidLoadIds.isEmpty())
                    {
                        LOGGER.warn(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                                "Load doesnt exist or it is already deleted , LoadSeqId-"+invalidLoadIds);
                    }

                    AtomicFileOperations fileWrite = new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));
                    
                    // write the updated data into the metadata file.
                    
                    try
                    {
                    dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
                    
                   /* dataOutputStream = FileFactory.getDataOutputStream(
                            dataLoadLocation,
                            FileFactory.getFileType(dataLoadLocation));*/
                    brWriter = new BufferedWriter(
                            new OutputStreamWriter(
                                    dataOutputStream,
                                    MolapCommonConstants.MOLAP_DEFAULT_STREAM_ENCODEFORMAT));

                    String metadataInstance = gsonObjectToWrite
                            .toJson(listOfLoadFolderDetailsArray);
                    brWriter.write(metadataInstance);
                    }
                    finally
                    {
                        if(null != brWriter)
                        {
                            brWriter.flush();
                        }
                        MolapUtil.closeStreams(brWriter);
                    }
                    
                    fileWrite.close();
                    
                }
                else
                {
                    LOGGER.warn(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                            "Load doesnt exist or it is already deleted , LoadSeqId-"+loadIds);
                    return loadIds;
                }

            }
            else
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "Unable to acquire the metadata lock");
            }
        }
        catch(IOException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "IOException"+e.getMessage());
        }
        finally
        {
            fileUnlock(molapLock);
        }
        
        return invalidLoadIds;
    }

    /**
     * @param molapLock
     */
    public static void fileUnlock(MolapLock molapLock)
    {
        if(molapLock.unlock())
        {
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Metadata lock has been successfully released");
        }
        else
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Not able to release the metadata lock");
        }
    }

    /**
     * @param loadIds
     * @param listOfLoadFolderDetailsArray
     * @param bFound
     * @return
     */
    public static void updateDeletionStatusInDetails(List<String> loadIds,
            LoadMetadataDetails[] listOfLoadFolderDetailsArray,List<String> invalidLoadIds)
    {
        for(String loadId : loadIds)
        {
            boolean loadFound = false;
            // For each load id loop through data and if the
            // load id is found then mark
            // the metadata as deleted.
            for(LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray)
            {

                if(loadId.equalsIgnoreCase(loadMetadata.getLoadName()))
                {
                    loadFound = true;
                    if(!MolapCommonConstants.MARKED_FOR_DELETE
                            .equals(loadMetadata.getLoadStatus()))
                    {
                        loadMetadata
                                .setLoadStatus(MolapCommonConstants.MARKED_FOR_DELETE);
                        loadMetadata.setDeletionTimestamp(MolapLoaderUtil
                                .readCurrentTime());
                        LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "LoadId "+loadId+" Marked for Delete");
                    }
                    else
                    {
                        // it is already deleted . can not delete it again.
                        invalidLoadIds.add(loadId);
                    }

                    break;
                }
            }

            if(!loadFound)
            {
                invalidLoadIds.add(loadId);
            }

        }
        
    }

}
