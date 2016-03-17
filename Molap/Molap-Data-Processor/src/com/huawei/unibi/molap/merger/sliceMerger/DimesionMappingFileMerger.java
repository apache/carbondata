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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XaakOZfZ+5pMBLkC6BE6zHPpjsfnb6rVp9llkBNma9gUkoqMxsIubh+7bMv0BAWvkOtGg
zNUpO0VtGfzzI2xdMGcJJCcDR9zE5/g3GLui2TC1qSvEfY63Ni1WkzT7YugjcQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.merger.sliceMerger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.file.manager.composite.FileData;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.file.manager.composite.LoadFolderData;
import com.huawei.unibi.molap.merger.Util.MolapSliceMergerUtil;
import com.huawei.unibi.molap.merger.exeception.SliceMergerException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : DimesionMappingFileMerger.java
 * Class Description : This class is responsible for merging the dimension files 
 * Version 1.0
 */
public class DimesionMappingFileMerger 
{
    
    /**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(DimesionMappingFileMerger.class.getName()); 
    
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(DimesionMappingFileMerger.class.getName());

    /**
     * merge location
     */
    private String mergeLocation;
    
    /**
     * File manager
     */
    private IFileManagerComposite fileManager;
    
    /**
     * DimesionMappingFileMerger  Constructor
     * 
     * @param mergeLocation
     *          merge location
     *
     */
    public DimesionMappingFileMerger(String mergeLocation)
    {
        this.mergeLocation=mergeLocation;
    }
    
    /**
     * This method is responsible for merging the dimension files from multiple
     * 
     * location if unique file is present in any folder then it will be copied
     * to destination 
     * 
     * @param slice location
     *          slice location
     * @throws SliceMergerException
     *          problem while merging     
     *          
     * 
     */
    public void mergerDimesionFile(String[] sliceLocation, boolean needToSkipAnyFile, List<String>fileNames) throws SliceMergerException
    {
        MolapFile[][] sliceFiles = new MolapFile[sliceLocation.length][];
        for(int i = 0;i < sliceLocation.length;i++)
        {
            sliceFiles[i] = getSortedPathForFiles(sliceLocation[i]);
        }
        Map<String, List<MolapFile>> filesMap = MolapSliceMergerUtil
                .getFileMap(sliceFiles);
        Set<Entry<String, List<MolapFile>>> entrySet = filesMap.entrySet();
        Iterator<Entry<String, List<MolapFile>>> iterator = entrySet.iterator();
        List<MolapFile> filesToBeCopied = new ArrayList<MolapFile>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        while(iterator.hasNext())
        {
            Entry<String, List<MolapFile>> next = iterator.next();
            if(needToSkipAnyFile)
            {
                if(fileNames.contains(next.getKey()))
                {
                    continue;
                }
            }
            List<MolapFile> value = next.getValue();
            if(value.size() == 1)
            {
                filesToBeCopied.add(value.get(0));
                iterator.remove();
            }
            else
            {
                filesToBeCopied.addAll(value);
            }
        }
        try
        {
            fileManager = new LoadFolderData();
            fileManager.setName(mergeLocation);
            for(MolapFile sourceFile : filesToBeCopied)
            {
                String sourceFileName = sourceFile.getName() + MolapCommonConstants.FILE_INPROGRESS_STATUS;
                String destFile = mergeLocation + File.separator
                        + sourceFileName;
                FileData fileData = new FileData(sourceFileName, mergeLocation);
                fileManager.add(fileData);
                MolapSliceMergerUtil.copyFile(sourceFile, 
                      destFile);
            }
        }
        catch(IOException e)
        {
            throw new SliceMergerException(
                    "Problem while copying the Dimension File ", e);
        }
        finally
        {
            // Encrypt the dimension mapping File.
            int size = fileManager.size();
            for(int j = 0;j < size; j++)
            {
                FileData fileData = (FileData)fileManager.get(j);
                String storePath = fileData.getStorePath();
                String inProgFileName = fileData.getFileName();
                String changedFileName = inProgFileName.substring(0,
                        inProgFileName.lastIndexOf('.'));
                
                File inProgress = new File(storePath + File.separator +  inProgFileName);
                File destFile = new File(storePath + File.separator + changedFileName);
                if(!inProgress.renameTo(destFile))
                {
                    LOGGER.error(
                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem while renaming the file");
                }

            }
        }
    }
    
    /**
     * below method will be used to get the files 
     * 
     * @param sliceLocation
     *          slocation locations
     * @return sorted files
     *
     */
    private MolapFile[] getSortedPathForFiles(String sliceLocation)
    {
        FileType fileType = FileFactory.getFileType(sliceLocation);
        MolapFile storeFolder = FileFactory.getMolapFile(sliceLocation, fileType);
        
        MolapFile[] listFiles = storeFolder.listFiles(new MolapFileFilter()
        {
            
            @Override
            public boolean accept(MolapFile pathname)
            {
                if((!pathname.isDirectory()) && !pathname.getName().contains("msrMetaData")
                        && !pathname.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT)
                        && !pathname.getName().endsWith(".hierarchy"))
                {
                    return true;
                }
                return false;
            }
        });
        
        return MolapUtil.getSortedFileList(listFiles);
    }
}
