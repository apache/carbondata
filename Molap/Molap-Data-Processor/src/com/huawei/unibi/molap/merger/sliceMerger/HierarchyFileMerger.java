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
61+XaZ5HF5eO450F+YoA8b9y5XeFRWrhSEu6FIu61UgUoSQBbiwHMYXqKsdLqMBlSVlqUgYo
446ty1BV9AdIFUi1TmXyEkWnszBTJG8+Tp7pc8nTMDCwzEauO0P+z2h3u/bt9g==*/
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
 * FileName : HierarchyFileMerger.java
 * Class Description : This class is responsible for merging the Hierarchy files 
 * Version 1.0
 */
public class HierarchyFileMerger
{
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(HierarchyFileMerger.class.getName());
    /**
     * merge location
     */
    private String mergeLocation;

    /**
     * hierarchy and its key size map
     */
    private Map<String, Integer> hierarchyAndKeySizeMap;
    
    /**
     * File manager
     */
    private IFileManagerComposite fileManager;

    /**
     * HierarchyFileMerger Constructor
     * 
     * @param mergeLocation
     *          merge location  
     * @param hierarchyAndKeySizeMap
     *       hierarchy and its key size map
     *
     */
    public HierarchyFileMerger(String mergeLocation, Map<String, Integer> hierarchyAndKeySizeMap)
    {
        this.mergeLocation = mergeLocation;
        this.hierarchyAndKeySizeMap = hierarchyAndKeySizeMap;
    }
    
    //TODO SIMIAN
    /**
     * This method is responsible for merging the Hierarchy files from multiple
     * location if unique file is present in any folder then it will be copied
     * to destination else file will be merged and merged file will be in sorted
     * order
     * 
     * @param slice location
     *          slice location
     * @throws SliceMergerException
     *          problem while merging     
     *          
     * 
     */ 
    
    public void mergerData(String[] sliceLocation) throws SliceMergerException
    {
        MolapFile[][] sliceFiles = new MolapFile[sliceLocation.length][];
        for (int k = 0; k < sliceLocation.length; k++)
        {
            sliceFiles[k] = getSortedPathForFiles(sliceLocation[k]);
        }
        Map<String, List<MolapFile>> filesMap = MolapSliceMergerUtil
                .getFileMap(sliceFiles);
        Set<Entry<String, List<MolapFile>>> entrySet = filesMap.entrySet();
        Iterator<Entry<String, List<MolapFile>>> iterator = entrySet.iterator();
        List<MolapFile> filesToBeCopied = new ArrayList<MolapFile>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        while (iterator.hasNext())
        {
            Entry<String, List<MolapFile>> next = iterator.next();
            List<MolapFile> value = next.getValue();
            
            if (value.size() == 1)
            {
                filesToBeCopied.add(value.get(0));
                iterator.remove();
            }
        }
        fileManager = new LoadFolderData();
        fileManager.setName(mergeLocation);
        String sourceFileName = "";
        try
        {
            for (MolapFile sourceFile : filesToBeCopied)
            {
                sourceFileName = sourceFile.getName()
                        + MolapCommonConstants.FILE_INPROGRESS_STATUS;
                String destFile = mergeLocation + File.separator
                        + sourceFileName;
                FileData fileData = new FileData(sourceFileName, mergeLocation);
                fileManager.add(fileData);

                MolapSliceMergerUtil.copyFile(sourceFile, destFile);
            }

        }
        catch (IOException e)
        {
            throw new SliceMergerException(
                    "Problem while copying the Hierarchy File "
                            + sourceFileName, e);
        }
        finally
        {
            int size = fileManager.size();

            for (int j = 0; j < size; j++)
            {
                FileData fileDataNew = (FileData) fileManager.get(j);

                String storePath = fileDataNew.getStorePath();
                String inProgFileName = fileDataNew.getFileName();
                String changedFileName = inProgFileName.substring(0,
                        inProgFileName.lastIndexOf('.'));
                File currentFile = new File(storePath + File.separator
                        + inProgFileName);
                File destHierFile = new File(storePath + File.separator
                        + changedFileName);

                if(!currentFile.renameTo(destHierFile))
                {
                    LOGGER.error(
                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem while renaming the file");
                }


                // fileManager.remove(fileData);
            }
        }

        HierarchyMergerExecuter mergeHierarchyFiles = new HierarchyMergerExecuter(
                filesMap, hierarchyAndKeySizeMap, mergeLocation);
        mergeHierarchyFiles.mergeExecuter();
    }

//    /**
//     * below method will be used to get the files 
//     * 
//     * @param sliceLocation
//     *          slocation locations
//     * @return sorted files
//     *
//     */
//    private MolapFile[] getSortedPathForFiles(String sliceLocation)
//    {
//        File file = new File(sliceLocation);
//        File[] files = null;
//        if(file.isDirectory())
//        {
//            files = file.listFiles(new FileFilter()
//            {
//                public boolean accept(File pathname)
//                {
//                    return (!pathname.isDirectory()) && pathname.getName().endsWith(".hierarchy");
//                }
//            });
//        }
//        /**
//         * Fortify Fix: FORWARD_NULL
//         */
//        if(null == files)
//        {
//            throw new IllegalArgumentException("Inavalid hierarchy file location");
//        }
//        return MolapUtil.getSortedFileList(files);
//    }
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
                if(!(pathname.isDirectory()) && pathname.getName().endsWith(".hierarchy"))
                {
                    return true;
                }
                return false;
            }
        });
        
        return MolapUtil.getSortedFileList(listFiles);
    }
}
