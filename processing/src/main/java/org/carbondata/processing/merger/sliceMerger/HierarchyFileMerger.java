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

package org.carbondata.processing.merger.sliceMerger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.filesystem.MolapFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.processing.merger.Util.MolapSliceMergerUtil;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;

public class HierarchyFileMerger {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(HierarchyFileMerger.class.getName());
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
     * @param mergeLocation          merge location
     * @param hierarchyAndKeySizeMap hierarchy and its key size map
     */
    public HierarchyFileMerger(String mergeLocation, Map<String, Integer> hierarchyAndKeySizeMap) {
        this.mergeLocation = mergeLocation;
        this.hierarchyAndKeySizeMap = hierarchyAndKeySizeMap;
    }

    /**
     * This method is responsible for merging the Hierarchy files from multiple
     * location if unique file is present in any folder then it will be copied
     * to destination else file will be merged and merged file will be in sorted
     * order
     *
     * @throws SliceMergerException problem while merging
     */

    public void mergerData(String[] sliceLocation) throws SliceMergerException {
        MolapFile[][] sliceFiles = new MolapFile[sliceLocation.length][];
        for (int k = 0; k < sliceLocation.length; k++) {
            sliceFiles[k] = getSortedPathForFiles(sliceLocation[k]);
        }
        Map<String, List<MolapFile>> filesMap = MolapSliceMergerUtil.getFileMap(sliceFiles);
        Set<Entry<String, List<MolapFile>>> entrySet = filesMap.entrySet();
        Iterator<Entry<String, List<MolapFile>>> iterator = entrySet.iterator();
        List<MolapFile> filesToBeCopied =
                new ArrayList<MolapFile>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        while (iterator.hasNext()) {
            Entry<String, List<MolapFile>> next = iterator.next();
            List<MolapFile> value = next.getValue();

            if (value.size() == 1) {
                filesToBeCopied.add(value.get(0));
                iterator.remove();
            }
        }
        fileManager = new LoadFolderData();
        fileManager.setName(mergeLocation);
        String sourceFileName = "";
        try {
            for (MolapFile sourceFile : filesToBeCopied) {
                sourceFileName = sourceFile.getName() + MolapCommonConstants.FILE_INPROGRESS_STATUS;
                String destFile = mergeLocation + File.separator + sourceFileName;
                FileData fileData = new FileData(sourceFileName, mergeLocation);
                fileManager.add(fileData);

                MolapSliceMergerUtil.copyFile(sourceFile, destFile);
            }

        } catch (IOException e) {
            throw new SliceMergerException(
                    "Problem while copying the Hierarchy File " + sourceFileName, e);
        } finally {
            int size = fileManager.size();

            for (int j = 0; j < size; j++) {
                FileData fileDataNew = (FileData) fileManager.get(j);

                String storePath = fileDataNew.getStorePath();
                String inProgFileName = fileDataNew.getFileName();
                String changedFileName =
                        inProgFileName.substring(0, inProgFileName.lastIndexOf('.'));
                File currentFile = new File(storePath + File.separator + inProgFileName);
                File destHierFile = new File(storePath + File.separator + changedFileName);

                if (!currentFile.renameTo(destHierFile)) {
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem while renaming the file");
                }

            }
        }

        HierarchyMergerExecuter mergeHierarchyFiles =
                new HierarchyMergerExecuter(filesMap, hierarchyAndKeySizeMap, mergeLocation);
        mergeHierarchyFiles.mergeExecuter();
    }

    /**
     * below method will be used to get the files
     *
     * @param sliceLocation slocation locations
     * @return sorted files
     */
    private MolapFile[] getSortedPathForFiles(String sliceLocation) {
        FileType fileType = FileFactory.getFileType(sliceLocation);
        MolapFile storeFolder = FileFactory.getMolapFile(sliceLocation, fileType);

        MolapFile[] listFiles = storeFolder.listFiles(new MolapFileFilter() {

            @Override
            public boolean accept(MolapFile pathname) {
                if (!(pathname.isDirectory()) && pathname.getName().endsWith(".hierarchy")) {
                    return true;
                }
                return false;
            }
        });

        return MolapUtil.getSortedFileList(listFiles);
    }
}
