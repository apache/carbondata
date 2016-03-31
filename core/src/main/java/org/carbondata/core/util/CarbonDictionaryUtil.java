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

import java.io.File;
import java.io.IOException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTypeIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Utility class which will perform operations like forming path,
 * creating folder structure related to dictionary file operation
 */
public class CarbonDictionaryUtil {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonDictionaryUtil.class.getName());

    /**
     * This method will check the existence of a file at a given path
     */
    public static boolean isFileExists(String fileName) {
        try {
            FileFactory.FileType fileType = FileFactory.getFileType(fileName);
            if (FileFactory.isFileExist(fileName, fileType)) {
                return true;
            }
        } catch (IOException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "@@@@@@  File not found at a given location @@@@@@ : " + fileName);
        }
        return false;
    }

    /**
     * This method will form the dictionary metadata file path for a column
     */
    public static String getDictionaryMetadataFilePath(CarbonTypeIdentifier carbonTypeIdentifier,
            String metadataFileDirPath, String columnName, boolean isSharedDimension) {
        // if dimension is shared between tables in a database then dictionary metadata
        // file for that
        // column should be created under shared directory
        if (isSharedDimension) {
            metadataFileDirPath = metadataFileDirPath + File.separator + columnName
                    + CarbonCommonConstants.UNDERSCORE + CarbonCommonConstants.DICTIONARY_CONSTANT
                    + CarbonCommonConstants.METADATA_CONSTANT
                    + CarbonCommonConstants.FILE_EXTENSION;
        } else {
            metadataFileDirPath =
                    metadataFileDirPath + File.separator + carbonTypeIdentifier.getTableName()
                            + CarbonCommonConstants.UNDERSCORE + columnName
                            + CarbonCommonConstants.UNDERSCORE
                            + CarbonCommonConstants.DICTIONARY_CONSTANT
                            + CarbonCommonConstants.METADATA_CONSTANT
                            + CarbonCommonConstants.FILE_EXTENSION;
        }
        return metadataFileDirPath;
    }

    /**
     * This method will form the dictionary file path for a column
     */
    public static String getDictionaryFilePath(CarbonTypeIdentifier carbonTypeIdentifier,
            String filePath, String columnName, boolean isSharedDimension) {
        // if dimension is shared between tables in a database then dictionary file for that
        // column should be created under shared directory
        if (isSharedDimension) {
            filePath = filePath + File.separator + columnName + CarbonCommonConstants.UNDERSCORE
                    + CarbonCommonConstants.DICTIONARY_CONSTANT
                    + CarbonCommonConstants.FILE_EXTENSION;
        } else {
            filePath = filePath + File.separator + carbonTypeIdentifier.getTableName()
                    + CarbonCommonConstants.UNDERSCORE + columnName
                    + CarbonCommonConstants.UNDERSCORE + CarbonCommonConstants.DICTIONARY_CONSTANT
                    + CarbonCommonConstants.FILE_EXTENSION;
        }
        return filePath;
    }

    /**
     * This method will return the path till shared directory folder or table
     * metadata folder for a column
     */
    public static String getDirectoryPath(CarbonTypeIdentifier carbonTypeIdentifier,
            String hdfsStorePath, boolean isSharedDimension) {
        String filePath = hdfsStorePath + File.separator + carbonTypeIdentifier.getDatabaseName();
        if (isSharedDimension) {
            filePath = filePath + File.separator + CarbonCommonConstants.SHARED_DIRECTORY;
        } else {
            filePath = filePath + File.separator + carbonTypeIdentifier.getTableName()
                    + CarbonCommonConstants.METADATA_CONSTANT + File.separator
                    + CarbonCommonConstants.DICTIONARY_CONSTANT;
        }
        return filePath;
    }

    /**
     * This method will check and create the given path
     */
    public static boolean checkAndCreateFolder(String path) {
        boolean created = false;
        try {
            FileFactory.FileType fileType = FileFactory.getFileType(path);
            if (FileFactory.isFileExist(path, fileType)) {
                created = true;
            } else {
                created = FileFactory.mkdirs(path, fileType);
            }
        } catch (IOException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
        }
        return created;
    }
}
