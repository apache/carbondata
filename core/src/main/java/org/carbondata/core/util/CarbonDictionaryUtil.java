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

import org.carbondata.core.carbon.CarbonTypeIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * Utility class which will perform operations like forming path,
 * creating folder structure related to dictionary file operation
 */
public class CarbonDictionaryUtil {

    /**
     * This method will form the dictionary metadata file path for a column
     */
    public static String getDictionaryMetadataFilePath(CarbonTypeIdentifier carbonTypeIdentifier,
            String metadataFileDirPath, String columnName, boolean isSharedDimension) {
        // if dimension is shared between tables in a database then dictionary metadata
        // file for that
        // column should be created under shared directory
        StringBuilder metadataFilePathBuilder = new StringBuilder();
        String tableName = "";
        if (!isSharedDimension) {
            tableName = carbonTypeIdentifier.getTableName() + CarbonCommonConstants.UNDERSCORE;
        }
        metadataFilePathBuilder.append(metadataFileDirPath)
                .append(CarbonCommonConstants.FILE_SEPARATOR_CHAR).append(tableName)
                .append(columnName).append(CarbonCommonConstants.UNDERSCORE)
                .append(CarbonCommonConstants.DICTIONARY_CONSTANT)
                .append(CarbonCommonConstants.METADATA_CONSTANT)
                .append(CarbonCommonConstants.FILE_EXTENSION);
        return metadataFilePathBuilder.toString();
    }

    /**
     * This method will form the dictionary file path for a column
     */
    public static String getDictionaryFilePath(CarbonTypeIdentifier carbonTypeIdentifier,
            String filePath, String columnName, boolean isSharedDimension) {
        // if dimension is shared between tables in a database then dictionary file for that
        // column should be created under shared directory
        StringBuilder dictionaryFilePathBuilder = new StringBuilder();
        String tableName = "";
        if (!isSharedDimension) {
            tableName = carbonTypeIdentifier.getTableName() + CarbonCommonConstants.UNDERSCORE;
        }
        dictionaryFilePathBuilder.append(filePath).append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                .append(tableName).append(columnName).append(CarbonCommonConstants.UNDERSCORE)
                .append(CarbonCommonConstants.DICTIONARY_CONSTANT)
                .append(CarbonCommonConstants.FILE_EXTENSION);
        return dictionaryFilePathBuilder.toString();
    }

    /**
     * This method will return the path till shared directory folder or table
     * metadata folder for a column
     */
    public static String getDirectoryPath(CarbonTypeIdentifier carbonTypeIdentifier,
            String hdfsStorePath, boolean isSharedDimension) {
        StringBuilder dirPathBuilder = new StringBuilder();
        dirPathBuilder.append(hdfsStorePath).append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                .append(carbonTypeIdentifier.getDatabaseName());
        if (isSharedDimension) {
            dirPathBuilder.append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                    .append(CarbonCommonConstants.SHARED_DIRECTORY);
        } else {
            dirPathBuilder.append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                    .append(carbonTypeIdentifier.getTableName())
                    .append(CarbonCommonConstants.METADATA_CONSTANT)
                    .append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                    .append(CarbonCommonConstants.DICTIONARY_CONSTANT);
        }
        return dirPathBuilder.toString();
    }
}
