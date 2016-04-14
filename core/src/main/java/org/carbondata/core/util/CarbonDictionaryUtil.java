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

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * Utility class which will perform operations like forming path,
 * creating folder structure related to dictionary file operation
 */
public class CarbonDictionaryUtil {

    /**
     * This method will form the dictionary metadata file path for a column
     *
     * @param carbonTableIdentifier table identifier which will give table name and
     *                              database name
     * @param metadataFileDirPath   dictionary metadata directory path
     * @param columnIdentifier      column unique identifier
     * @param isSharedDimension     flag for shared dimension
     * @return dictionary metadata file path
     */
    public static String getDictionaryMetadataFilePath(CarbonTableIdentifier carbonTableIdentifier,
            String metadataFileDirPath, String columnIdentifier, boolean isSharedDimension) {
        // if dimension is shared between tables in a database then dictionary metadata
        // file for that
        // column should be created under shared directory
        StringBuilder metadataFilePathBuilder = new StringBuilder();
        String tableName = "";
        if (!isSharedDimension) {
            tableName = carbonTableIdentifier.getTableName() + CarbonCommonConstants.UNDERSCORE;
        }
        metadataFilePathBuilder.append(metadataFileDirPath)
                .append(CarbonCommonConstants.FILE_SEPARATOR_CHAR).append(tableName)
                .append(columnIdentifier).append(CarbonCommonConstants.UNDERSCORE)
                .append(CarbonCommonConstants.DICTIONARY_CONSTANT)
                .append(CarbonCommonConstants.METADATA_CONSTANT)
                .append(CarbonCommonConstants.FILE_EXTENSION);
        return metadataFilePathBuilder.toString();
    }

    /**
     * This method will form the dictionary file path for a column
     *
     * @param carbonTableIdentifier table identifier which will give table name and
     *                              database name
     * @param dictionaryLocation    dictionary file directory path
     * @param columnIdentifier      column unique identifier
     * @param isSharedDimension     flag for shared dimension
     * @return dictionary file path
     */
    public static String getDictionaryFilePath(CarbonTableIdentifier carbonTableIdentifier,
            String dictionaryLocation, String columnIdentifier, boolean isSharedDimension) {
        // if dimension is shared between tables in a database then dictionary file for that
        // column should be created under shared directory
        StringBuilder dictionaryFilePathBuilder = new StringBuilder();
        String tableName = "";
        if (!isSharedDimension) {
            tableName = carbonTableIdentifier.getTableName() + CarbonCommonConstants.UNDERSCORE;
        }
        dictionaryFilePathBuilder.append(dictionaryLocation)
                .append(CarbonCommonConstants.FILE_SEPARATOR_CHAR).append(tableName)
                .append(columnIdentifier).append(CarbonCommonConstants.UNDERSCORE)
                .append(CarbonCommonConstants.DICTIONARY_CONSTANT)
                .append(CarbonCommonConstants.FILE_EXTENSION);
        return dictionaryFilePathBuilder.toString();
    }

    /**
     * This method will return the path till shared directory folder or table
     * metadata folder for a column
     *
     * @param carbonTableIdentifier table identifier which will give table name and
     *                              database name
     * @param hdfsStorePath         HDFS store path
     * @param isSharedDimension     flag for shared dimension
     * @return dictionary location
     */
    public static String getDirectoryPath(CarbonTableIdentifier carbonTableIdentifier,
            String hdfsStorePath, boolean isSharedDimension) {
        StringBuilder dirPathBuilder = new StringBuilder();
        dirPathBuilder.append(hdfsStorePath).append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                .append(carbonTableIdentifier.getDatabaseName());
        if (isSharedDimension) {
            dirPathBuilder.append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                    .append(CarbonCommonConstants.SHARED_DIRECTORY);
        } else {
            dirPathBuilder.append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                    .append(carbonTableIdentifier.getTableName())
                    .append(CarbonCommonConstants.METADATA_CONSTANT)
                    .append(CarbonCommonConstants.FILE_SEPARATOR_CHAR)
                    .append(CarbonCommonConstants.DICTIONARY_CONSTANT);
        }
        return dirPathBuilder.toString();
    }

}
