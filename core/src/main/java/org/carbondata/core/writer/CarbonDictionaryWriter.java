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

package org.carbondata.core.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.thrift.TBase;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTypeIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.ThriftReader;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonDictionaryUtil;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.format.ColumnDictionaryChunk;
import org.carbondata.format.ColumnDictionaryChunkMeta;

/**
 * This class is responsible for writing the dictionary file and its metadata
 */
public class CarbonDictionaryWriter {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonDictionaryWriter.class.getName());

    /**
     * carbon type identifier
     */
    private CarbonTypeIdentifier carbonTypeIdentifier;

    /**
     * iterator over column values
     */
    private Iterator<String> columnUniqueValueListIterator;

    /**
     * column name
     */
    private String columnName;

    /**
     * segment name
     */
    private String segmentName;

    /**
     * HDFS store path
     */
    private String hdfsStorePath;

    /**
     * shared dimension flag
     */
    private boolean isSharedDimension;

    /**
     * directory path for dictionary file
     */
    private String directoryPath;

    /**
     * constructor
     */
    public CarbonDictionaryWriter(CarbonTypeIdentifier carbonTypeIdentifier,
            Iterator<String> columnUniqueValueListIterator, String columnName, String segmentName,
            String hdfsStorePath, boolean isSharedDimension) {
        this.carbonTypeIdentifier = carbonTypeIdentifier;
        this.columnUniqueValueListIterator = columnUniqueValueListIterator;
        this.columnName = columnName;
        this.segmentName = segmentName;
        this.hdfsStorePath = hdfsStorePath;
        this.isSharedDimension = isSharedDimension;
    }

    /**
     * This method will read the column unique values and convert them into bytes
     */
    public void processColumnUniqueValueList() throws CarbonUtilException {
        long startTime = System.currentTimeMillis();
        boolean created = checkAndCreateDirectoryPath();
        if (!created) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Failed to created dictionary path :: " + this.directoryPath);
            return;
        }
        String columnUniqueValue = null;
        int totalRecordCount = 0;
        // start offset of each segment dictionary
        long start_offset = 0L;
        List<String> columnUniqueValueList =
                new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        String dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(carbonTypeIdentifier, this.directoryPath, columnName,
                        isSharedDimension);
        // if dictionary file is written first time for a column then write default member value
        if (!CarbonUtil.isFileExists(dictionaryFilePath)) {
            columnUniqueValueList.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
            totalRecordCount++;
        } else {
            // start offset of dictionary for new segment
            start_offset = CarbonUtil.getFileSize(dictionaryFilePath);
        }
        while (columnUniqueValueListIterator.hasNext()) {
            columnUniqueValue = columnUniqueValueListIterator.next();
            columnUniqueValueList.add(columnUniqueValue);
            totalRecordCount++;
        }
        // case1: Data load for first time - in this case there will unique values
        // for a column and dictionary file will be written
        // case2: incremental load - there might not be unique values for a column
        // and hence no need to write the dictionary file
        if (!columnUniqueValueList.isEmpty()) {
            // write dictionary data
            writeDictionaryFile(dictionaryFilePath, columnUniqueValueList);
        }
        // write dictionary metadata
        writeDictionaryMetadataFile(totalRecordCount, start_offset);
        LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "Total time taken for writing dictionary file for column " + this.columnName
                        + " is " + (System.currentTimeMillis() - startTime) + " ms");
    }

    /**
     * This method will serialize the object of dictionary file
     */
    private void writeDictionaryFile(String dictionaryFilePath, List<String> columnUniqueValueList)
            throws CarbonUtilException {
        ColumnDictionaryChunk columnDictionaryChunk =
                getDictionaryThriftObject(columnUniqueValueList);
        boolean append = false;
        FileFactory.FileType fileType = FileFactory.getFileType(dictionaryFilePath);
        // if file exists then append the thrift object to an existing file
        if (CarbonUtil.isFileExists(dictionaryFilePath)) {
            append = true;
        }
        writeThriftObject(dictionaryFilePath, columnDictionaryChunk, append);
    }

    /**
     * This method will populate and return the thrift object
     */
    private ColumnDictionaryChunk getDictionaryThriftObject(List<String> columnUniqueValueList) {
        ColumnDictionaryChunk columnDictionaryChunk = new ColumnDictionaryChunk();
        columnDictionaryChunk.setValues(columnUniqueValueList);
        return columnDictionaryChunk;
    }

    /**
     * This method will check and created the directory path where dictionary file has to be created
     */
    private boolean checkAndCreateDirectoryPath() {
        this.directoryPath = CarbonDictionaryUtil
                .getDirectoryPath(carbonTypeIdentifier, hdfsStorePath, isSharedDimension);
        boolean created = CarbonUtil.checkAndCreateFolder(directoryPath);
        LOGGER.debug(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "Dictionary Folder creation status :: " + created);
        return created;
    }

    /**
     * This method will write the dictionary metadata file for a given column
     */
    private void writeDictionaryMetadataFile(int totalRecordCount, long start_offset)
            throws CarbonUtilException {
        // Format of dictionary metadata file
        // min, max, start offset, segment id
        ColumnDictionaryChunkMeta chunkMetaObjectForLastSegmentEntry = null;
        boolean append = false;
        int min = 0;
        int max = 0;
        String metadataFilePath = CarbonDictionaryUtil
                .getDictionaryMetadataFilePath(carbonTypeIdentifier, directoryPath, columnName,
                        isSharedDimension);
        FileFactory.FileType fileType = FileFactory.getFileType(metadataFilePath);
        // if file already exists then read metadata file and
        // get previousMax value and append the new thrift object to
        // the existing file
        if (CarbonUtil.isFileExists(metadataFilePath)) {
            chunkMetaObjectForLastSegmentEntry =
                    getChunkMetaObjectForLastSegmentEntry(metadataFilePath);
            append = true;
        }
        // case 1: first time dictionary writing
        // previousMax = 0, totalRecordCount = 5, min = 1, max= 5
        // case2: file already exists
        // previousMax = 5, totalRecordCount = 10, min = 6, max = 15
        // case 3: no unique values, total records 0
        // previousMax = 15, totalRecordCount = 0, min = 15, max = 15
        // both min and max equal to previous max
        if (null != chunkMetaObjectForLastSegmentEntry) {
            if (0 == totalRecordCount) {
                min = chunkMetaObjectForLastSegmentEntry.getMax_surrogate_key();
            } else {
                min = chunkMetaObjectForLastSegmentEntry.getMax_surrogate_key() + 1;
            }
            max = chunkMetaObjectForLastSegmentEntry.getMax_surrogate_key() + totalRecordCount;
        } else {
            if (totalRecordCount > 0) {
                min = 1;
            }
            max = totalRecordCount;
        }
        ColumnDictionaryChunkMeta chunkMeta = new ColumnDictionaryChunkMeta(min, max, start_offset);
        writeThriftObject(metadataFilePath, chunkMeta, append);
        LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "Dictionary metadata file written successfully for column " + this.columnName
                        + " at path " + metadataFilePath);
    }

    /**
     * This method will write the thrift object to a file
     */
    private void writeThriftObject(String filePath, TBase thriftObject, boolean append)
            throws CarbonUtilException {
        ThriftWriter thriftWriter = null;
        try {
            thriftWriter = new ThriftWriter(filePath, append);
            thriftWriter.open();
            thriftWriter.write(thriftObject);
        } catch (IOException e) {
            throw new CarbonUtilException(e.getMessage(), e);
        } finally {
            thriftWriter.close();
        }
    }

    /**
     * This method will read the dictionary chunk metadata thrift object for last entry
     */
    private ColumnDictionaryChunkMeta getChunkMetaObjectForLastSegmentEntry(String metadataFilePath)
            throws CarbonUtilException {
        ColumnDictionaryChunkMeta dictionaryChunkMeta = null;
        ThriftReader thriftIn = new ThriftReader(metadataFilePath, new ThriftReader.TBaseCreator() {
            @Override public TBase create() {
                return new ColumnDictionaryChunkMeta();
            }
        });
        try {
            // Open it
            thriftIn.open();
            // Read objects
            while (thriftIn.hasNext()) {
                dictionaryChunkMeta = (ColumnDictionaryChunkMeta) thriftIn.read();
            }
        } catch (IOException e) {
            throw new CarbonUtilException(e.getMessage(), e);
        } finally {
            // Close reader
            thriftIn.close();
        }
        return dictionaryChunkMeta;
    }

}
