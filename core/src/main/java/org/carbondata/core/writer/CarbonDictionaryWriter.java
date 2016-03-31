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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDictionaryMetadata;
import org.carbondata.core.carbon.CarbonTypeIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.carbondata.core.util.CarbonDictionaryUtil;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.format.ColumnDictionaryChunk;
import org.carbondata.query.util.CarbonEngineLogEvent;

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
     * buffer size
     */
    private static final int bufferSize = 2048;

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
     * end offset for current segment
     */
    private long currentSegmentEndOffset;

    /**
     * directory path for dictionary file
     */
    private String directoryPath;

    /**
     * one segment entry length
     * 4 byte each for segmentId, min, max value and 8 byte for endOffset which is of long type
     */
    private static final int ONE_SEGMENT_DETAIL_LENGTH = 20;

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
    public void processColumnUniqueValueList() {
        long startTime = System.currentTimeMillis();
        boolean created = checkAndCreateDirectoryPath();
        if (!created) {
            LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "Failed to created dictionary path :: " + this.directoryPath);
            return;
        }
        String columnUniqueValue = null;
        byte[] columnValueInBytes = null;
        int totalRecordCount = 0;
        List<ByteBuffer> columnUniqueValueList =
                new ArrayList<ByteBuffer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        String dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(carbonTypeIdentifier, this.directoryPath, columnName,
                        isSharedDimension);
        // if dictionary file is written first time for a column then write default member value
        if (!CarbonDictionaryUtil.isFileExists(dictionaryFilePath)) {
            byte[] defaultValueInBytes = CarbonCommonConstants.MEMBER_DEFAULT_VAL.getBytes();
            columnUniqueValueList.add(getByteBuffer(defaultValueInBytes));
            incrementOffset(CarbonCommonConstants.INT_SIZE_IN_BYTE + defaultValueInBytes.length);
            totalRecordCount++;
        }
        while (columnUniqueValueListIterator.hasNext()) {
            columnUniqueValue = columnUniqueValueListIterator.next();
            columnValueInBytes = columnUniqueValue.getBytes();
            columnUniqueValueList.add(getByteBuffer(columnValueInBytes));
            incrementOffset(CarbonCommonConstants.INT_SIZE_IN_BYTE + columnValueInBytes.length);
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
        writeDictionaryMetadataFile(totalRecordCount);
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Total time taken for writing dictionary file for column " + this.columnName
                        + " is " + (System.currentTimeMillis() - startTime) + " ms");
    }

    /**
     * This method will put column value to byte buffer
     */
    private ByteBuffer getByteBuffer(byte[] columnValueInBytes) {
        ByteBuffer buffer = ByteBuffer
                .allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE + columnValueInBytes.length);
        buffer.putInt(columnValueInBytes.length);
        buffer.put(columnValueInBytes, 0, columnValueInBytes.length);
        buffer.rewind();
        return buffer;
    }

    /**
     * This method will serialize the object of dictionary file
     */
    private void writeDictionaryFile(String dictionaryFilePath,
            List<ByteBuffer> columnUniqueValueList) {
        ColumnDictionaryChunk columnDictionaryChunk = getThriftObject(columnUniqueValueList);
        ThriftWriter thriftWriter = null;
        boolean append = false;
        try {
            FileFactory.FileType fileType = FileFactory.getFileType(dictionaryFilePath);
            if (FileFactory.isFileExist(dictionaryFilePath, fileType)) {
                append = true;
            }
            thriftWriter = new ThriftWriter(dictionaryFilePath, append);
            thriftWriter.open();
            thriftWriter.write(columnDictionaryChunk);
        } catch (IOException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
        } finally {
            thriftWriter.close();
        }
    }

    /**
     * This method will populate and return the thrift object
     */
    private ColumnDictionaryChunk getThriftObject(List<ByteBuffer> columnUniqueValueList) {
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
        boolean created = CarbonDictionaryUtil.checkAndCreateFolder(directoryPath);
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Dictionary Folder creation status :: " + created);
        return created;
    }

    /**
     * This method will write the dictionary metadata file for a given column
     */
    private void writeDictionaryMetadataFile(int totalRecordCount) {
        // Format of dictionary metadata file
        // total length of 1 segment detail entry which
        // includes in order : segment id, min, max, currentSegmentEndOffset
        DataOutputStream dataOutputStream = null;
        CarbonDictionaryMetadata dictionaryMetadata = null;
        int min = 0;
        int max = 0;
        String metadataFilePath = CarbonDictionaryUtil
                .getDictionaryMetadataFilePath(carbonTypeIdentifier, directoryPath, columnName,
                        isSharedDimension);
        try {
            FileFactory.FileType fileType = FileFactory.getFileType(metadataFilePath);
            int segmentId = Integer.parseInt(
                    segmentName.substring(CarbonCommonConstants.SEGMENT_CONSTANT.length()));
            // if file already exists then read metadata file and
            // get previousMax and previousSegmentEndOffset
            // value for the last loaded segment
            if (CarbonDictionaryUtil.isFileExists(metadataFilePath)) {
                dictionaryMetadata = CarbonDictionaryMetadataReader
                        .readAndGetDictionaryMetadataForLastSegment(metadataFilePath,
                                ONE_SEGMENT_DETAIL_LENGTH);
                // append data to existing dictionary metadata file
                dataOutputStream = FileFactory
                        .getDataOutputStream(metadataFilePath, fileType, true, bufferSize);
            } else {
                // create new file and write dictionary metadata to it
                dataOutputStream = FileFactory.getDataOutputStream(metadataFilePath, fileType);
            }
            // case 1: first time dictionary writing
            // previousMax = 0, totalRecordCount = 5, min = 1, max= 5
            // case2: file already exists
            // previousMax = 5, totalRecordCount = 10, min = 6, max = 15
            // case 3: no unique values, total records 0
            // previousMax = 15, totalRecordCount = 0, min = 15, max = 15
            // both min and max equal to previous max
            if (null != dictionaryMetadata) {
                if (0 == totalRecordCount) {
                    min = dictionaryMetadata.getMax();
                } else {
                    min = dictionaryMetadata.getMax() + 1;
                }
                max = dictionaryMetadata.getMax() + totalRecordCount;
                this.currentSegmentEndOffset =
                        dictionaryMetadata.getOffset() + this.currentSegmentEndOffset;
            } else {
                if (totalRecordCount > 0) {
                    min = 1;
                }
                max = totalRecordCount;
            }
            dataOutputStream.writeInt(segmentId); // write segment id
            dataOutputStream.writeInt(min); // write min value
            dataOutputStream.writeInt(max); // write max value
            dataOutputStream.writeLong(
                    this.currentSegmentEndOffset); // write end offset for current segment
            LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "Dictionary metadata file written successfully for column " + this.columnName
                            + " at path " + metadataFilePath);
        } catch (IOException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
        } finally {
            CarbonUtil.closeStreams(dataOutputStream);
        }
    }

    /**
     * increment the offset by given length
     */
    private void incrementOffset(int length) {
        this.currentSegmentEndOffset = this.currentSegmentEndOffset + length;
    }

}
