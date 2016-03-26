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

import java.io.*;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.metadata.LeafNodeInfo;
import org.carbondata.core.util.MolapCoreLogEvent;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.writer.exception.MolapDataWriterException;

public class MolapDataWriter {
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapDataWriter.class.getName());
    /**
     * table name
     */
    private String tableName;
    /**
     * data file size;
     */
    private long fileSizeInBytes;
    /**
     * measure count
     */
    private int measureCount;
    /**
     * this will be used for holding leaf node metadata
     */
    private List<LeafNodeInfo> leafNodeInfoList;
    /**
     * current size of file
     */
    private long currentFileSize;
    /**
     * leaf metadata size
     */
    private int leafMetaDataSize;
    /**
     * file count will be used to give sequence number to the leaf node file
     */
    private int fileCount;
    /**
     * Leaf node filename format
     */
    private String fileNameFormat;
    /**
     * leaf node file name
     */
    private String fileName;
    /**
     * File manager
     */
    private IFileManagerComposite fileManager;
    /**
     * Store Location
     */
    private String storeLocation;
    /**
     * fileExtension
     */
    private String fileExtension;
    /**
     * isNewFileCreationRequired
     */
    private boolean isNewFileCreationRequired;
    /**
     * isInProgressExtrequired
     */
    private boolean isInProgressExtrequired;
    /**
     * fileDataOutStream
     */
    private DataOutputStream fileDataOutStream;
    /**
     * metadataOffset for maintaining the offset of pagination file.
     */
    private int metadataOffset;

    /**
     * MolapDataWriter constructor to initialize all the instance variables
     * required for wrting the data i to the file
     *
     * @param storeLocation current store location
     * @param measureCount  total number of measures
     * @param mdKeyLength   mdkey length
     * @param tableName     table name
     */
    public MolapDataWriter(String storeLocation, int measureCount, int mdKeyLength,
            String tableName, String fileExtension, boolean isNewFileCreationRequired,
            boolean isInProgressExtrequired) {
        // measure count
        this.measureCount = measureCount;
        // table name
        this.tableName = tableName;

        this.storeLocation = storeLocation;
        this.fileExtension = fileExtension;
        // create the leaf node file format
        this.fileNameFormat =
                storeLocation + File.separator + this.tableName + '_' + "{0}" + this.fileExtension;

        this.leafMetaDataSize = MolapCommonConstants.INT_SIZE_IN_BYTE * (2 + measureCount)
                + MolapCommonConstants.LONG_SIZE_IN_BYTE * (measureCount + 1) + (2 * mdKeyLength);
        this.leafNodeInfoList = new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        // get max file size;
        this.fileSizeInBytes = Long.parseLong(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.MAX_FILE_SIZE,
                        MolapCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL))
                * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR * 1L;
        this.isNewFileCreationRequired = isNewFileCreationRequired;
        this.isInProgressExtrequired = isInProgressExtrequired;
    }

    /**
     * This method will be used to initialize the channel
     *
     * @throws MolapDataWriterException
     */
    public void initChannel() throws MolapDataWriterException {
        // update the filename with new new sequence
        // increment the file sequence counter
        initFileCount();
        if (this.isInProgressExtrequired) {
            this.fileName = MessageFormat.format(this.fileNameFormat, this.fileCount)
                    + MolapCommonConstants.FILE_INPROGRESS_STATUS;
            FileData fileData = new FileData(this.fileName, this.storeLocation);
            fileManager.add(fileData);
        } else {
            this.fileName = MessageFormat.format(this.fileNameFormat, this.fileCount);
        }
        this.fileCount++;
        try {
            // open stream for new leaf node file
            this.fileDataOutStream = FileFactory
                    .getDataOutputStream(this.fileName, FileFactory.getFileType(this.fileName),
                            (short) 1);
        } catch (FileNotFoundException fileNotFoundException) {
            throw new MolapDataWriterException("Problem while getting the writer for Leaf File",
                    fileNotFoundException);
        } catch (IOException e) {
            throw new MolapDataWriterException("Problem while getting the writer for Leaf File", e);
        }
    }

    /**
     * Method will be used to close the open stream
     */
    public MolapFile closeChannle() {
        MolapUtil.closeStreams(this.fileDataOutStream);

        MolapFile molapFile = FileFactory.getMolapFile(fileName, FileFactory.getFileType(fileName));

        if (!molapFile.renameTo(fileName.substring(0, this.fileName.lastIndexOf('.')))) {
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "file renaming failed from _0.querymerged to _0");
        }

        return molapFile;
    }

    private int initFileCount() {
        int fileCnt = 0;
        File[] dataFiles = new File(storeLocation).listFiles(new FileFilter() {

            @Override
            public boolean accept(File file) {
                if (!file.isDirectory() && file.getName().startsWith(tableName) && file.getName()
                        .contains(fileExtension)) {
                    return true;
                }
                return false;
            }
        });
        if (dataFiles != null && dataFiles.length > 0) {
            Arrays.sort(dataFiles);
            String fileName = dataFiles[dataFiles.length - 1].getName();
            try {
                fileCnt = Integer.parseInt(
                        fileName.substring(fileName.lastIndexOf('_') + 1).split("\\.")[0]);
            } catch (NumberFormatException ex) {
                fileCnt = 0;
            }
            fileCnt++;
        }
        return fileCnt;
    }

    /**
     * This method will be used to update the file channel with new file; new
     * file will be created once existing file reached the file size limit This
     * method will first check whether existing file size is exceeded the file
     * size limit if yes then write the leaf metadata to file then set the
     * current file size to 0 close the existing file channel get the new file
     * name and get the channel for new file
     *
     * @throws MolapDataWriterException if any problem
     */
    private void updateLeafNodeFileChannel() throws MolapDataWriterException {
        // get the current file size exceeding the file size threshold
        if (currentFileSize >= fileSizeInBytes) {
            // write meta data to end of the existing file
            writeleafMetaDataToFile();
            // set the current file size;
            this.currentFileSize = 0;
            // close the current open file channel
            MolapUtil.closeStreams(fileDataOutStream);
            // initialize the new channel
            initChannel();
        }
    }

    /**
     * This method will be used to write leaf data to file
     * file format
     * <key><measure1><measure2>....
     *
     * @param keyArray   key array
     * @param dataArray  measure array
     * @param entryCount number of entries
     * @param startKey   start key of leaf
     * @param endKey     end key of leaf
     * @throws MolapDataWriterException
     * @throws MolapDataWriterException throws new MolapDataWriterException if any problem
     */
    public void writeDataToFile(byte[] keyArray, byte[][] dataArray, int entryCount,
            byte[] startKey, byte[] endKey) throws MolapDataWriterException {
        if (this.isNewFileCreationRequired) {
            updateLeafNodeFileChannel();
        }
        // total measure length;
        int totalMsrArraySize = 0;
        // current measure length;
        int currentMsrLenght = 0;
        int[] msrLength = new int[this.measureCount];

        // calculate the total size required for all the measure and get the
        // each measure size
        for (int i = 0; i < dataArray.length; i++) {
            currentMsrLenght = dataArray[i].length;
            totalMsrArraySize += currentMsrLenght;
            msrLength[i] = currentMsrLenght;
        }
        byte[] writableDataArray = new byte[totalMsrArraySize];

        // start position will be used for adding the measure in
        // writableDataArray after adding measure increment the start position
        // by added measure length which will be used for next measure start
        // position
        int startPosition = 0;
        for (int i = 0; i < dataArray.length; i++) {
            System.arraycopy(dataArray[i], 0, writableDataArray, startPosition,
                    dataArray[i].length);
            startPosition += msrLength[i];
        }
        writeDataToFile(keyArray, writableDataArray, msrLength, entryCount, startKey, endKey);
    }

    /**
     * This method will be used to write leaf data to file
     * file format
     * <key><measure1><measure2>....
     *
     * @param keyArray   key array
     * @param dataArray  measure array
     * @param entryCount number of entries
     * @param startKey   start key of leaf
     * @param endKey     end key of leaf
     * @throws MolapDataWriterException
     * @throws MolapDataWriterException throws new MolapDataWriterException if any problem
     */
    public void writeDataToFile(byte[] keyArray, byte[] dataArray, int[] msrLength, int entryCount,
            byte[] startKey, byte[] endKey) throws MolapDataWriterException {
        int keySize = keyArray.length;
        // write data to leaf file and get its offset
        long offset = writeDataToFile(keyArray, dataArray);

        // get the leaf node info for currently added leaf node
        LeafNodeInfo leafNodeInfo =
                getLeafNodeInfo(keySize, msrLength, offset, entryCount, startKey, endKey);
        // add leaf info to list
        this.leafNodeInfoList.add(leafNodeInfo);
        // calculate the current size of the file
        this.currentFileSize +=
                keySize + dataArray.length + (leafNodeInfoList.size() * this.leafMetaDataSize)
                        + MolapCommonConstants.LONG_SIZE_IN_BYTE;
    }

    /**
     * This method will be used to get the leaf node metadata
     *
     * @param keySize    key size
     * @param msrLength  measure length array
     * @param offset     current offset
     * @param entryCount total number of rows in leaf
     * @param startKey   start key of leaf
     * @param endKey     end key of leaf
     * @return LeafNodeInfo - leaf metadata
     */
    private LeafNodeInfo getLeafNodeInfo(int keySize, int[] msrLength, long offset, int entryCount,
            byte[] startKey, byte[] endKey) {
        // create the info object for leaf entry
        LeafNodeInfo info = new LeafNodeInfo();
        // add total entry count
        info.setNumberOfKeys(entryCount);

        // add the key array length
        info.setKeyLength(keySize);

        // add key offset
        info.setKeyOffset(offset);

        // increment the current offset by adding key length to get the measure
        // offset position
        // format of metadata will be
        // <entrycount>,<keylenght>,<keyoffset>,<msr1lenght><msr1offset><msr2length><msr2offset>
        offset += keySize;

        // add measure length
        info.setMeasureLength(msrLength);

        long[] msrOffset = new long[this.measureCount];

        for (int i = 0; i < this.measureCount; i++) {
            msrOffset[i] = offset;
            // now increment the offset by adding measure length to get the next
            // measure offset;
            offset += msrLength[i];
        }
        // add measure offset
        info.setMeasureOffset(msrOffset);
        // set startkey
        info.setStartKey(startKey);
        // set end key
        info.setEndKey(endKey);
        // return leaf metadata
        return info;
    }

    /**
     * This method is responsible for writing leaf node to the leaf node file
     *
     * @param keyArray     mdkey array
     * @param measureArray measure array
     * @return file offset offset is the current position of the file
     * @throws MolapDataWriterException if will throw MolapDataWriterException when any thing goes wrong
     *                                  while while writing the leaf file
     */
    private long writeDataToFile(byte[] keyArray, byte[] measureArray)
            throws MolapDataWriterException {
        long offset = metadataOffset;
        try {
            metadataOffset += keyArray.length + measureArray.length;
            this.fileDataOutStream.write(keyArray);
            this.fileDataOutStream.write(measureArray);
        } catch (IOException exception) {
            throw new MolapDataWriterException("Problem in writing Leaf Node File: ", exception);
        }
        // return the offset, this offset will be used while reading the file in
        // engine side to get from which position to start reading the file
        return offset;
    }

    /**
     * This method will write metadata at the end of file file format
     * <KeyArray><measure1><measure2> <KeyArray><measure1><measure2>
     * <KeyArray><measure1><measure2> <KeyArray><measure1><measure2>
     * <entrycount>
     * <keylength><keyoffset><measure1length><measure1offset><measure2length
     * ><measure2offset>
     *
     * @throws MolapDataWriterException throw MolapDataWriterException when problem in writing the meta data
     *                                  to file
     */
    public void writeleafMetaDataToFile() throws MolapDataWriterException {
        ByteBuffer buffer = null;
        int[] msrLength = null;
        long[] msroffset = null;
        try {
            // get the current position of the file, this will be used for
            // reading the file meta data, meta data start position in file will
            // be this position
            for (LeafNodeInfo info : this.leafNodeInfoList) {
                // get the measure length array
                msrLength = info.getMeasureLength();
                // get the measure offset array
                msroffset = info.getMeasureOffset();
                // allocate total size for buffer
                buffer = ByteBuffer.allocate(this.leafMetaDataSize);
                // add entry count
                buffer.putInt(info.getNumberOfKeys());
                // add key length
                buffer.putInt(info.getKeyLength());
                // add key offset
                buffer.putLong(info.getKeyOffset());
                // set the start key
                buffer.put(info.getStartKey());
                // set the end key
                buffer.put(info.getEndKey());
                // add each measure length and its offset
                for (int i = 0; i < this.measureCount; i++) {
                    buffer.putInt(msrLength[i]);
                    buffer.putLong(msroffset[i]);
                }
                // flip the buffer
                buffer.flip();
                // write metadat to file
                this.fileDataOutStream.write(buffer.array());
            }
            // create new for adding the offset of meta data
            // write offset to file
            this.fileDataOutStream.writeLong(metadataOffset);
        } catch (IOException exception) {
            throw new MolapDataWriterException("Problem while writing the Leaf Node File: ",
                    exception);
        }
        // create new leaf node info list for new file
        this.leafNodeInfoList = new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    }

    /**
     * This method will be used to get the leaf meta list size
     *
     * @return list size
     */
    public int getMetaListSize() {
        return leafNodeInfoList.size();
    }

    public void setFileManager(IFileManagerComposite fileManager) {
        this.fileManager = fileManager;
    }

    /**
     * getFileCount
     *
     * @return int
     */
    public int getFileCount() {
        return fileCount;
    }

    /**
     * setFileCount
     *
     * @param fileCount void
     */
    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }
}
