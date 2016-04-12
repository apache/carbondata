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

package org.carbondata.processing.store.writer;

import java.io.*;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.util.CarbonMergerUtil;
import org.carbondata.core.util.CarbonMetadataUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.writer.CarbonMetaDataWriter;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

public abstract class AbstractFactDataWriter<T> implements CarbonFactDataWriter<T>

{

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(AbstractFactDataWriter.class.getName());
    /**
     * measure count
     */
    protected int measureCount;
    /**
     * current size of file
     */
    protected long currentFileSize;
    /**
     * leaf node file channel
     */
    protected FileChannel fileChannel;
    /**
     * isNodeHolderRequired
     */
    protected boolean isNodeHolderRequired;
    /**
     * Node Holder
     */
    protected List<NodeHolder> nodeHolderList;
    /**
     * this will be used for holding leaf node metadata
     */
    protected List<LeafNodeInfoColumnar> leafNodeInfoList;
    /**
     * keyBlockSize
     */
    protected int[] keyBlockSize;
    protected boolean[] isNoDictionary;
    /**
     * mdkeySize
     */
    protected int mdkeySize;
    /**
     * tabel name
     */
    private String tableName;
    /**
     * data file size;
     */
    private long fileSizeInBytes;
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
    protected String fileName;
    /**
     * File manager
     */
    private IFileManagerComposite fileManager;
    /**
     * Store Location
     */
    private String storeLocation;
    /**
     * executorService
     */
    private ExecutorService executorService;

    /**
     * Local cardinality for the segment
     */
    protected int[] localCardinality;

    public AbstractFactDataWriter(String storeLocation, int measureCount, int mdKeyLength,
            String tableName, boolean isNodeHolder, IFileManagerComposite fileManager,
            int[] keyBlockSize, boolean isUpdateFact) {

        // measure count
        this.measureCount = measureCount;
        // table name
        this.tableName = tableName;

        this.storeLocation = storeLocation;
        // create the leaf node file format
        if (isUpdateFact) {
            this.fileNameFormat =
                    System.getProperty("java.io.tmpdir") + File.separator + this.tableName + '_'
                            + "{0}" + CarbonCommonConstants.FACT_UPDATE_EXTENSION;
        } else {
            this.fileNameFormat = storeLocation + File.separator + this.tableName + '_' + "{0}"
                    + CarbonCommonConstants.FACT_FILE_EXT;
        }

        this.fileName = MessageFormat.format(this.fileNameFormat, this.fileCount);
        this.leafNodeInfoList =
                new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        // get max file size;
        CarbonProperties propInstance = CarbonProperties.getInstance();
        this.fileSizeInBytes = Long.parseLong(propInstance
                .getProperty(CarbonCommonConstants.MAX_FILE_SIZE,
                        CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL))
                * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR * 1L;
        this.isNodeHolderRequired =
                Boolean.valueOf(CarbonCommonConstants.WRITE_ALL_NODE_IN_SINGLE_TIME_DEFAULT_VALUE);
        this.fileManager = fileManager;

        /**
         * keyBlockSize
         */
        this.keyBlockSize = keyBlockSize;
        /**
         *
         */
        this.mdkeySize = mdKeyLength;

        this.isNodeHolderRequired = this.isNodeHolderRequired && isNodeHolder;
        if (this.isNodeHolderRequired) {
            this.nodeHolderList = new CopyOnWriteArrayList<NodeHolder>();

            this.executorService = Executors.newFixedThreadPool(5);
        }

        //TODO: We should delete the levelmetadata file after reading here.
        this.localCardinality = CarbonMergerUtil
                .getCardinalityFromLevelMetadata(storeLocation, tableName);
    }

    /**
     * @param isNoDictionary the isNoDictionary to set
     */
    public void setIsNoDictionary(boolean[] isNoDictionary) {
        this.isNoDictionary = isNoDictionary;
    }

    /**
     * This method will be used to update the file channel with new file; new
     * file will be created once existing file reached the file size limit This
     * method will first check whether existing file size is exceeded the file
     * size limit if yes then write the leaf metadata to file then set the
     * current file size to 0 close the existing file channel get the new file
     * name and get the channel for new file
     *
     * @throws CarbonDataWriterException if any problem
     */
    protected void updateLeafNodeFileChannel() throws CarbonDataWriterException {
        // get the current file size exceeding the file size threshold
        if (currentFileSize >= fileSizeInBytes) {
            // set the current file size to zero
            this.currentFileSize = 0;
            if (this.isNodeHolderRequired) {
                FileChannel channel = fileChannel;
                List<NodeHolder> localNodeHolderList = this.nodeHolderList;
                executorService.submit(new WriterThread(channel, localNodeHolderList));
                this.nodeHolderList = new CopyOnWriteArrayList<NodeHolder>();
                // close the current open file channel
            } else {
                // write meta data to end of the existing file
                writeleafMetaDataToFile(leafNodeInfoList, fileChannel);
                leafNodeInfoList =
                        new ArrayList<LeafNodeInfoColumnar>(
                                CarbonCommonConstants.CONSTANT_SIZE_TEN);
                CarbonUtil.closeStreams(fileChannel);
            }
            // initialize the new channel
            initializeWriter();
        }
    }

    /**
     * This method will be used to initialize the channel
     *
     * @throws CarbonDataWriterException
     */
    public void initializeWriter() throws CarbonDataWriterException {
        // update the filename with new new sequence
        // increment the file sequence counter
        initFileCount();
        this.fileName = MessageFormat.format(this.fileNameFormat, this.fileCount);
        String actualFileNameVal =
                this.tableName + '_' + this.fileCount + CarbonCommonConstants.FACT_FILE_EXT
                        + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
        FileData fileData = new FileData(actualFileNameVal, this.storeLocation);
        fileManager.add(fileData);
        this.fileName = this.fileName + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

        this.fileCount++;
        try {
            // open channle for new leaf node file
            this.fileChannel = new FileOutputStream(this.fileName, true).getChannel();
        } catch (FileNotFoundException fileNotFoundException) {
            throw new CarbonDataWriterException(
                    "Problem while getting the FileChannel for Leaf File", fileNotFoundException);
        }
    }

    private int initFileCount() {
        int fileInitialCount = 0;
        File[] dataFiles = new File(storeLocation).listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathVal) {
                if (!pathVal.isDirectory() && pathVal.getName().startsWith(tableName) && pathVal
                        .getName().contains(CarbonCommonConstants.FACT_FILE_EXT)) {
                    return true;
                }
                return false;
            }
        });
        if (dataFiles != null && dataFiles.length > 0) {
            Arrays.sort(dataFiles);
            String dataFileName = dataFiles[dataFiles.length - 1].getName();
            try {
                fileInitialCount = Integer.parseInt(
                        dataFileName.substring(dataFileName.lastIndexOf('_') + 1).split("\\.")[0]);
            } catch (NumberFormatException ex) {
                fileInitialCount = 0;
            }
            fileInitialCount++;
        }
        return fileInitialCount;
    }

    /**
     * Below method will be used to write data and its meta data to file
     *
     * @param channel
     * @param nodeHolderList
     * @throws CarbonDataWriterException
     */
    private void writeData(FileChannel channel, List<NodeHolder> nodeHolderList)
            throws CarbonDataWriterException {
        List<LeafNodeInfoColumnar> leafMetaInfos =
                new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        for (NodeHolder nodeHolder : nodeHolderList) {
            long offSet = writeDataToFile(nodeHolder, channel);
            leafMetaInfos.add(getLeafNodeInfo(nodeHolder, offSet));
        }
        writeleafMetaDataToFile(leafMetaInfos, channel);
        CarbonUtil.closeStreams(channel);
    }

    /**
     * This method will write metadata at the end of file file format in thrift format
     *
     */
    protected void writeleafMetaDataToFile(List<LeafNodeInfoColumnar> infoList, FileChannel channel)
            throws CarbonDataWriterException {
        try {
            long currentPosition = channel.size();
            CarbonMetaDataWriter writer = new CarbonMetaDataWriter(this.fileName);
            writer.writeMetaData(
                    CarbonMetadataUtil
                            .convertFileMeta(infoList, localCardinality.length, localCardinality),
                            currentPosition);

        } catch (IOException e) {
            throw new CarbonDataWriterException("Problem while writing the Leaf Node File: ",
                    e);
        }

    }

    protected int calculateAndSetLeafNodeMetaSize(NodeHolder nodeHolder) {
        int metaSize = 0;
        //measure offset and measure length
        metaSize += (measureCount * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (measureCount
                * CarbonCommonConstants.LONG_SIZE_IN_BYTE);
        //start and end key
        metaSize += mdkeySize * 2;

        // keyblock length + key offsets + number of tuples+ number of columnar block
        metaSize += (nodeHolder.getKeyLengths().length * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (
                nodeHolder.getKeyLengths().length * CarbonCommonConstants.LONG_SIZE_IN_BYTE)
                + CarbonCommonConstants.INT_SIZE_IN_BYTE + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        //if sorted or not
        metaSize += nodeHolder.getIsSortedKeyBlock().length;

        //column min max size
        //for length of columnMinMax byte array
        metaSize += CarbonCommonConstants.INT_SIZE_IN_BYTE;
        for (int i = 0; i < nodeHolder.getColumnMinMaxData().length; i++) {
            //length of sub byte array
            metaSize += CarbonCommonConstants.INT_SIZE_IN_BYTE;
            metaSize += nodeHolder.getColumnMinMaxData()[i].length;
        }

        // key block index length + key block index offset + number of key block
        metaSize +=
                (nodeHolder.getKeyBlockIndexLength().length *
                        CarbonCommonConstants.INT_SIZE_IN_BYTE)
                        + (nodeHolder.getKeyBlockIndexLength().length
                        * CarbonCommonConstants.LONG_SIZE_IN_BYTE)
                        + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        return metaSize;
    }

    /**
     * This method will be used to get the leaf node metadata
     *
     * @return LeafNodeInfo - leaf metadata
     */
    protected LeafNodeInfoColumnar getLeafNodeInfo(NodeHolder nodeHolder, long offset) {
        // create the info object for leaf entry
        LeafNodeInfoColumnar infoObj = new LeafNodeInfoColumnar();
        // add total entry count
        infoObj.setNumberOfKeys(nodeHolder.getEntryCount());

        // add the key array length
        infoObj.setKeyLengths(nodeHolder.getKeyLengths());
        //add column min max data
        infoObj.setColumnMinMaxData(nodeHolder.getColumnMinMaxData());

        long[] keyOffSets = new long[nodeHolder.getKeyLengths().length];

        for (int i = 0; i < keyOffSets.length; i++) {
            keyOffSets[i] = offset;
            offset += nodeHolder.getKeyLengths()[i];
        }
        // add key offset
        infoObj.setKeyOffSets(keyOffSets);

        // add measure length
        infoObj.setMeasureLength(nodeHolder.getMeasureLenght());

        long[] msrOffset = new long[this.measureCount];

        for (int i = 0; i < this.measureCount; i++) {
            msrOffset[i] = offset;
            // now increment the offset by adding measure length to get the next
            // measure offset;
            offset += nodeHolder.getMeasureLenght()[i];
        }
        // add measure offset
        infoObj.setMeasureOffset(msrOffset);
        infoObj.setIsSortedKeyColumn(nodeHolder.getIsSortedKeyBlock());
        infoObj.setKeyBlockIndexLength(nodeHolder.getKeyBlockIndexLength());
        long[] keyBlockIndexOffsets = new long[nodeHolder.getKeyBlockIndexLength().length];
        for (int i = 0; i < keyBlockIndexOffsets.length; i++) {
            keyBlockIndexOffsets[i] = offset;
            offset += nodeHolder.getKeyBlockIndexLength()[i];
        }
        infoObj.setKeyBlockIndexOffSets(keyBlockIndexOffsets);
        // set startkey
        infoObj.setStartKey(nodeHolder.getStartKey());
        // set end key
        infoObj.setEndKey(nodeHolder.getEndKey());
        infoObj.setLeafNodeMetaSize(calculateAndSetLeafNodeMetaSize(nodeHolder));
        infoObj.setCompressionModel(nodeHolder.getCompressionModel());
        // return leaf metadata
        return infoObj;
    }

    /**
     * Method will be used to close the open file channel
     *
     * @throws CarbonDataWriterException
     */
    public void closeWriter() {
        if (!this.isNodeHolderRequired) {
            CarbonUtil.closeStreams(this.fileChannel);
            // close channel
        } else {
            this.executorService.shutdown();
            try {
                this.executorService.awaitTermination(2, TimeUnit.HOURS);
            } catch (InterruptedException ex) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, ex);
            }
            CarbonUtil.closeStreams(this.fileChannel);
            this.nodeHolderList = null;
        }

        File origFile = new File(this.fileName.substring(0, this.fileName.lastIndexOf('.')));
        File curFile = new File(this.fileName);
        if (!curFile.renameTo(origFile)) {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Problem while renaming the file");
        }
        if (origFile.length() < 1) {
            if (!origFile.delete()) {
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Problem while deleting the empty fact file");
            }
        }
    }

    /**
     * Write leaf meta data to File.
     *
     * @throws CarbonDataWriterException
     */
    public void writeleafMetaDataToFile() throws CarbonDataWriterException {
        if (!isNodeHolderRequired) {
            writeleafMetaDataToFile(this.leafNodeInfoList, fileChannel);
            this.leafNodeInfoList =
                    new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        } else {
            if (this.nodeHolderList.size() > 0) {
                List<NodeHolder> localNodeHodlerList = nodeHolderList;
                writeData(fileChannel, localNodeHodlerList);
                nodeHolderList = new CopyOnWriteArrayList<NodeHolder>();
            }
        }
    }

    /**
     * This method will be used to write leaf data to file
     * file format
     * <key><measure1><measure2>....
     *
     * @throws CarbonDataWriterException
     * @throws CarbonDataWriterException throws new CarbonDataWriterException if any problem
     */
    protected void writeDataToFile(NodeHolder nodeHolder) throws CarbonDataWriterException {
        // write data to leaf file and get its offset
        long offset = writeDataToFile(nodeHolder, fileChannel);

        // get the leaf node info for currently added leaf node
        LeafNodeInfoColumnar leafNodeInfo = getLeafNodeInfo(nodeHolder, offset);
        // add leaf info to list
        leafNodeInfoList.add(leafNodeInfo);
        // calculate the current size of the file
    }

    protected abstract long writeDataToFile(NodeHolder nodeHolder, FileChannel channel)
            throws CarbonDataWriterException;

    @Override
    public int getLeafMetadataSize() {
        return leafNodeInfoList.size();

    }

    @Override
    public String getTempStoreLocation() {

        return this.fileName;
    }

    /**
     * Thread class for writing data to file
     */
    private final class WriterThread implements Callable<Void> {

        private List<NodeHolder> nodeHolderList;

        private FileChannel channel;

        private WriterThread(FileChannel channel, List<NodeHolder> nodeHolderList) {
            this.channel = channel;
            this.nodeHolderList = nodeHolderList;
        }

        @Override
        public Void call() throws Exception {
            writeData(channel, nodeHolderList);
            return null;
        }
    }

}
