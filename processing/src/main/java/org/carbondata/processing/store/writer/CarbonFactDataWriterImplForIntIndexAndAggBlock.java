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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.util.CarbonMetadataUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.writer.CarbonMetaDataWriter;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

public class CarbonFactDataWriterImplForIntIndexAndAggBlock extends AbstractFactDataWriter<int[]> {
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(CarbonFactDataWriterImplForIntIndexAndAggBlock.class.getName());
    protected boolean[] aggBlocks;
    private NumberCompressor numberCompressor;
    private boolean[] isComplexType;
    private int NoDictionaryCount;

    public CarbonFactDataWriterImplForIntIndexAndAggBlock(String storeLocation, int measureCount,
            int mdKeyLength, String tableName, boolean isNodeHolder,
            IFileManagerComposite fileManager, int[] keyBlockSize, boolean[] aggBlocks,
            boolean isUpdateFact, boolean[] isComplexType, int NoDictionaryCount) {
        this(storeLocation, measureCount, mdKeyLength, tableName, isNodeHolder, fileManager,
                keyBlockSize, aggBlocks, isUpdateFact);
        this.isComplexType = isComplexType;
        this.NoDictionaryCount = NoDictionaryCount;
    }

    public CarbonFactDataWriterImplForIntIndexAndAggBlock(String storeLocation, int measureCount,
            int mdKeyLength, String tableName, boolean isNodeHolder,
            IFileManagerComposite fileManager, int[] keyBlockSize, boolean[] aggBlocks,
            boolean isUpdateFact) {
        super(storeLocation, measureCount, mdKeyLength, tableName, isNodeHolder, fileManager,
                keyBlockSize, isUpdateFact);
        this.aggBlocks = aggBlocks;
        this.numberCompressor = new NumberCompressor(Integer.parseInt(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.LEAFNODE_SIZE,
                        CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL)));
    }

    @Override
    public void writeDataToFile(IndexStorage<int[]>[] keyStorageArray, byte[][] dataArray,
            int entryCount, byte[] startKey, byte[] endKey, ValueCompressionModel compressionModel)
            throws CarbonDataWriterException {
        updateLeafNodeFileChannel();
        // total measure length;
        int totalMsrArrySize = 0;
        // current measure length;
        int currentMsrLenght = 0;
        int totalKeySize = 0;
        int keyBlockSize = 0;

        boolean[] isSortedData = new boolean[keyStorageArray.length];
        int[] keyLengths = new int[keyStorageArray.length];

        //below will calculate min and max value for each column
        //for below 2d array, first index will be for column and second will be min max
        // value for same column
        byte[][] columnMinMaxData = new byte[keyStorageArray.length][];

        byte[][] keyBlockData = fillAndCompressedKeyBlockData(keyStorageArray, entryCount);

        for (int i = 0; i < keyLengths.length; i++) {
            keyLengths[i] = keyBlockData[i].length;
            isSortedData[i] = keyStorageArray[i].isAlreadySorted();
            if (!isSortedData[i]) {
                keyBlockSize++;

            }
            totalKeySize += keyLengths[i];

            if (isNoDictionary[i]) {
                columnMinMaxData[i] =
                        new byte[keyStorageArray[i].getKeyBlock()[0].length + keyStorageArray[i]
                                .getKeyBlock()[keyStorageArray[i].getKeyBlock().length - 1].length];

                byte[] minVal = keyStorageArray[i].getKeyBlock()[0];
                byte[] maxVal =
                        keyStorageArray[i].getKeyBlock()[keyStorageArray[i].getKeyBlock().length
                                - 1];
                System.arraycopy(minVal, 0, columnMinMaxData[i], 0, minVal.length);
                System.arraycopy(maxVal, 0, columnMinMaxData[i], minVal.length, maxVal.length);
            } else {
                //for column min max value
                columnMinMaxData[i] = new byte[this.keyBlockSize[i] * 2];
                byte[] minVal = keyStorageArray[i].getKeyBlock()[0];
                byte[] maxVal =
                        keyStorageArray[i].getKeyBlock()[keyStorageArray[i].getKeyBlock().length
                                - 1];
                System.arraycopy(minVal, 0, columnMinMaxData[i], 0, this.keyBlockSize[i]);
                System.arraycopy(maxVal, 0, columnMinMaxData[i], this.keyBlockSize[i],
                        this.keyBlockSize[i]);
            }
        }
        int[] keyBlockIdxLengths = new int[keyBlockSize];
        byte[][] dataAfterCompression = new byte[keyBlockSize][];
        byte[][] indexMap = new byte[keyBlockSize][];
        int idx = 0;
        for (int i = 0; i < isSortedData.length; i++) {
            if (!isSortedData[i]) {
                dataAfterCompression[idx] =
                        numberCompressor.compress(keyStorageArray[i].getDataAfterComp());
                if (null != keyStorageArray[i].getIndexMap()
                        && keyStorageArray[i].getIndexMap().length > 0) {
                    indexMap[idx] = numberCompressor.compress(keyStorageArray[i].getIndexMap());
                } else {
                    indexMap[idx] = new byte[0];
                }
                keyBlockIdxLengths[idx] = (dataAfterCompression[idx].length + indexMap[idx].length)
                        + CarbonCommonConstants.INT_SIZE_IN_BYTE;
                idx++;
            }
        }
        int compressDataBlockSize = 0;
        for (int i = 0; i < aggBlocks.length; i++) {
            if (aggBlocks[i]) {
                compressDataBlockSize++;
            }
        }
        byte[][] compressedDataIndex = new byte[compressDataBlockSize][];
        int[] dataIndexMapLength = new int[compressDataBlockSize];
        idx = 0;
        for (int i = 0; i < aggBlocks.length; i++) {
            if (aggBlocks[i]) {
                try {
                    compressedDataIndex[idx] =
                            numberCompressor.compress(keyStorageArray[i].getDataIndexMap());
                    dataIndexMapLength[idx] = compressedDataIndex[idx].length;
                    idx++;
                } catch (Exception e) {
                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
                }
            }
        }

        byte[] writableKeyArray = new byte[totalKeySize];
        int startPosition = 0;
        for (int i = 0; i < keyLengths.length; i++) {
            System.arraycopy(keyBlockData[i], 0, writableKeyArray, startPosition,
                    keyBlockData[i].length);
            startPosition += keyLengths[i];
        }
        int[] msrLength = new int[this.measureCount];
        // calculate the total size required for all the measure and get the
        // each measure size
        for (int i = 0; i < dataArray.length; i++) {
            currentMsrLenght = dataArray[i].length;
            totalMsrArrySize += currentMsrLenght;
            msrLength[i] = currentMsrLenght;
        }
        byte[] writableDataArray = new byte[totalMsrArrySize];

        // start position will be used for adding the measure in
        // writableDataArray after adding measure increment the start position
        // by added measure length which will be used for next measure start
        // position
        startPosition = 0;
        for (int i = 0; i < dataArray.length; i++) {
            System.arraycopy(dataArray[i], 0, writableDataArray, startPosition,
                    dataArray[i].length);
            startPosition += msrLength[i];
        }
        // current file size;
        this.currentFileSize += writableKeyArray.length + writableDataArray.length;

        NodeHolder holder = new NodeHolder();
        holder.setDataArray(writableDataArray);
        holder.setKeyArray(writableKeyArray);
        holder.setEndKey(endKey);
        holder.setMeasureLenght(msrLength);
        holder.setStartKey(startKey);
        holder.setEntryCount(entryCount);
        holder.setKeyLengths(keyLengths);
        holder.setKeyBlockIndexLength(keyBlockIdxLengths);
        holder.setIsSortedKeyBlock(isSortedData);
        holder.setCompressedIndex(dataAfterCompression);
        holder.setCompressedIndexMap(indexMap);
        holder.setDataIndexMapLength(dataIndexMapLength);
        holder.setCompressedDataIndex(compressedDataIndex);
        holder.setCompressionModel(compressionModel);
        //setting column min max value
        holder.setColumnMinMaxData(columnMinMaxData);
        holder.setAggBlocks(aggBlocks);
        if (!this.isNodeHolderRequired) {
            writeDataToFile(holder);
        } else {
            nodeHolderList.add(holder);
        }
    }

    protected byte[][] fillAndCompressedKeyBlockData(IndexStorage<int[]>[] keyStorageArray,
            int entryCount) {
        byte[][] keyBlockData = new byte[keyStorageArray.length][];
        int destPos = 0;
        for (int i = 0; i < keyStorageArray.length; i++) {
            destPos = 0;
            //handling for high card dims
            if (i >= keyBlockSize.length && !isComplexType[i]) {
                int totalLength = 0;
                // calc size of the total bytes in all the colmns.
                for (int k = 0; k < keyStorageArray[i].getKeyBlock().length; k++) {
                    byte[] colValue = keyStorageArray[i].getKeyBlock()[k];
                    totalLength += colValue.length;
                }
                keyBlockData[i] = new byte[totalLength];

                for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
                    int length = keyStorageArray[i].getKeyBlock()[j].length;
                    System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i],
                            destPos, length);
                    destPos += length;
                }
            } else {
                if (aggBlocks[i]) {
                    keyBlockData[i] = new byte[keyStorageArray[i].getTotalSize()];
                    for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
                        System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i],
                                destPos, keyStorageArray[i].getKeyBlock()[j].length);
                        destPos += keyStorageArray[i].getKeyBlock()[j].length;
                    }
                } else {
                    if (isComplexType[i]) {
                        keyBlockData[i] =
                                new byte[keyStorageArray[i].getKeyBlock().length * keyBlockSize[i]];
                    } else {
                        keyBlockData[i] = new byte[entryCount * keyBlockSize[i]];
                    }
                    for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
                        System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i],
                                destPos, keyBlockSize[i]);
                        destPos += keyBlockSize[i];
                    }
                }
            }
            keyBlockData[i] = SnappyByteCompression.INSTANCE.compress(keyBlockData[i]);
        }
        return keyBlockData;
    }

    /**
     * This method is responsible for writing leaf node to the leaf node file
     *
     * @return file offset offset is the current position of the file
     * @throws CarbonDataWriterException if will throw CarbonDataWriterException when any thing
     * goes wrong while while writing the leaf file
     */
    protected long writeDataToFile(NodeHolder nodeHolder, FileChannel channel)
            throws CarbonDataWriterException {
        // create byte buffer
        byte[][] compressedIndex = nodeHolder.getCompressedIndex();
        byte[][] compressedIndexMap = nodeHolder.getCompressedIndexMap();
        byte[][] compressedDataIndex = nodeHolder.getCompressedDataIndex();
        int indexBlockSize = 0;
        int index = 0;
        for (int i = 0; i < nodeHolder.getKeyBlockIndexLength().length; i++) {
            indexBlockSize += nodeHolder.getKeyBlockIndexLength()[index++]
                    + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        }

        for (int i = 0; i < nodeHolder.getDataIndexMapLength().length; i++) {
            indexBlockSize += nodeHolder.getDataIndexMapLength()[i];
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(
                nodeHolder.getKeyArray().length + nodeHolder.getDataArray().length
                        + indexBlockSize);
        long offset = 0;
        try {
            // get the current offset
            offset = channel.size();
            // add key array to byte buffer
            byteBuffer.put(nodeHolder.getKeyArray());
            // add measure data array to byte buffer
            byteBuffer.put(nodeHolder.getDataArray());

            ByteBuffer buffer1 = null;
            for (int i = 0; i < compressedIndex.length; i++) {
                buffer1 = ByteBuffer.allocate(nodeHolder.getKeyBlockIndexLength()[i]);
                buffer1.putInt(compressedIndex[i].length);
                buffer1.put(compressedIndex[i]);
                if (compressedIndexMap[i].length > 0) {
                    buffer1.put(compressedIndexMap[i]);
                }
                buffer1.rewind();
                byteBuffer.put(buffer1.array());

            }
            for (int i = 0; i < compressedDataIndex.length; i++) {
                byteBuffer.put(compressedDataIndex[i]);
            }
            byteBuffer.flip();
            // write data to file
            channel.write(byteBuffer);
        } catch (IOException exception) {
            throw new CarbonDataWriterException("Problem in writing Leaf Node File: ", exception);
        }
        // return the offset, this offset will be used while reading the file in
        // engine side to get from which position to start reading the file
        return offset;
    }

    /**
     * This method will be used to get the leaf node metadata
     *
     * @return LeafNodeInfo - leaf metadata
     */
    protected LeafNodeInfoColumnar getLeafNodeInfo(NodeHolder nodeHolder, long offset) {
        // create the info object for leaf entry
        LeafNodeInfoColumnar info = new LeafNodeInfoColumnar();
        //add aggBlocks array
        info.setAggKeyBlock(nodeHolder.getAggBlocks());
        // add total entry count
        info.setNumberOfKeys(nodeHolder.getEntryCount());

        // add the key array length
        info.setKeyLengths(nodeHolder.getKeyLengths());

        //add column min max length
        info.setColumnMinMaxData(nodeHolder.getColumnMinMaxData());

        long[] keyOffSets = new long[nodeHolder.getKeyLengths().length];

        for (int i = 0; i < keyOffSets.length; i++) {
            keyOffSets[i] = offset;
            offset += nodeHolder.getKeyLengths()[i];
        }
        // key offset will be 8 bytes from current offset because first 4 bytes
        // will be for number of entry in leaf, then next 4 bytes will be for
        // key lenght;
        //        offset += CarbonCommonConstants.INT_SIZE_IN_BYTE * 2;

        // add key offset
        info.setKeyOffSets(keyOffSets);

        // add measure length
        info.setMeasureLength(nodeHolder.getMeasureLenght());

        long[] msrOffset = new long[this.measureCount];

        for (int i = 0; i < this.measureCount; i++) {
            // increment the current offset by 4 bytes because 4 bytes will be
            // used for measure byte length
            //            offset += CarbonCommonConstants.INT_SIZE_IN_BYTE;
            msrOffset[i] = offset;
            // now increment the offset by adding measure length to get the next
            // measure offset;
            offset += nodeHolder.getMeasureLenght()[i];
        }
        // add measure offset
        info.setMeasureOffset(msrOffset);
        info.setIsSortedKeyColumn(nodeHolder.getIsSortedKeyBlock());
        info.setKeyBlockIndexLength(nodeHolder.getKeyBlockIndexLength());
        long[] keyBlockIndexOffsets = new long[nodeHolder.getKeyBlockIndexLength().length];
        for (int i = 0; i < keyBlockIndexOffsets.length; i++) {
            keyBlockIndexOffsets[i] = offset;
            offset += nodeHolder.getKeyBlockIndexLength()[i];
        }
        info.setDataIndexMapLength(nodeHolder.getDataIndexMapLength());
        long[] dataIndexMapOffsets = new long[nodeHolder.getDataIndexMapLength().length];
        for (int i = 0; i < dataIndexMapOffsets.length; i++) {
            dataIndexMapOffsets[i] = offset;
            offset += nodeHolder.getDataIndexMapLength()[i];
        }
        info.setDataIndexMapOffsets(dataIndexMapOffsets);
        info.setKeyBlockIndexOffSets(keyBlockIndexOffsets);
        // set startkey
        info.setStartKey(nodeHolder.getStartKey());
        // set end key
        info.setEndKey(nodeHolder.getEndKey());
        info.setLeafNodeMetaSize(calculateAndSetLeafNodeMetaSize(nodeHolder));
        info.setCompressionModel(nodeHolder.getCompressionModel());
        // return leaf metadata
        return info;
    }

    protected int calculateAndSetLeafNodeMetaSize(NodeHolder nodeHolderInfo) {
        int metaSize = 0;
        //measure offset and measure length
        metaSize += (measureCount * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (measureCount
                * CarbonCommonConstants.LONG_SIZE_IN_BYTE);
        // start and end key
        metaSize += mdkeySize * 2;

        // keyblock length + key offsets + number of tuples+ number of columnar block
        metaSize +=
                (nodeHolderInfo.getKeyLengths().length * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (
                        nodeHolderInfo.getKeyLengths().length
                                * CarbonCommonConstants.LONG_SIZE_IN_BYTE)
                        + CarbonCommonConstants.INT_SIZE_IN_BYTE
                        + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        // if sorted or not
        metaSize += nodeHolderInfo.getIsSortedKeyBlock().length;

        //column min max size
        //for length of columnMinMax byte array
        metaSize += CarbonCommonConstants.INT_SIZE_IN_BYTE;
        for (int i = 0; i < nodeHolderInfo.getColumnMinMaxData().length; i++) {
            //length of sub byte array
            metaSize += CarbonCommonConstants.INT_SIZE_IN_BYTE;
            metaSize += nodeHolderInfo.getColumnMinMaxData()[i].length;
        }

        // key block index length + key block index offset + number of key block
        metaSize += (nodeHolderInfo.getKeyBlockIndexLength().length
                * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (
                nodeHolderInfo.getKeyBlockIndexLength().length
                        * CarbonCommonConstants.LONG_SIZE_IN_BYTE)
                + CarbonCommonConstants.INT_SIZE_IN_BYTE;

        // aggregate block length + agg block offsets + number of block aggergated
        metaSize += (nodeHolderInfo.getDataIndexMapLength().length
                * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (
                nodeHolderInfo.getDataIndexMapLength().length
                        * CarbonCommonConstants.LONG_SIZE_IN_BYTE)
                + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        return metaSize;
    }

    //TODO SIMIAN

    /**
     * This method will write metadata at the end of file file format in thrift format
     *
     * @throws CarbonDataWriterException throw CarbonDataWriterException when problem in writing
     * the meta data to file
     */
    protected void writeleafMetaDataToFile(List<LeafNodeInfoColumnar> infoList, FileChannel channel)
            throws CarbonDataWriterException {
        try {
            long currentPos = channel.size();
            CarbonMetaDataWriter writer = new CarbonMetaDataWriter(this.fileName);
            writer.writeMetaData(
                    CarbonMetadataUtil
                            .convertFileMeta(infoList, localCardinality.length, localCardinality),
                    currentPos);
        } catch (IOException e) {
            throw new CarbonDataWriterException("Problem while writing the Leaf Node File: ", e);
        }
    }
}