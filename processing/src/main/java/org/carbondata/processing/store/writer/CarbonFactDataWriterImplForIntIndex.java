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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

public class CarbonFactDataWriterImplForIntIndex extends AbstractFactDataWriter<int[]> {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(AbstractFactDataWriter.class.getName());
    private NumberCompressor numberCompressor;

    public CarbonFactDataWriterImplForIntIndex(String storeLocation, int measureCount,
            int mdKeyLength, String tableName, boolean isNodeHolder,
            IFileManagerComposite fileManager, int[] keyBlockSize, boolean isUpdateFact) {
        super(storeLocation, measureCount, mdKeyLength, tableName, isNodeHolder, fileManager,
                keyBlockSize, isUpdateFact);

        this.numberCompressor = new NumberCompressor(Integer.parseInt(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.LEAFNODE_SIZE,
                        CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL)));

        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "********************************Integer based will be used");
    }

    @Override
    public void writeDataToFile(IndexStorage<int[]>[] keyStorageArray, byte[][] dataArray,
            int entryCount, byte[] startKey, byte[] endKey, ValueCompressionModel compressionModel)
            throws CarbonDataWriterException {
        updateLeafNodeFileChannel();

        // current measure length;
        int currentMsrLenght = 0;
        // total measure length;
        int totalMsrArraySize = 0;

        boolean[] isSortedData = new boolean[keyStorageArray.length];
        int[] keyLengths = new int[keyStorageArray.length];

        int totalKeySize = 0;
        int keyBlkSize = 0;

        byte[][] keyBlockData = fillAndCompressedKeyBlockData(keyStorageArray, entryCount);

        for (int i = 0; i < keyLengths.length; i++) {
            keyLengths[i] = keyBlockData[i].length;
            isSortedData[i] = keyStorageArray[i].isAlreadySorted();
            if (!isSortedData[i]) {
                keyBlkSize++;

            }
            totalKeySize += keyLengths[i];
        }
        int[] keyBlockIndexLengths = new int[keyBlkSize];
        byte[][] dataAfterCompression = new byte[keyBlkSize][];
        byte[][] indexMap = new byte[keyBlkSize][];
        int index = 0;
        for (int i = 0; i < isSortedData.length; i++) {
            if (!isSortedData[i]) {
                dataAfterCompression[index] =
                        numberCompressor.compress(keyStorageArray[i].getDataAfterComp());
                if (null != keyStorageArray[i].getIndexMap()
                        && keyStorageArray[i].getIndexMap().length > 0) {
                    indexMap[index] = numberCompressor.compress(keyStorageArray[i].getIndexMap());
                } else {
                    indexMap[index] = new byte[0];
                }
                keyBlockIndexLengths[index] =
                        (dataAfterCompression[index].length + indexMap[index].length)
                                + CarbonCommonConstants.INT_SIZE_IN_BYTE;
                index++;
            }
        }
        byte[] writableKeyArry = new byte[totalKeySize];
        int startPos = 0;
        for (int i = 0; i < keyLengths.length; i++) {
            System.arraycopy(keyBlockData[i], 0, writableKeyArry, startPos, keyBlockData[i].length);
            startPos += keyLengths[i];
        }
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
        startPos = 0;
        for (int i = 0; i < dataArray.length; i++) {
            System.arraycopy(dataArray[i], 0, writableDataArray, startPos, dataArray[i].length);
            startPos += msrLength[i];
        }
        // current file size;
        this.currentFileSize += writableKeyArry.length + writableDataArray.length;

        NodeHolder holder = new NodeHolder();
        holder.setDataArray(writableDataArray);
        holder.setKeyArray(writableKeyArry);
        holder.setEndKey(endKey);
        holder.setMeasureLenght(msrLength);
        holder.setStartKey(startKey);
        holder.setEntryCount(entryCount);
        holder.setKeyLengths(keyLengths);
        holder.setKeyBlockIndexLength(keyBlockIndexLengths);
        holder.setIsSortedKeyBlock(isSortedData);
        holder.setCompressedIndex(dataAfterCompression);
        holder.setCompressedIndexMap(indexMap);
        holder.setCompressionModel(compressionModel);
        if (!this.isNodeHolderRequired) {
            writeDataToFile(holder);
        } else {
            nodeHolderList.add(holder);
        }
    }

    private byte[][] fillAndCompressedKeyBlockData(IndexStorage<int[]>[] keyStorageArray,
            int entryCount) {
        byte[][] keyBlockDataArray = new byte[keyStorageArray.length][];
        int destPos = 0;
        for (int i = 0; i < keyStorageArray.length; i++) {
            keyBlockDataArray[i] = new byte[entryCount * keyBlockSize[i]];
            destPos = 0;
            for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
                System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockDataArray[i],
                        destPos, keyBlockSize[i]);
                destPos += keyBlockSize[i];
            }
            keyBlockDataArray[i] = SnappyByteCompression.INSTANCE.compress(keyBlockDataArray[i]);
        }
        return keyBlockDataArray;
    }

    //TODO SIMIAN

    /**
     * This method is responsible for writing leaf node to the leaf node file
     *
     * @return file offset offset is the current position of the file
     * @throws CarbonDataWriterException if will throw CarbonDataWriterException when any thing goes wrong
     *                                  while while writing the leaf file
     */
    protected long writeDataToFile(NodeHolder nodeHolder, FileChannel channel)
            throws CarbonDataWriterException {
        // create byte buffer
        byte[][] compressedIndex = nodeHolder.getCompressedIndex();
        byte[][] compressedIndexMap = nodeHolder.getCompressedIndexMap();
        int indexBlockSize = 0;
        int index = 0;
        for (int i = 0; i < nodeHolder.getKeyBlockIndexLength().length; i++) {
            indexBlockSize += nodeHolder.getKeyBlockIndexLength()[index++]
                    + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(
                nodeHolder.getKeyArray().length + nodeHolder.getDataArray().length
                        + indexBlockSize);
        long offsetVal = 0;
        try {
            // get the current offset
            offsetVal = channel.size();
            // add key array to byte buffer
            byteBuffer.put(nodeHolder.getKeyArray());
            // add measure data array to byte buffer
            byteBuffer.put(nodeHolder.getDataArray());

            ByteBuffer buffer1 = null;
            for (int j = 0; j < compressedIndex.length; j++) {
                buffer1 = ByteBuffer.allocate(nodeHolder.getKeyBlockIndexLength()[j]);
                buffer1.putInt(compressedIndex[j].length);
                buffer1.put(compressedIndex[j]);
                if (compressedIndexMap[j].length > 0) {
                    buffer1.put(compressedIndexMap[j]);
                }
                buffer1.rewind();
                byteBuffer.put(buffer1.array());

            }
            byteBuffer.flip();
            // write data to file
            channel.write(byteBuffer);
        } catch (IOException exception) {
            throw new CarbonDataWriterException("Problem in writing Leaf Node File: ", exception);
        }
        // return the offset, this offset will be used while reading the file in
        // engine side to get from which position to start reading the file
        return offsetVal;
    }

}
