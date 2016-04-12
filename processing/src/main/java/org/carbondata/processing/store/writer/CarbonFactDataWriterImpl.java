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

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;

public class CarbonFactDataWriterImpl extends AbstractFactDataWriter<short[]> {

    public CarbonFactDataWriterImpl(String storeLocation, int measureCount, int mdKeyLength,
            String tableName, boolean isNodeHolder, IFileManagerComposite fileManager,
            int[] keyBlockSize, boolean isUpdateFact) {
        super(storeLocation, measureCount, mdKeyLength, tableName, isNodeHolder, fileManager,
                keyBlockSize, isUpdateFact);
    }

    @Override
    public void writeDataToFile(IndexStorage<short[]>[] keyStorageArray, byte[][] dataArray,
            int entryCount, byte[] startKey, byte[] endKey, ValueCompressionModel compressionModel)
            throws CarbonDataWriterException {
        updateLeafNodeFileChannel();
        // total measure length;
        int totalMsrArraySize = 0;
        // current measure length;
        int currentMsrLength = 0;

        boolean[] isSortedData = new boolean[keyStorageArray.length];
        int[] keyLengths = new int[keyStorageArray.length];

        int keyBlockSize = 0;
        int totalKeySize = 0;

        byte[][] keyBlockData = fillAndCompressedKeyBlockData(keyStorageArray, entryCount);

        for (int i = 0; i < keyLengths.length; i++) {
            keyLengths[i] = keyBlockData[i].length;
            isSortedData[i] = keyStorageArray[i].isAlreadySorted();
            if (!isSortedData[i]) {
                keyBlockSize++;

            }
            totalKeySize += keyLengths[i];
        }
        int[] keyBlockIndexLengths = new int[keyBlockSize];
        short[][] dataAfterCompression = new short[keyBlockSize][];
        short[][] indexMap = new short[keyBlockSize][];
        int index = 0;
        for (int i = 0; i < isSortedData.length; i++) {
            if (!isSortedData[i]) {
                dataAfterCompression[index] = keyStorageArray[i].getDataAfterComp();
                indexMap[index] = keyStorageArray[i].getIndexMap();
                // here total size of compressed offsets and index map * 2  for storing short+ length of compressed offsets
                keyBlockIndexLengths[index] = (dataAfterCompression[index].length + (
                        null != indexMap[index] && indexMap[index].length > 0 ?
                                indexMap[index].length :
                                0)) * 2 + CarbonCommonConstants.INT_SIZE_IN_BYTE;
                index++;
            }
        }
        byte[] writableKeyArray = new byte[totalKeySize];
        int startPos = 0;
        for (int i = 0; i < keyLengths.length; i++) {
            System.arraycopy(keyBlockData[i], 0, writableKeyArray, startPos,
                    keyBlockData[i].length);
            startPos += keyLengths[i];
        }
        int[] msrLength = new int[this.measureCount];
        // calculate the total size required for all the measure and get the
        // each measure size
        for (int i = 0; i < dataArray.length; i++) {
            currentMsrLength = dataArray[i].length;
            totalMsrArraySize += currentMsrLength;
            msrLength[i] = currentMsrLength;
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
        this.currentFileSize += writableKeyArray.length + writableDataArray.length;

        NodeHolder nodeHolderObj = new NodeHolder();
        nodeHolderObj.setDataArray(writableDataArray);
        nodeHolderObj.setKeyArray(writableKeyArray);
        nodeHolderObj.setEndKey(endKey);
        nodeHolderObj.setMeasureLenght(msrLength);
        nodeHolderObj.setStartKey(startKey);
        nodeHolderObj.setEntryCount(entryCount);
        nodeHolderObj.setKeyLengths(keyLengths);
        nodeHolderObj.setDataAfterCompression(dataAfterCompression);
        nodeHolderObj.setIndexMap(indexMap);
        nodeHolderObj.setKeyBlockIndexLength(keyBlockIndexLengths);
        nodeHolderObj.setIsSortedKeyBlock(isSortedData);
        nodeHolderObj.setCompressionModel(compressionModel);
        if (!this.isNodeHolderRequired) {
            writeDataToFile(nodeHolderObj);
        } else {
            nodeHolderList.add(nodeHolderObj);
        }
    }

    private byte[][] fillAndCompressedKeyBlockData(IndexStorage<short[]>[] keyStorageArray,
            int entryCount) {
        byte[][] keyBlockData = new byte[keyStorageArray.length][];
        int destPos = 0;
        for (int i = 0; i < keyStorageArray.length; i++) {
            keyBlockData[i] = new byte[entryCount * keyBlockSize[i]];
            destPos = 0;
            for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
                System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos,
                        keyBlockSize[i]);
                destPos += keyBlockSize[i];
            }
            keyBlockData[i] = SnappyByteCompression.INSTANCE.compress(keyBlockData[i]);
        }
        return keyBlockData;
    }

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

        short[][] dataAfterCompression = nodeHolder.getDataAfterCompression();
        short[][] indexMap = nodeHolder.getIndexMap();
        int indexBlockSize = 0;
        int index = 0;
        for (int i = 0; i < nodeHolder.getKeyBlockIndexLength().length; i++) {
            indexBlockSize += nodeHolder.getKeyBlockIndexLength()[index++]
                    + CarbonCommonConstants.INT_SIZE_IN_BYTE;
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
            for (int i = 0; i < dataAfterCompression.length; i++) {
                buffer1 = ByteBuffer.allocate(nodeHolder.getKeyBlockIndexLength()[i]);
                buffer1.putInt(dataAfterCompression[i].length * 2);
                for (int j = 0; j < dataAfterCompression[i].length; j++) {
                    buffer1.putShort(dataAfterCompression[i][j]);
                }
                if (null != indexMap[i] && indexMap.length > 0) {
                    for (int j = 0; j < indexMap[i].length; j++) {
                        buffer1.putShort(indexMap[i][j]);
                    }
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
        return offset;
    }

}
