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

import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.carbondata.core.metadata.BlockletInfoColumnar;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.store.CarbonDataFileAttributes;
import org.carbondata.processing.store.colgroup.ColGroupBlockStorage;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;

public class CarbonFactDataWriterImplForIntIndexAndAggBlock extends AbstractFactDataWriter<int[]> {

  protected boolean[] aggBlocks;
  private NumberCompressor numberCompressor;
  private boolean[] isComplexType;
  private int numberOfNoDictionaryColumn;
  private boolean[] isDictionaryColumn;

  public CarbonFactDataWriterImplForIntIndexAndAggBlock(String storeLocation, int measureCount,
      int mdKeyLength, String tableName, IFileManagerComposite fileManager, int[] keyBlockSize,
      boolean[] aggBlocks, boolean[] isComplexType, int NoDictionaryCount,
      CarbonDataFileAttributes carbonDataFileAttributes, String databaseName,
      List<ColumnSchema> wrapperColumnSchemaList, int numberOfNoDictionaryColumn,
      boolean[] isDictionaryColumn, String carbonDataDirectoryPath, int[] colCardinality) {
    super(storeLocation, measureCount, mdKeyLength, tableName, fileManager, keyBlockSize,
        carbonDataFileAttributes, wrapperColumnSchemaList, carbonDataDirectoryPath, colCardinality);
    this.isComplexType = isComplexType;
    this.databaseName = databaseName;
    this.numberOfNoDictionaryColumn = numberOfNoDictionaryColumn;
    this.isDictionaryColumn = isDictionaryColumn;
    this.aggBlocks = aggBlocks;
    this.numberCompressor = new NumberCompressor(Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)));
  }

  @Override
  public NodeHolder buildDataNodeHolder(IndexStorage<int[]>[] keyStorageArray, byte[][] dataArray,
      int entryCount, byte[] startKey, byte[] endKey, ValueCompressionModel compressionModel,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) throws CarbonDataWriterException {
    // if there are no NO-Dictionary column present in the table then
    // set the empty byte array
    if (null == noDictionaryEndKey && null == noDictionaryStartKey) {
      noDictionaryStartKey = new byte[0];
      noDictionaryEndKey = new byte[0];
    }
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
    //    byte[][] columnMinMaxData = new byte[keyStorageArray.length][];

    byte[][] allMinValue = new byte[keyStorageArray.length][];
    byte[][] allMaxValue = new byte[keyStorageArray.length][];
    byte[][] keyBlockData = fillAndCompressedKeyBlockData(keyStorageArray, entryCount);
    boolean[] colGrpBlock = new boolean[keyStorageArray.length];

    for (int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keyBlockData[i].length;
      isSortedData[i] = keyStorageArray[i].isAlreadySorted();
      if (!isSortedData[i]) {
        keyBlockSize++;

      }
      totalKeySize += keyLengths[i];
      if (isComplexType[i] || isDictionaryColumn[i]) {
        allMinValue[i] = keyStorageArray[i].getMin();
        allMaxValue[i] = keyStorageArray[i].getMax();
      } else {
        allMinValue[i] = updateMinMaxForNoDictionary(keyStorageArray[i].getMin());
        allMaxValue[i] = updateMinMaxForNoDictionary(keyStorageArray[i].getMax());
      }
      //if keyStorageArray is instance of ColGroupBlockStorage than it's colGroup chunk
      if (keyStorageArray[i] instanceof ColGroupBlockStorage) {
        colGrpBlock[i] = true;
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
          throw new CarbonDataWriterException(e.getMessage());
        }
      }
    }

    byte[] writableKeyArray = new byte[totalKeySize];
    int startPosition = 0;
    for (int i = 0; i < keyLengths.length; i++) {
      System.arraycopy(keyBlockData[i], 0, writableKeyArray, startPosition, keyBlockData[i].length);
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
      System.arraycopy(dataArray[i], 0, writableDataArray, startPosition, dataArray[i].length);
      startPosition += msrLength[i];
    }
    // current file size;
    int indexBlockSize = 0;
    for (int i = 0; i < keyBlockIdxLengths.length; i++) {
      indexBlockSize += keyBlockIdxLengths[i] + CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }

    for (int i = 0; i < dataIndexMapLength.length; i++) {
      indexBlockSize += dataIndexMapLength[i];
    }

    NodeHolder holder = new NodeHolder();
    holder.setDataArray(writableDataArray);
    holder.setKeyArray(writableKeyArray);
    // end key format will be <length of dictionary key><length of no
    // dictionary key><DictionaryKey><No Dictionary key>
    byte[] updatedNoDictionaryEndKey = updateNoDictionaryStartAndEndKey(noDictionaryEndKey);
    ByteBuffer buffer = ByteBuffer.allocate(
        CarbonCommonConstants.INT_SIZE_IN_BYTE + CarbonCommonConstants.INT_SIZE_IN_BYTE
            + endKey.length + updatedNoDictionaryEndKey.length);
    buffer.putInt(endKey.length);
    buffer.putInt(updatedNoDictionaryEndKey.length);
    buffer.put(endKey);
    buffer.put(updatedNoDictionaryEndKey);
    buffer.rewind();
    holder.setEndKey(buffer.array());
    holder.setMeasureLenght(msrLength);
    byte[] updatedNoDictionaryStartKey = updateNoDictionaryStartAndEndKey(noDictionaryStartKey);
    // start key format will be <length of dictionary key><length of no
    // dictionary key><DictionaryKey><No Dictionary key>
    buffer = ByteBuffer.allocate(
        CarbonCommonConstants.INT_SIZE_IN_BYTE + CarbonCommonConstants.INT_SIZE_IN_BYTE
            + startKey.length + updatedNoDictionaryStartKey.length);
    buffer.putInt(startKey.length);
    buffer.putInt(updatedNoDictionaryStartKey.length);
    buffer.put(startKey);
    buffer.put(updatedNoDictionaryStartKey);
    buffer.rewind();
    holder.setStartKey(buffer.array());
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
    holder.setColumnMaxData(allMaxValue);
    holder.setColumnMinData(allMinValue);
    holder.setAggBlocks(aggBlocks);
    holder.setColGrpBlocks(colGrpBlock);
    return holder;
  }

  @Override public void writeBlockletData(NodeHolder holder) throws CarbonDataWriterException {
    int indexBlockSize = 0;
    for (int i = 0; i < holder.getKeyBlockIndexLength().length; i++) {
      indexBlockSize += holder.getKeyBlockIndexLength()[i] + CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }

    for (int i = 0; i < holder.getDataIndexMapLength().length; i++) {
      indexBlockSize += holder.getDataIndexMapLength()[i];
    }
    long blockletDataSize =
        holder.getKeyArray().length + holder.getDataArray().length + indexBlockSize;
    updateBlockletFileChannel(blockletDataSize);
    writeDataToFile(holder);
  }

  /**
   * Below method will be used to update the min or max value
   * by removing the length from it
   *
   * @return min max value without length
   */
  private byte[] updateMinMaxForNoDictionary(byte[] valueWithLength) {
    ByteBuffer buffer = ByteBuffer.wrap(valueWithLength);
    byte[] actualValue = new byte[buffer.getShort()];
    buffer.get(actualValue);
    return actualValue;
  }

  /**
   * Below method will be used to update the no dictionary start and end key
   *
   * @param key key to be updated
   * @return return no dictionary key
   */
  private byte[] updateNoDictionaryStartAndEndKey(byte[] key) {
    if (key.length == 0) {
      return key;
    }
    // add key to byte buffer remove the length part of the data
    ByteBuffer buffer = ByteBuffer.wrap(key, 2, key.length - 2);
    // create a output buffer without length
    ByteBuffer output = ByteBuffer.allocate(key.length - 2);
    short numberOfByteToStorLength = 2;
    // as length part is removed, so each no dictionary value index
    // needs to be reshuffled by 2 bytes
    for (int i = 0; i < numberOfNoDictionaryColumn; i++) {
      output.putShort((short) (buffer.getShort() - numberOfByteToStorLength));
    }
    // copy the data part
    while (buffer.hasRemaining()) {
      output.put(buffer.get());
    }
    output.rewind();
    return output.array();
  }

  protected byte[][] fillAndCompressedKeyBlockData(IndexStorage<int[]>[] keyStorageArray,
      int entryCount) {
    byte[][] keyBlockData = new byte[keyStorageArray.length][];
    int destPos = 0;
    int keyBlockSizePosition = -1;
    for (int i = 0; i < keyStorageArray.length; i++) {
      destPos = 0;
      //handling for high card dims
      if (!isComplexType[i] && !this.isDictionaryColumn[i]) {
        int totalLength = 0;
        // calc size of the total bytes in all the colmns.
        for (int k = 0; k < keyStorageArray[i].getKeyBlock().length; k++) {
          byte[] colValue = keyStorageArray[i].getKeyBlock()[k];
          totalLength += colValue.length;
        }
        keyBlockData[i] = new byte[totalLength];

        for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
          int length = keyStorageArray[i].getKeyBlock()[j].length;
          System
              .arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos, length);
          destPos += length;
        }
      } else {
        keyBlockSizePosition++;
        if (aggBlocks[i]) {
          keyBlockData[i] = new byte[keyStorageArray[i].getTotalSize()];
          for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
            System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos,
                keyStorageArray[i].getKeyBlock()[j].length);
            destPos += keyStorageArray[i].getKeyBlock()[j].length;
          }
        } else {
          if (isComplexType[i]) {
            keyBlockData[i] = new byte[keyStorageArray[i].getKeyBlock().length
                * keyBlockSize[keyBlockSizePosition]];
          } else {
            keyBlockData[i] = new byte[entryCount * keyBlockSize[keyBlockSizePosition]];
          }
          for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
            System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos,
                keyBlockSize[keyBlockSizePosition]);
            destPos += keyBlockSize[keyBlockSizePosition];
          }
        }
      }
      keyBlockData[i] = SnappyByteCompression.INSTANCE.compress(keyBlockData[i]);
    }
    return keyBlockData;
  }

  /**
   * This method is responsible for writing blocklet to the data file
   *
   * @return file offset offset is the current position of the file
   * @throws CarbonDataWriterException if will throw CarbonDataWriterException when any thing
   *                                   goes wrong while while writing the leaf file
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
      indexBlockSize +=
          nodeHolder.getKeyBlockIndexLength()[index++] + CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }

    for (int i = 0; i < nodeHolder.getDataIndexMapLength().length; i++) {
      indexBlockSize += nodeHolder.getDataIndexMapLength()[i];
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(
        nodeHolder.getKeyArray().length + nodeHolder.getDataArray().length + indexBlockSize);
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
      throw new CarbonDataWriterException("Problem in writing carbon file: ", exception);
    }
    // return the offset, this offset will be used while reading the file in
    // engine side to get from which position to start reading the file
    return offset;
  }

  /**
   * This method will be used to get the blocklet metadata
   *
   * @return BlockletInfo - blocklet metadata
   */
  protected BlockletInfoColumnar getBlockletInfo(NodeHolder nodeHolder, long offset) {
    // create the info object for leaf entry
    BlockletInfoColumnar info = new BlockletInfoColumnar();
    //add aggBlocks array
    info.setAggKeyBlock(nodeHolder.getAggBlocks());
    // add total entry count
    info.setNumberOfKeys(nodeHolder.getEntryCount());

    // add the key array length
    info.setKeyLengths(nodeHolder.getKeyLengths());

    //add column min max length
    info.setColumnMaxData(nodeHolder.getColumnMaxData());
    info.setColumnMinData(nodeHolder.getColumnMinData());
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
    info.setCompressionModel(nodeHolder.getCompressionModel());
    // return leaf metadata

    //colGroup Blocks
    info.setColGrpBlocks(nodeHolder.getColGrpBlocks());

    return info;
  }

}