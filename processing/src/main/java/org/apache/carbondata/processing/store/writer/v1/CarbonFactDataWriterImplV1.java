/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.store.writer.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.metadata.BlockletInfoColumnar;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.NodeHolder;
import org.apache.carbondata.core.writer.CarbonFooterWriter;
import org.apache.carbondata.format.FileFooter;
import org.apache.carbondata.processing.store.colgroup.ColGroupBlockStorage;
import org.apache.carbondata.processing.store.writer.AbstractFactDataWriter;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;

public class CarbonFactDataWriterImplV1 extends AbstractFactDataWriter<int[]> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataWriterImplV1.class.getName());

  public CarbonFactDataWriterImplV1(CarbonDataWriterVo dataWriterVo) {
    super(dataWriterVo);
  }

  @Override
  public NodeHolder buildDataNodeHolder(IndexStorage<int[]>[] keyStorageArray, byte[][] dataArray,
      int entryCount, byte[] startKey, byte[] endKey, WriterCompressModel compressionModel,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey, BitSet[] nullValueIndexBitSet)
      throws CarbonDataWriterException {
    // if there are no NO-Dictionary column present in the table then
    // set the empty byte array
    if (null == noDictionaryEndKey) {
      noDictionaryEndKey = new byte[0];
    }
    if (null == noDictionaryStartKey) {
      noDictionaryStartKey = new byte[0];
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
      if (dataWriterVo.getIsComplexType()[i] || dataWriterVo.getIsDictionaryColumn()[i]) {
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
    for (int i = 0; i < dataWriterVo.getAggBlocks().length; i++) {
      if (dataWriterVo.getAggBlocks()[i]) {
        compressDataBlockSize++;
      }
    }
    byte[][] compressedDataIndex = new byte[compressDataBlockSize][];
    int[] dataIndexMapLength = new int[compressDataBlockSize];
    idx = 0;
    for (int i = 0; i < dataWriterVo.getAggBlocks().length; i++) {
      if (dataWriterVo.getAggBlocks()[i]) {
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

    int[] msrLength = new int[dataWriterVo.getMeasureCount()];
    // calculate the total size required for all the measure and get the
    // each measure size
    for (int i = 0; i < dataArray.length; i++) {
      currentMsrLenght = dataArray[i].length;
      totalMsrArrySize += currentMsrLenght;
      msrLength[i] = currentMsrLenght;
    }
    NodeHolder holder = new NodeHolder();
    holder.setDataArray(dataArray);
    holder.setKeyArray(keyBlockData);
    holder.setMeasureNullValueIndex(nullValueIndexBitSet);
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
    holder.setTotalDimensionArrayLength(totalKeySize);
    holder.setTotalMeasureArrayLength(totalMsrArrySize);
    //setting column min max value
    holder.setColumnMaxData(allMaxValue);
    holder.setColumnMinData(allMinValue);
    holder.setAggBlocks(dataWriterVo.getAggBlocks());
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
        holder.getTotalDimensionArrayLength() + holder.getTotalMeasureArrayLength()
            + indexBlockSize;
    updateBlockletFileChannel(blockletDataSize);
    // write data to file and get its offset
    long offset = writeDataToFile(holder, fileChannel);
    // get the blocklet info for currently added blocklet
    BlockletInfoColumnar blockletInfo = getBlockletInfo(holder, offset);
    // add blocklet info to list
    blockletInfoList.add(blockletInfo);
    LOGGER.info("A new blocklet is added, its data size is: " + blockletDataSize + " Byte");
  }

  /**
   * This method is responsible for writing blocklet to the data file
   *
   * @return file offset offset is the current position of the file
   * @throws CarbonDataWriterException if will throw CarbonDataWriterException when any thing
   *                                   goes wrong while while writing the leaf file
   */
  private long writeDataToFile(NodeHolder nodeHolder, FileChannel channel)
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
        nodeHolder.getTotalDimensionArrayLength() + nodeHolder.getTotalMeasureArrayLength()
            + indexBlockSize);
    long offset = 0;
    try {
      // get the current offset
      offset = channel.size();
      // add key array to byte buffer
      for (int i = 0; i < nodeHolder.getKeyArray().length; i++) {
        byteBuffer.put(nodeHolder.getKeyArray()[i]);
      }
      for (int i = 0; i < nodeHolder.getDataArray().length; i++) {
        byteBuffer.put(nodeHolder.getDataArray()[i]);
      }
      // add measure data array to byte buffer

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
    // adding null measure index bit set
    info.setMeasureNullValueIndex(nodeHolder.getMeasureNullValueIndex());
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

    long[] msrOffset = new long[dataWriterVo.getMeasureCount()];

    for (int i = 0; i < msrOffset.length; i++) {
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

  /**
   * This method will write metadata at the end of file file format in thrift format
   */
  protected void writeBlockletInfoToFile(FileChannel channel, String filePath)
      throws CarbonDataWriterException {
    try {
      long currentPosition = channel.size();
      CarbonFooterWriter writer = new CarbonFooterWriter(filePath);
      FileFooter convertFileMeta = CarbonMetadataUtil
          .convertFileFooter(blockletInfoList, localCardinality.length, localCardinality,
              thriftColumnSchemaList, dataWriterVo.getSegmentProperties());
      fillBlockIndexInfoDetails(convertFileMeta.getNum_rows(), carbonDataFilePath, currentPosition);
      writer.writeFooter(convertFileMeta, currentPosition);
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }
  }
}