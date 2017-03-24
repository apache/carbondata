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
package org.apache.carbondata.processing.store.writer.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.NodeHolder;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.processing.store.colgroup.ColGroupBlockStorage;
import org.apache.carbondata.processing.store.writer.AbstractFactDataWriter;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;

/**
 * Below class will be used to write the data in V3 format
 * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
 * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
 * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
 * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
 */
public class CarbonFactDataWriterImplV3 extends AbstractFactDataWriter<short[]> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataWriterImplV3.class.getName());

  /**
   * persist the page data to be written in the file
   */
  private DataWriterHolder dataWriterHolder;

  private long blockletSize;

  public CarbonFactDataWriterImplV3(CarbonDataWriterVo dataWriterVo) {
    super(dataWriterVo);
    blockletSize = Long.parseLong(CarbonProperties.getInstance()
        .getProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE))
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    if (blockletSize > fileSizeInBytes) {
      blockletSize = fileSizeInBytes;
      LOGGER.info("Blocklet size configure for table is: " + blockletSize);
    }
    dataWriterHolder = new DataWriterHolder();
  }

  /**
   * Below method will be used to build the node holder object
   * This node holder object will be used to persist data which will
   * be written in carbon data file
   */
  @Override public NodeHolder buildDataNodeHolder(IndexStorage<short[]>[] keyStorageArray,
      byte[][] dataArray, int entryCount, byte[] startKey, byte[] endKey,
      WriterCompressModel compressionModel, byte[] noDictionaryStartKey, byte[] noDictionaryEndKey,
      BitSet[] nullValueIndexBitSet) throws CarbonDataWriterException {
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

    // below will calculate min and max value for each column
    // for below 2d array, first index will be for column and second will be min
    // max
    // value for same column
    // byte[][] columnMinMaxData = new byte[keyStorageArray.length][];

    byte[][] dimensionMinValue = new byte[keyStorageArray.length][];
    byte[][] dimensionMaxValue = new byte[keyStorageArray.length][];

    byte[][] measureMinValue = new byte[dataArray.length][];
    byte[][] measureMaxValue = new byte[dataArray.length][];

    byte[][] keyBlockData = fillAndCompressedKeyBlockData(keyStorageArray, entryCount);
    boolean[] colGrpBlock = new boolean[keyStorageArray.length];

    for (int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keyBlockData[i].length;
      isSortedData[i] = keyStorageArray[i].isAlreadySorted();
      keyBlockSize++;
      totalKeySize += keyLengths[i];
      if (dataWriterVo.getIsComplexType()[i] || dataWriterVo.getIsDictionaryColumn()[i]) {
        dimensionMinValue[i] = keyStorageArray[i].getMin();
        dimensionMaxValue[i] = keyStorageArray[i].getMax();
      } else {
        dimensionMinValue[i] = updateMinMaxForNoDictionary(keyStorageArray[i].getMin());
        dimensionMaxValue[i] = updateMinMaxForNoDictionary(keyStorageArray[i].getMax());
      }
      // if keyStorageArray is instance of ColGroupBlockStorage than it's
      // colGroup chunk
      if (keyStorageArray[i] instanceof ColGroupBlockStorage) {
        colGrpBlock[i] = true;
      }
    }
    for (int i = 0; i < dataArray.length; i++) {
      measureMaxValue[i] = CarbonMetadataUtil
          .getByteValueForMeasure(compressionModel.getMaxValue()[i],
              dataWriterVo.getSegmentProperties().getMeasures().get(i).getDataType());
      measureMinValue[i] = CarbonMetadataUtil
          .getByteValueForMeasure(compressionModel.getMinValue()[i],
              dataWriterVo.getSegmentProperties().getMeasures().get(i).getDataType());
    }
    int[] keyBlockIdxLengths = new int[keyBlockSize];
    byte[][] dataAfterCompression = new byte[keyBlockSize][];
    byte[][] indexMap = new byte[keyBlockSize][];
    for (int i = 0; i < isSortedData.length; i++) {
      if (!isSortedData[i]) {
        dataAfterCompression[i] = getByteArray(keyStorageArray[i].getDataAfterComp());
        if (null != keyStorageArray[i].getIndexMap()
            && keyStorageArray[i].getIndexMap().length > 0) {
          indexMap[i] = getByteArray(keyStorageArray[i].getIndexMap());
        } else {
          indexMap[i] = new byte[0];
        }
        keyBlockIdxLengths[i] = (dataAfterCompression[i].length + indexMap[i].length)
            + CarbonCommonConstants.INT_SIZE_IN_BYTE;
      }
    }
    byte[][] compressedDataIndex = new byte[keyBlockSize][];
    int[] dataIndexMapLength = new int[keyBlockSize];
    for (int i = 0; i < dataWriterVo.getAggBlocks().length; i++) {
      if (dataWriterVo.getAggBlocks()[i]) {
        try {
          compressedDataIndex[i] = getByteArray(keyStorageArray[i].getDataIndexMap());
          dataIndexMapLength[i] = compressedDataIndex[i].length;
        } catch (Exception e) {
          throw new CarbonDataWriterException(e.getMessage(), e);
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
    holder.setMeasureColumnMaxData(measureMaxValue);
    holder.setMeasureColumnMinData(measureMinValue);
    // setting column min max value
    holder.setColumnMaxData(dimensionMaxValue);
    holder.setColumnMinData(dimensionMinValue);
    holder.setAggBlocks(dataWriterVo.getAggBlocks());
    holder.setColGrpBlocks(colGrpBlock);
    List<byte[]> dimensionDataChunk2 = null;
    List<byte[]> measureDataChunk2 = null;
    try {
      dimensionDataChunk2 = CarbonMetadataUtil
          .getDataChunk2(holder, thriftColumnSchemaList, dataWriterVo.getSegmentProperties(), true);
      measureDataChunk2 = CarbonMetadataUtil
          .getDataChunk2(holder, thriftColumnSchemaList, dataWriterVo.getSegmentProperties(),
              false);

    } catch (IOException e) {
      throw new CarbonDataWriterException(e.getMessage());
    }
    holder.setHolderSize(calculateSize(holder, dimensionDataChunk2, measureDataChunk2));
    return holder;
  }

  private int calculateSize(NodeHolder holder, List<byte[]> dimensionDataChunk2,
      List<byte[]> measureDataChunk2) {
    int size = 0;
    // add row id index length
    for (int i = 0; i < holder.getKeyBlockIndexLength().length; i++) {
      if (!holder.getIsSortedKeyBlock()[i]) {
        size += holder.getKeyBlockIndexLength()[i];
      }
    }
    // add rle index length
    for (int i = 0; i < holder.getDataIndexMapLength().length; i++) {
      if (holder.getAggBlocks()[i]) {
        size += holder.getDataIndexMapLength()[i];
      }
    }
    for (int i = 0; i < dimensionDataChunk2.size(); i++) {
      size += dimensionDataChunk2.get(i).length;
    }
    for (int i = 0; i < measureDataChunk2.size(); i++) {
      size += measureDataChunk2.get(i).length;
    }
    size += holder.getTotalDimensionArrayLength() + holder.getTotalMeasureArrayLength();
    return size;
  }

  /**
   * Below method will be used to convert short array to byte array
   *
   * @param data in short data
   * @return byte array
   */
  private byte[] getByteArray(short[] data) {
    ByteBuffer buffer = ByteBuffer.allocate(data.length * 2);
    for (int i = 0; i < data.length; i++) {
      buffer.putShort(data[i]);
    }
    buffer.flip();
    return buffer.array();
  }

  @Override protected void writeBlockletInfoToFile(FileChannel channel, String filePath)
      throws CarbonDataWriterException {
    try {
      // get the current file position
      long currentPosition = channel.size();
      // get thrift file footer instance
      FileFooter3 convertFileMeta = CarbonMetadataUtil
          .convertFileFooterVersion3(blockletMetadata, blockletIndex, localCardinality,
              thriftColumnSchemaList.size(), dataWriterVo.getSegmentProperties());
      // fill the carbon index details
      fillBlockIndexInfoDetails(convertFileMeta.getNum_rows(), carbonDataFilePath, currentPosition);
      // write the footer
      byte[] byteArray = CarbonUtil.getByteArray(convertFileMeta);
      ByteBuffer buffer =
          ByteBuffer.allocate(byteArray.length + CarbonCommonConstants.LONG_SIZE_IN_BYTE);
      buffer.put(byteArray);
      buffer.putLong(currentPosition);
      buffer.flip();
      channel.write(buffer);
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }
  }

  /**
   * Below method will be used to write blocklet data to file
   */
  @Override public void writeBlockletData(NodeHolder holder) throws CarbonDataWriterException {
    // check the number of pages present in data holder, if pages is exceeding threshold
    // it will write the pages to file
    if (dataWriterHolder.getSize() + holder.getHolderSize() >= blockletSize) {
      LOGGER.info("Number of Pages for blocklet is: " + dataWriterHolder.getSize());
      writeDataToFile(fileChannel);
    }
    dataWriterHolder.addNodeHolder(holder);
  }

  private void writeDataToFile(FileChannel channel) {
    // get the list of node holder list
    List<NodeHolder> nodeHolderList = dataWriterHolder.getNodeHolder();
    long blockletDataSize = 0;
    // get data chunks for all the column
    byte[][] dataChunkBytes =
        new byte[nodeHolderList.get(0).getKeyArray().length + nodeHolderList.get(0)
            .getDataArray().length][];
    int measureStartIndex = nodeHolderList.get(0).getKeyArray().length;
    // calculate the size of data chunks
    try {
      for (int i = 0; i < nodeHolderList.get(0).getKeyArray().length; i++) {
        dataChunkBytes[i] = CarbonUtil.getByteArray(CarbonMetadataUtil
            .getDataChunk3(nodeHolderList, thriftColumnSchemaList,
                dataWriterVo.getSegmentProperties(), i, true));
        blockletDataSize += dataChunkBytes[i].length;
      }
      for (int i = 0; i < nodeHolderList.get(0).getDataArray().length; i++) {
        dataChunkBytes[measureStartIndex] = CarbonUtil.getByteArray(CarbonMetadataUtil
            .getDataChunk3(nodeHolderList, thriftColumnSchemaList,
                dataWriterVo.getSegmentProperties(), i, false));
        blockletDataSize += dataChunkBytes[measureStartIndex].length;
        measureStartIndex++;
      }
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while getting the data chunks", e);
    }
    // calculate the total size of data to be written
    blockletDataSize += dataWriterHolder.getSize();
    // to check if data size will exceed the block size then create a new file
    updateBlockletFileChannel(blockletDataSize);
    // write data to file
    writeDataToFile(fileChannel, dataChunkBytes);
    // clear the data holder
    dataWriterHolder.clear();
  }

  /**
   * Below method will be used to write data in carbon data file
   * Data Format
   * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
   * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
   * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
   * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
   * Each page will contain column data, Inverted index and rle index
   *
   * @param channel
   * @param dataChunkBytes
   */
  private void writeDataToFile(FileChannel channel, byte[][] dataChunkBytes) {
    long offset = 0;
    // write the header
    try {
      if (fileChannel.size() == 0) {
        // below code is to write the file header
        byte[] fileHeader = CarbonUtil.getByteArray(CarbonMetadataUtil
            .getFileHeader(true, thriftColumnSchemaList, dataWriterVo.getSchemaUpdatedTimeStamp()));
        ByteBuffer buffer = ByteBuffer.allocate(fileHeader.length);
        buffer.put(fileHeader);
        buffer.flip();
        fileChannel.write(buffer);
      }
      offset = channel.size();
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while getting the file channel size");
    }
    // to maintain the offset of each data chunk in blocklet
    List<Long> currentDataChunksOffset = new ArrayList<>();
    // to maintain the length of each data chunk in blocklet
    List<Integer> currentDataChunksLength = new ArrayList<>();
    // get the node holder list
    List<NodeHolder> nodeHolderList = dataWriterHolder.getNodeHolder();
    int numberOfDimension = nodeHolderList.get(0).getKeyArray().length;
    int numberOfMeasures = nodeHolderList.get(0).getDataArray().length;
    NodeHolder nodeHolder = null;
    ByteBuffer buffer = null;
    int bufferSize = 0;
    long dimensionOffset = 0;
    long measureOffset = 0;
    int numberOfRows = 0;
    // calculate the number of rows in each blocklet
    for (int j = 0; j < nodeHolderList.size(); j++) {
      numberOfRows += nodeHolderList.get(j).getEntryCount();
    }
    try {
      for (int i = 0; i < numberOfDimension; i++) {
        currentDataChunksOffset.add(offset);
        currentDataChunksLength.add(dataChunkBytes[i].length);
        buffer = ByteBuffer.allocate(dataChunkBytes[i].length);
        buffer.put(dataChunkBytes[i]);
        buffer.flip();
        fileChannel.write(buffer);
        offset += dataChunkBytes[i].length;
        for (int j = 0; j < nodeHolderList.size(); j++) {
          nodeHolder = nodeHolderList.get(j);
          bufferSize = nodeHolder.getKeyLengths()[i] + (!nodeHolder.getIsSortedKeyBlock()[i] ?
              nodeHolder.getKeyBlockIndexLength()[i] :
              0) + (dataWriterVo.getAggBlocks()[i] ?
              nodeHolder.getCompressedDataIndex()[i].length :
              0);
          buffer = ByteBuffer.allocate(bufferSize);
          buffer.put(nodeHolder.getKeyArray()[i]);
          if (!nodeHolder.getIsSortedKeyBlock()[i]) {
            buffer.putInt(nodeHolder.getCompressedIndex()[i].length);
            buffer.put(nodeHolder.getCompressedIndex()[i]);
            if (nodeHolder.getCompressedIndexMap()[i].length > 0) {
              buffer.put(nodeHolder.getCompressedIndexMap()[i]);
            }
          }
          if (nodeHolder.getAggBlocks()[i]) {
            buffer.put(nodeHolder.getCompressedDataIndex()[i]);
          }
          buffer.flip();
          fileChannel.write(buffer);
          offset += bufferSize;
        }
      }
      dimensionOffset = offset;
      int dataChunkStartIndex = nodeHolderList.get(0).getKeyArray().length;
      for (int i = 0; i < numberOfMeasures; i++) {
        nodeHolderList = dataWriterHolder.getNodeHolder();
        currentDataChunksOffset.add(offset);
        currentDataChunksLength.add(dataChunkBytes[dataChunkStartIndex].length);
        buffer = ByteBuffer.allocate(dataChunkBytes[dataChunkStartIndex].length);
        buffer.put(dataChunkBytes[dataChunkStartIndex]);
        buffer.flip();
        fileChannel.write(buffer);
        offset += dataChunkBytes[dataChunkStartIndex].length;
        dataChunkStartIndex++;
        for (int j = 0; j < nodeHolderList.size(); j++) {
          nodeHolder = nodeHolderList.get(j);
          bufferSize = nodeHolder.getDataArray()[i].length;
          buffer = ByteBuffer.allocate(bufferSize);
          buffer.put(nodeHolder.getDataArray()[i]);
          buffer.flip();
          fileChannel.write(buffer);
          offset += bufferSize;
        }
      }
      measureOffset = offset;
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the data", e);
    }
    blockletIndex.add(CarbonMetadataUtil
        .getBlockletIndex(nodeHolderList, dataWriterVo.getSegmentProperties().getMeasures()));
    BlockletInfo3 blockletInfo3 =
        new BlockletInfo3(numberOfRows, currentDataChunksOffset, currentDataChunksLength,
            dimensionOffset, measureOffset, dataWriterHolder.getNodeHolder().size());
    blockletMetadata.add(blockletInfo3);
  }

  /**
   * Below method will be used to fill the block info details
   *
   * @param numberOfRows    number of rows in file
   * @param filePath        file path
   * @param currentPosition current offset
   */
  protected void fillBlockIndexInfoDetails(long numberOfRows, String filePath,
      long currentPosition) {
    byte[][] currentMinValue = new byte[blockletIndex.get(0).min_max_index.max_values.size()][];
    byte[][] currentMaxValue = new byte[blockletIndex.get(0).min_max_index.max_values.size()][];
    for (int i = 0; i < currentMaxValue.length; i++) {
      currentMinValue[i] = blockletIndex.get(0).min_max_index.getMin_values().get(i).array();
      currentMaxValue[i] = blockletIndex.get(0).min_max_index.getMax_values().get(i).array();
    }
    byte[] minValue = null;
    byte[] maxValue = null;
    int measureStartIndex = currentMinValue.length - dataWriterVo.getMeasureCount();
    for (int i = 1; i < blockletIndex.size(); i++) {
      for (int j = 0; j < measureStartIndex; j++) {
        minValue = blockletIndex.get(i).min_max_index.getMin_values().get(j).array();
        maxValue = blockletIndex.get(i).min_max_index.getMax_values().get(j).array();
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMinValue[j], minValue) > 0) {
          currentMinValue[j] = minValue.clone();
        }
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMaxValue[j], maxValue) < 0) {
          currentMaxValue[j] = maxValue.clone();
        }
      }
      int measureIndex = 0;
      for (int j = measureStartIndex; j < currentMinValue.length; j++) {
        minValue = blockletIndex.get(i).min_max_index.getMin_values().get(j).array();
        maxValue = blockletIndex.get(i).min_max_index.getMax_values().get(j).array();

        if (CarbonMetadataUtil.compareMeasureData(currentMinValue[j], minValue,
            dataWriterVo.getSegmentProperties().getMeasures().get(measureIndex).getDataType())
            > 0) {
          currentMinValue[j] = minValue.clone();
        }
        if (CarbonMetadataUtil.compareMeasureData(currentMaxValue[j], maxValue,
            dataWriterVo.getSegmentProperties().getMeasures().get(measureIndex).getDataType())
            < 0) {
          currentMaxValue[j] = maxValue.clone();
        }
        measureIndex++;
      }
    }
    BlockletBTreeIndex btree =
        new BlockletBTreeIndex(blockletIndex.get(0).b_tree_index.getStart_key(),
            blockletIndex.get(blockletIndex.size() - 1).b_tree_index.getEnd_key());
    BlockletMinMaxIndex minmax = new BlockletMinMaxIndex();
    minmax.setMinValues(currentMinValue);
    minmax.setMaxValues(currentMaxValue);
    org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex blockletIndex =
        new org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex(btree, minmax);
    BlockIndexInfo blockIndexInfo =
        new BlockIndexInfo(numberOfRows, filePath, currentPosition, blockletIndex);
    blockIndexInfoList.add(blockIndexInfo);
  }

  /**
   * Method will be used to close the open file channel
   *
   * @throws CarbonDataWriterException
   */
  public void closeWriter() throws CarbonDataWriterException {
    if (dataWriterHolder.getNodeHolder().size() > 0) {
      writeDataToFile(fileChannel);
      writeBlockletInfoToFile(fileChannel, fileName);
      CarbonUtil.closeStreams(this.fileOutputStream, this.fileChannel);
      renameCarbonDataFile();
      copyCarbonDataFileToCarbonStorePath(
          this.fileName.substring(0, this.fileName.lastIndexOf('.')));
      try {
        writeIndexFile();
      } catch (IOException e) {
        throw new CarbonDataWriterException("Problem while writing the index file", e);
      }
    }
    closeExecutorService();
  }
}
