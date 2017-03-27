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
package org.apache.carbondata.processing.store.writer.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.BlockletInfoColumnar;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.NodeHolder;
import org.apache.carbondata.core.writer.CarbonFooterWriter;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.FileFooter;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.store.writer.v1.CarbonFactDataWriterImplV1;

/**
 * Below method will be used to write the data in version 2 format
 */
public class CarbonFactDataWriterImplV2 extends CarbonFactDataWriterImplV1 {

  /**
   * logger
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataWriterImplV2.class.getName());

  /**
   * Constructor create instance of this class
   *
   * @param dataWriterVo
   */
  public CarbonFactDataWriterImplV2(CarbonDataWriterVo dataWriterVo) {
    super(dataWriterVo);
  }

  /**
   * Below method will be used to write the data to carbon data file
   *
   * @param holder
   * @throws CarbonDataWriterException any problem in writing operation
   */
  @Override public void writeBlockletData(NodeHolder holder) throws CarbonDataWriterException {
    // size to calculate the size of the blocklet
    int size = 0;
    // get the blocklet info object
    BlockletInfoColumnar blockletInfo = getBlockletInfo(holder, 0);

    List<DataChunk2> datachunks = null;
    try {
      // get all the data chunks
      datachunks = CarbonMetadataUtil
          .getDatachunk2(blockletInfo, thriftColumnSchemaList, dataWriterVo.getSegmentProperties());
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while getting the data chunks", e);
    }
    // data chunk byte array
    byte[][] dataChunkByteArray = new byte[datachunks.size()][];
    for (int i = 0; i < dataChunkByteArray.length; i++) {
      dataChunkByteArray[i] = CarbonUtil.getByteArray(datachunks.get(i));
      // add the data chunk size
      size += dataChunkByteArray[i].length;
    }
    // add row id index length
    for (int i = 0; i < holder.getKeyBlockIndexLength().length; i++) {
      size += holder.getKeyBlockIndexLength()[i];
    }
    // add rle index length
    for (int i = 0; i < holder.getDataIndexMapLength().length; i++) {
      size += holder.getDataIndexMapLength()[i];
    }
    // add dimension column data page and measure column data page size
    long blockletDataSize =
        holder.getTotalDimensionArrayLength() + holder.getTotalMeasureArrayLength() + size;
    // if size of the file already reached threshold size then create a new file and get the file
    // channel object
    updateBlockletFileChannel(blockletDataSize);
    // writer the version header in the file if current file size is zero
    // this is done so carbondata file can be read separately
    try {
      if (fileChannel.size() == 0) {
        ColumnarFormatVersion version = CarbonProperties.getInstance().getFormatVersion();
        byte[] header = (CarbonCommonConstants.CARBON_DATA_VERSION_HEADER + version).getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(header.length);
        buffer.put(header);
        buffer.rewind();
        fileChannel.write(buffer);
      }
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while getting the file channel size", e);
    }
    // write data to file and get its offset
    writeDataToFile(holder, dataChunkByteArray, fileChannel);
    // add blocklet info to list
    blockletInfoList.add(blockletInfo);
    LOGGER.info("A new blocklet is added, its data size is: " + blockletDataSize + " Byte");
  }

  /**
   * Below method will be used to write the data to file
   * Data Format
   * <DColumn1DataChunk><DColumnDataPage><DColumnRle>
   * <DColumn2DataChunk><DColumn2DataPage><DColumn2RowIds><DColumn2Rle>
   * <DColumn3DataChunk><DColumn3DataPage><column3RowIds>
   * <MColumn1DataChunk><MColumn1DataPage>
   * <MColumn2DataChunk><MColumn2DataPage>
   * <MColumn2DataChunk><MColumn2DataPage>
   *
   * @param nodeHolder
   * @param dataChunksBytes
   * @param channel
   * @throws CarbonDataWriterException
   */
  private void writeDataToFile(NodeHolder nodeHolder, byte[][] dataChunksBytes, FileChannel channel)
      throws CarbonDataWriterException {
    long offset = 0;
    try {
      offset = channel.size();
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while getting the file channel size");
    }
    List<Long> currentDataChunksOffset = new ArrayList<>();
    List<Short> currentDataChunksLength = new ArrayList<>();
    dataChunksLength.add(currentDataChunksLength);
    dataChunksOffsets.add(currentDataChunksOffset);
    int bufferSize = 0;
    int rowIdIndex = 0;
    int rleIndex = 0;
    for (int i = 0; i < nodeHolder.getIsSortedKeyBlock().length; i++) {
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add((short) dataChunksBytes[i].length);
      bufferSize += dataChunksBytes[i].length + nodeHolder.getKeyLengths()[i] + (!nodeHolder
          .getIsSortedKeyBlock()[i] ? nodeHolder.getKeyBlockIndexLength()[rowIdIndex] : 0) + (
          dataWriterVo.getAggBlocks()[i] ?
              nodeHolder.getCompressedDataIndex()[rleIndex].length :
              0);
      offset += dataChunksBytes[i].length;
      offset += nodeHolder.getKeyLengths()[i];
      if (!nodeHolder.getIsSortedKeyBlock()[i]) {
        offset += nodeHolder.getKeyBlockIndexLength()[rowIdIndex];
        rowIdIndex++;
      }
      if (dataWriterVo.getAggBlocks()[i]) {
        offset += nodeHolder.getDataIndexMapLength()[rleIndex];
        rleIndex++;
      }
    }
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    rleIndex = 0;
    rowIdIndex = 0;
    for (int i = 0; i < nodeHolder.getIsSortedKeyBlock().length; i++) {
      buffer.put(dataChunksBytes[i]);
      buffer.put(nodeHolder.getKeyArray()[i]);
      if (!nodeHolder.getIsSortedKeyBlock()[i]) {
        buffer.putInt(nodeHolder.getCompressedIndex()[rowIdIndex].length);
        buffer.put(nodeHolder.getCompressedIndex()[rowIdIndex]);
        if (nodeHolder.getCompressedIndexMap()[rowIdIndex].length > 0) {
          buffer.put(nodeHolder.getCompressedIndexMap()[rowIdIndex]);
        }
        rowIdIndex++;
      }
      if (dataWriterVo.getAggBlocks()[i]) {
        buffer.put(nodeHolder.getCompressedDataIndex()[rleIndex]);
        rleIndex++;
      }
    }
    try {
      buffer.flip();
      channel.write(buffer);
    } catch (IOException e) {
      throw new CarbonDataWriterException(
          "Problem while writing the dimension data in carbon data file", e);
    }

    int dataChunkIndex = nodeHolder.getKeyArray().length;
    int totalLength = 0;
    for (int i = 0; i < nodeHolder.getDataArray().length; i++) {
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add((short) dataChunksBytes[dataChunkIndex].length);
      offset += dataChunksBytes[dataChunkIndex].length;
      offset += nodeHolder.getDataArray()[i].length;
      totalLength += dataChunksBytes[dataChunkIndex].length;
      totalLength += nodeHolder.getDataArray()[i].length;
      dataChunkIndex++;
    }
    buffer = ByteBuffer.allocate(totalLength);
    dataChunkIndex = nodeHolder.getKeyArray().length;
    for (int i = 0; i < nodeHolder.getDataArray().length; i++) {
      buffer.put(dataChunksBytes[dataChunkIndex++]);
      buffer.put(nodeHolder.getDataArray()[i]);
    }
    try {
      buffer.flip();
      channel.write(buffer);
    } catch (IOException e) {
      throw new CarbonDataWriterException(
          "Problem while writing the measure data in carbon data file", e);
    }
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

    // add measure length
    info.setMeasureLength(nodeHolder.getMeasureLenght());

    info.setIsSortedKeyColumn(nodeHolder.getIsSortedKeyBlock());
    info.setKeyBlockIndexLength(nodeHolder.getKeyBlockIndexLength());
    info.setDataIndexMapLength(nodeHolder.getDataIndexMapLength());
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
  protected void writeBlockletInfoToFile(FileChannel channel,
      String filePath) throws CarbonDataWriterException {
    try {
      // get the current file position
      long currentPosition = channel.size();
      CarbonFooterWriter writer = new CarbonFooterWriter(filePath);
      // get thrift file footer instance
      FileFooter convertFileMeta = CarbonMetadataUtil
          .convertFilterFooter2(blockletInfoList, localCardinality, thriftColumnSchemaList,
              dataChunksOffsets, dataChunksLength);
      // fill the carbon index details
      fillBlockIndexInfoDetails(convertFileMeta.getNum_rows(), carbonDataFileName, currentPosition);
      // write the footer
      writer.writeFooter(convertFileMeta, currentPosition);
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }
  }
}
