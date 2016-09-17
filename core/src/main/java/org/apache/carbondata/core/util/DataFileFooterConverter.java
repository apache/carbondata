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
package org.apache.carbondata.core.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.block.BlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.ChunkCompressorMeta;
import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.CompressionCodec;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.sort.SortState;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.reader.CarbonFooterReader;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.FileFooter;

/**
 * Below class will be used to convert the thrift object of data file
 * meta data to wrapper object
 */
public class DataFileFooterConverter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataFileFooterConverter.class.getName());

  /**
   * Below method will be used to get the index info from index file
   *
   * @param filePath           file path of the index file
   * @param tableBlockInfoList table block index
   * @return list of index info
   * @throws IOException problem while reading the index file
   */
  public List<DataFileFooter> getIndexInfo(String filePath, List<TableBlockInfo> tableBlockInfoList)
      throws IOException, CarbonUtilException {
    CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
    List<DataFileFooter> dataFileFooters = new ArrayList<DataFileFooter>();
    try {
      // open the reader
      indexReader.openThriftReader(filePath);
      // get the index header
      org.apache.carbondata.format.IndexHeader readIndexHeader = indexReader.readIndexHeader();
      List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
      List<org.apache.carbondata.format.ColumnSchema> table_columns =
          readIndexHeader.getTable_columns();
      for (int i = 0; i < table_columns.size(); i++) {
        columnSchemaList.add(thriftColumnSchmeaToWrapperColumnSchema(table_columns.get(i)));
      }
      // get the segment info
      SegmentInfo segmentInfo = getSegmentInfo(readIndexHeader.getSegment_info());
      BlockletIndex blockletIndex = null;
      int counter = 0;
      DataFileFooter dataFileFooter = null;
      // read the block info from file
      while (indexReader.hasNext()) {
        BlockIndex readBlockIndexInfo = indexReader.readBlockIndexInfo();
        blockletIndex = getBlockletIndex(readBlockIndexInfo.getBlock_index());
        dataFileFooter = new DataFileFooter();
        TableBlockInfo tableBlockInfo = tableBlockInfoList.get(counter++);
        int blockletSize = getBlockletSize(readBlockIndexInfo);
        tableBlockInfo.getBlockletInfos().setNoOfBlockLets(blockletSize);
        dataFileFooter.setBlockletIndex(blockletIndex);
        dataFileFooter.setColumnInTable(columnSchemaList);
        dataFileFooter.setNumberOfRows(readBlockIndexInfo.getNum_rows());
        dataFileFooter.setBlockInfo(new BlockInfo(tableBlockInfo));
        dataFileFooter.setSegmentInfo(segmentInfo);
        dataFileFooters.add(dataFileFooter);
      }
    } finally {
      indexReader.closeThriftReader();
    }
    return dataFileFooters;
  }

  /**
   * the methods returns the number of blocklets in a block
   * @param readBlockIndexInfo
   * @return
   */
  private int getBlockletSize(BlockIndex readBlockIndexInfo) {
    long num_rows = readBlockIndexInfo.getNum_rows();
    int blockletSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    int remainder = (int) (num_rows % blockletSize);
    int noOfBlockLet = (int) (num_rows / blockletSize);
    // there could be some blocklets which will not
    // contain the total records equal to the blockletSize
    if (remainder > 0) {
      noOfBlockLet = noOfBlockLet + 1;
    }
    return noOfBlockLet;
  }

  /**
   * Below method will be used to convert thrift file meta to wrapper file meta
   */
  public DataFileFooter readDataFileFooter(String filePath, long blockOffset, long blockLength)
      throws IOException {
    DataFileFooter dataFileFooter = new DataFileFooter();
    FileHolder fileReader = null;
    try {
      long completeBlockLength = blockOffset + blockLength;
      long footerPointer = completeBlockLength - 8;
      fileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath));
      long actualFooterOffset = fileReader.readLong(filePath, footerPointer);
      CarbonFooterReader reader = new CarbonFooterReader(filePath, actualFooterOffset);
      FileFooter footer = reader.readFooter();
      dataFileFooter.setVersionId(footer.getVersion());
      dataFileFooter.setNumberOfRows(footer.getNum_rows());
      dataFileFooter.setSegmentInfo(getSegmentInfo(footer.getSegment_info()));
      List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
      List<org.apache.carbondata.format.ColumnSchema> table_columns = footer.getTable_columns();
      for (int i = 0; i < table_columns.size(); i++) {
        columnSchemaList.add(thriftColumnSchmeaToWrapperColumnSchema(table_columns.get(i)));
      }
      dataFileFooter.setColumnInTable(columnSchemaList);

      List<org.apache.carbondata.format.BlockletIndex> leaf_node_indices_Thrift =
          footer.getBlocklet_index_list();
      List<BlockletIndex> blockletIndexList = new ArrayList<BlockletIndex>();
      for (int i = 0; i < leaf_node_indices_Thrift.size(); i++) {
        BlockletIndex blockletIndex = getBlockletIndex(leaf_node_indices_Thrift.get(i));
        blockletIndexList.add(blockletIndex);
      }

      List<org.apache.carbondata.format.BlockletInfo> leaf_node_infos_Thrift =
          footer.getBlocklet_info_list();
      List<BlockletInfo> blockletInfoList = new ArrayList<BlockletInfo>();
      for (int i = 0; i < leaf_node_infos_Thrift.size(); i++) {
        BlockletInfo blockletInfo = getBlockletInfo(leaf_node_infos_Thrift.get(i));
        blockletInfo.setBlockletIndex(blockletIndexList.get(i));
        blockletInfoList.add(blockletInfo);
      }
      dataFileFooter.setBlockletList(blockletInfoList);
      dataFileFooter.setBlockletIndex(getBlockletIndexForDataFileFooter(blockletIndexList));
    } finally {
      if (null != fileReader) {
        fileReader.finish();
      }
    }
    return dataFileFooter;
  }

  /**
   * Below method will be used to get blocklet index for data file meta
   *
   * @param blockletIndexList
   * @return blocklet index
   */
  private BlockletIndex getBlockletIndexForDataFileFooter(List<BlockletIndex> blockletIndexList) {
    BlockletIndex blockletIndex = new BlockletIndex();
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStartKey(blockletIndexList.get(0).getBtreeIndex().getStartKey());
    blockletBTreeIndex
        .setEndKey(blockletIndexList.get(blockletIndexList.size() - 1).getBtreeIndex().getEndKey());
    blockletIndex.setBtreeIndex(blockletBTreeIndex);
    byte[][] currentMinValue = blockletIndexList.get(0).getMinMaxIndex().getMinValues().clone();
    byte[][] currentMaxValue = blockletIndexList.get(0).getMinMaxIndex().getMaxValues().clone();
    byte[][] minValue = null;
    byte[][] maxValue = null;
    for (int i = 1; i < blockletIndexList.size(); i++) {
      minValue = blockletIndexList.get(i).getMinMaxIndex().getMinValues();
      maxValue = blockletIndexList.get(i).getMinMaxIndex().getMaxValues();
      for (int j = 0; j < maxValue.length; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMinValue[j], minValue[j]) > 0) {
          currentMinValue[j] = minValue[j].clone();
        }
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMaxValue[j], maxValue[j]) < 0) {
          currentMaxValue[j] = maxValue[j].clone();
        }
      }
    }

    BlockletMinMaxIndex minMax = new BlockletMinMaxIndex();
    minMax.setMaxValues(currentMaxValue);
    minMax.setMinValues(currentMinValue);
    blockletIndex.setMinMaxIndex(minMax);
    return blockletIndex;
  }

  private ColumnSchema thriftColumnSchmeaToWrapperColumnSchema(
      org.apache.carbondata.format.ColumnSchema externalColumnSchema) {
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    wrapperColumnSchema.setColumnUniqueId(externalColumnSchema.getColumn_id());
    wrapperColumnSchema.setColumnName(externalColumnSchema.getColumn_name());
    wrapperColumnSchema.setColumnar(externalColumnSchema.isColumnar());
    wrapperColumnSchema
        .setDataType(thriftDataTyopeToWrapperDataType(externalColumnSchema.data_type));
    wrapperColumnSchema.setDimensionColumn(externalColumnSchema.isDimension());
    List<Encoding> encoders = new ArrayList<Encoding>();
    for (org.apache.carbondata.format.Encoding encoder : externalColumnSchema.getEncoders()) {
      encoders.add(fromExternalToWrapperEncoding(encoder));
    }
    wrapperColumnSchema.setEncodingList(encoders);
    wrapperColumnSchema.setNumberOfChild(externalColumnSchema.getNum_child());
    wrapperColumnSchema.setPrecision(externalColumnSchema.getPrecision());
    wrapperColumnSchema.setColumnGroup(externalColumnSchema.getColumn_group_id());
    wrapperColumnSchema.setScale(externalColumnSchema.getScale());
    wrapperColumnSchema.setDefaultValue(externalColumnSchema.getDefault_value());
    wrapperColumnSchema.setAggregateFunction(externalColumnSchema.getAggregate_function());
    return wrapperColumnSchema;
  }

  /**
   * Below method is to convert the blocklet info of the thrift to wrapper
   * blocklet info
   *
   * @param blockletInfoThrift blocklet info of the thrift
   * @return blocklet info wrapper
   */
  private BlockletInfo getBlockletInfo(
      org.apache.carbondata.format.BlockletInfo blockletInfoThrift) {
    BlockletInfo blockletInfo = new BlockletInfo();
    List<DataChunk> dimensionColumnChunk = new ArrayList<DataChunk>();
    List<DataChunk> measureChunk = new ArrayList<DataChunk>();
    Iterator<org.apache.carbondata.format.DataChunk> column_data_chunksIterator =
        blockletInfoThrift.getColumn_data_chunksIterator();
    if (null != column_data_chunksIterator) {
      while (column_data_chunksIterator.hasNext()) {
        org.apache.carbondata.format.DataChunk next = column_data_chunksIterator.next();
        if (next.isRowMajor()) {
          dimensionColumnChunk.add(getDataChunk(next, false));
        } else if (next.getEncoders().contains(org.apache.carbondata.format.Encoding.DELTA)) {
          measureChunk.add(getDataChunk(next, true));
        } else {
          dimensionColumnChunk.add(getDataChunk(next, false));
        }
      }
    }
    blockletInfo.setDimensionColumnChunk(dimensionColumnChunk);
    blockletInfo.setMeasureColumnChunk(measureChunk);
    blockletInfo.setNumberOfRows(blockletInfoThrift.getNum_rows());
    return blockletInfo;
  }

  /**
   * Below method is convert the thrift encoding to wrapper encoding
   *
   * @param encoderThrift thrift encoding
   * @return wrapper encoding
   */
  private Encoding fromExternalToWrapperEncoding(
      org.apache.carbondata.format.Encoding encoderThrift) {
    switch (encoderThrift) {
      case DICTIONARY:
        return Encoding.DICTIONARY;
      case DELTA:
        return Encoding.DELTA;
      case RLE:
        return Encoding.RLE;
      case INVERTED_INDEX:
        return Encoding.INVERTED_INDEX;
      case BIT_PACKED:
        return Encoding.BIT_PACKED;
      case DIRECT_DICTIONARY:
        return Encoding.DIRECT_DICTIONARY;
      default:
        return Encoding.DICTIONARY;
    }
  }

  /**
   * Below method will be used to convert the thrift compression to wrapper
   * compression codec
   *
   * @param compressionCodecThrift
   * @return wrapper compression codec
   */
  private CompressionCodec getCompressionCodec(
      org.apache.carbondata.format.CompressionCodec compressionCodecThrift) {
    switch (compressionCodecThrift) {
      case SNAPPY:
        return CompressionCodec.SNAPPY;
      default:
        return CompressionCodec.SNAPPY;
    }
  }

  /**
   * Below method will be used to convert thrift segment object to wrapper
   * segment object
   *
   * @param segmentInfo thrift segment info object
   * @return wrapper segment info object
   */
  private SegmentInfo getSegmentInfo(org.apache.carbondata.format.SegmentInfo segmentInfo) {
    SegmentInfo info = new SegmentInfo();
    int[] cardinality = new int[segmentInfo.getColumn_cardinalities().size()];
    for (int i = 0; i < cardinality.length; i++) {
      cardinality[i] = segmentInfo.getColumn_cardinalities().get(i);
    }
    info.setColumnCardinality(cardinality);
    info.setNumberOfColumns(segmentInfo.getNum_cols());
    return info;
  }

  /**
   * Below method will be used to convert the blocklet index of thrift to
   * wrapper
   *
   * @param blockletIndexThrift
   * @return blocklet index wrapper
   */
  private BlockletIndex getBlockletIndex(
      org.apache.carbondata.format.BlockletIndex blockletIndexThrift) {
    org.apache.carbondata.format.BlockletBTreeIndex btreeIndex =
        blockletIndexThrift.getB_tree_index();
    org.apache.carbondata.format.BlockletMinMaxIndex minMaxIndex =
        blockletIndexThrift.getMin_max_index();
    return new BlockletIndex(
        new BlockletBTreeIndex(btreeIndex.getStart_key(), btreeIndex.getEnd_key()),
        new BlockletMinMaxIndex(minMaxIndex.getMin_values(), minMaxIndex.getMax_values()));
  }

  /**
   * Below method will be used to convert the thrift compression meta to
   * wrapper chunk compression meta
   *
   * @param chunkCompressionMetaThrift
   * @return chunkCompressionMetaWrapper
   */
  private ChunkCompressorMeta getChunkCompressionMeta(
      org.apache.carbondata.format.ChunkCompressionMeta chunkCompressionMetaThrift) {
    ChunkCompressorMeta compressorMeta = new ChunkCompressorMeta();
    compressorMeta
        .setCompressor(getCompressionCodec(chunkCompressionMetaThrift.getCompression_codec()));
    compressorMeta.setCompressedSize(chunkCompressionMetaThrift.getTotal_compressed_size());
    compressorMeta.setUncompressedSize(chunkCompressionMetaThrift.getTotal_uncompressed_size());
    return compressorMeta;
  }

  /**
   * Below method will be used to convert the thrift data type to wrapper data
   * type
   *
   * @param dataTypeThrift
   * @return dataType wrapper
   */
  private DataType thriftDataTyopeToWrapperDataType(
      org.apache.carbondata.format.DataType dataTypeThrift) {
    switch (dataTypeThrift) {
      case STRING:
        return DataType.STRING;
      case SHORT:
        return DataType.SHORT;
      case INT:
        return DataType.INT;
      case LONG:
        return DataType.LONG;
      case DOUBLE:
        return DataType.DOUBLE;
      case DECIMAL:
        return DataType.DECIMAL;
      case TIMESTAMP:
        return DataType.TIMESTAMP;
      case ARRAY:
        return DataType.ARRAY;
      case STRUCT:
        return DataType.STRUCT;
      default:
        return DataType.STRING;
    }
  }

  /**
   * Below method will be used to convert the thrift presence meta to wrapper
   * presence meta
   *
   * @param presentMetadataThrift
   * @return wrapper presence meta
   */
  private PresenceMeta getPresenceMeta(
      org.apache.carbondata.format.PresenceMeta presentMetadataThrift) {
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setRepresentNullValues(presentMetadataThrift.isRepresents_presence());
    presenceMeta.setBitSet(BitSet.valueOf(presentMetadataThrift.getPresent_bit_stream()));
    return presenceMeta;
  }

  /**
   * Below method will be used to convert the thrift object to wrapper object
   *
   * @param sortStateThrift
   * @return wrapper sort state object
   */
  private SortState getSortState(org.apache.carbondata.format.SortState sortStateThrift) {
    if (sortStateThrift == org.apache.carbondata.format.SortState.SORT_EXPLICIT) {
      return SortState.SORT_EXPLICT;
    } else if (sortStateThrift == org.apache.carbondata.format.SortState.SORT_NATIVE) {
      return SortState.SORT_NATIVE;
    } else {
      return SortState.SORT_NONE;
    }
  }

  /**
   * Below method will be used to convert the thrift data chunk to wrapper
   * data chunk
   *
   * @param datachunkThrift
   * @return wrapper data chunk
   */
  private DataChunk getDataChunk(org.apache.carbondata.format.DataChunk datachunkThrift,
      boolean isPresenceMetaPresent) {
    DataChunk dataChunk = new DataChunk();
    dataChunk.setColumnUniqueIdList(datachunkThrift.getColumn_ids());
    dataChunk.setDataPageLength(datachunkThrift.getData_page_length());
    dataChunk.setDataPageOffset(datachunkThrift.getData_page_offset());
    if (isPresenceMetaPresent) {
      dataChunk.setNullValueIndexForColumn(getPresenceMeta(datachunkThrift.getPresence()));
    }
    dataChunk.setRlePageLength(datachunkThrift.getRle_page_length());
    dataChunk.setRlePageOffset(datachunkThrift.getRle_page_offset());
    dataChunk.setRowMajor(datachunkThrift.isRowMajor());
    dataChunk.setRowIdPageLength(datachunkThrift.getRowid_page_length());
    dataChunk.setRowIdPageOffset(datachunkThrift.getRowid_page_offset());
    dataChunk.setSortState(getSortState(datachunkThrift.getSort_state()));
    dataChunk.setChunkCompressionMeta(getChunkCompressionMeta(datachunkThrift.getChunk_meta()));
    List<Encoding> encodingList = new ArrayList<Encoding>(datachunkThrift.getEncoders().size());
    for (int i = 0; i < datachunkThrift.getEncoders().size(); i++) {
      encodingList.add(fromExternalToWrapperEncoding(datachunkThrift.getEncoders().get(i)));
    }
    dataChunk.setEncoderList(encodingList);
    if (encodingList.contains(Encoding.DELTA)) {
      List<ByteBuffer> thriftEncoderMeta = datachunkThrift.getEncoder_meta();
      List<ValueEncoderMeta> encodeMetaList =
          new ArrayList<ValueEncoderMeta>(thriftEncoderMeta.size());
      for (int i = 0; i < thriftEncoderMeta.size(); i++) {
        encodeMetaList.add(deserializeEncoderMeta(thriftEncoderMeta.get(i).array()));
      }
      dataChunk.setValueEncoderMeta(encodeMetaList);
    }
    return dataChunk;
  }

  /**
   * Below method will be used to convert the encode metadata to
   * ValueEncoderMeta object
   *
   * @param encoderMeta
   * @return ValueEncoderMeta object
   */
  private ValueEncoderMeta deserializeEncoderMeta(byte[] encoderMeta) {
    // TODO : should remove the unnecessary fields.
    ByteArrayInputStream aos = null;
    ObjectInputStream objStream = null;
    ValueEncoderMeta meta = null;
    try {
      aos = new ByteArrayInputStream(encoderMeta);
      objStream = new ObjectInputStream(aos);
      meta = (ValueEncoderMeta) objStream.readObject();
    } catch (ClassNotFoundException e) {
      LOGGER.error(e);
    } catch (IOException e) {
      CarbonUtil.closeStreams(objStream);
    }
    return meta;
  }
}
