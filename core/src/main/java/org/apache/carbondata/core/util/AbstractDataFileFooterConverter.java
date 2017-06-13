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
package org.apache.carbondata.core.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.BlockInfo;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockIndex;

/**
 * Footer reader class
 */
public abstract class AbstractDataFileFooterConverter {

  /**
   * Below method will be used to convert the thrift presence meta to wrapper
   * presence meta
   *
   * @param presentMetadataThrift
   * @return wrapper presence meta
   */
  private static PresenceMeta getPresenceMeta(
      org.apache.carbondata.format.PresenceMeta presentMetadataThrift) {
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setRepresentNullValues(presentMetadataThrift.isRepresents_presence());
    presenceMeta.setBitSet(BitSet.valueOf(presentMetadataThrift.getPresent_bit_stream()));
    return presenceMeta;
  }

  /**
   * Below method will be used to get the index info from index file
   *
   * @param filePath           file path of the index file
   * @param tableBlockInfoList table block index
   * @return list of index info
   * @throws IOException problem while reading the index file
   */
  public List<DataFileFooter> getIndexInfo(String filePath, List<TableBlockInfo> tableBlockInfoList)
      throws IOException {
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
      int index = 0;
      DataFileFooter dataFileFooter = null;
      // read the block info from file
      while (indexReader.hasNext()) {
        BlockIndex readBlockIndexInfo = indexReader.readBlockIndexInfo();
        blockletIndex = getBlockletIndex(readBlockIndexInfo.getBlock_index());
        dataFileFooter = new DataFileFooter();
        TableBlockInfo tableBlockInfo = tableBlockInfoList.get(index);
        if (Integer.parseInt(CarbonTablePath.DataFileUtil.getPartNo(
            tableBlockInfo.getFilePath())) == counter++) {
          tableBlockInfo.setBlockOffset(readBlockIndexInfo.getOffset());
          tableBlockInfo.setVersion(
              ColumnarFormatVersion.valueOf((short) readIndexHeader.getVersion()));
          int blockletSize = getBlockletSize(readBlockIndexInfo);
          tableBlockInfo.getBlockletInfos().setNoOfBlockLets(blockletSize);
          dataFileFooter.setBlockletIndex(blockletIndex);
          dataFileFooter.setColumnInTable(columnSchemaList);
          dataFileFooter.setNumberOfRows(readBlockIndexInfo.getNum_rows());
          dataFileFooter.setBlockInfo(new BlockInfo(tableBlockInfo));
          dataFileFooter.setSegmentInfo(segmentInfo);
          dataFileFooters.add(dataFileFooter);
          if (++index == tableBlockInfoList.size()) {
            break;
          }
        }
      }
    } finally {
      indexReader.closeThriftReader();
    }
    return dataFileFooters;
  }

  /**
   * the methods returns the number of blocklets in a block
   *
   * @param readBlockIndexInfo
   * @return
   */
  protected int getBlockletSize(BlockIndex readBlockIndexInfo) {
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
  public abstract DataFileFooter readDataFileFooter(TableBlockInfo tableBlockInfo)
      throws IOException;

  /**
   * Below method will be used to get blocklet index for data file meta
   *
   * @param blockletIndexList
   * @return blocklet index
   */
  protected BlockletIndex getBlockletIndexForDataFileFooter(List<BlockletIndex> blockletIndexList) {
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

  protected ColumnSchema thriftColumnSchmeaToWrapperColumnSchema(
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
    Map<String, String> properties = externalColumnSchema.getColumnProperties();
    if (properties != null) {
      if (properties.get(CarbonCommonConstants.SORT_COLUMNS) != null) {
        wrapperColumnSchema.setSortColumn(true);
      }
    }
    return wrapperColumnSchema;
  }

  /**
   * Below method is convert the thrift encoding to wrapper encoding
   *
   * @param encoderThrift thrift encoding
   * @return wrapper encoding
   */
  protected Encoding fromExternalToWrapperEncoding(
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
        throw new IllegalArgumentException(encoderThrift.toString() + " is not supported");
    }
  }

  /**
   * Below method will be used to convert thrift segment object to wrapper
   * segment object
   *
   * @param segmentInfo thrift segment info object
   * @return wrapper segment info object
   */
  protected SegmentInfo getSegmentInfo(org.apache.carbondata.format.SegmentInfo segmentInfo) {
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
  protected BlockletIndex getBlockletIndex(
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
   * Below method will be used to convert the thrift data type to wrapper data
   * type
   *
   * @param dataTypeThrift
   * @return dataType wrapper
   */
  protected DataType thriftDataTyopeToWrapperDataType(
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
      case DATE:
        return DataType.DATE;
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
   * Below method will be used to convert the thrift data chunk to wrapper
   * data chunk
   *
   * @param datachunkThrift
   * @return wrapper data chunk
   */
  protected DataChunk getDataChunk(org.apache.carbondata.format.DataChunk datachunkThrift,
      boolean isPresenceMetaPresent) {
    DataChunk dataChunk = new DataChunk();
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
    List<Encoding> encodingList = new ArrayList<Encoding>(datachunkThrift.getEncoders().size());
    for (int i = 0; i < datachunkThrift.getEncoders().size(); i++) {
      encodingList.add(fromExternalToWrapperEncoding(datachunkThrift.getEncoders().get(i)));
    }
    dataChunk.setEncodingList(encodingList);
    if (encodingList.contains(Encoding.DELTA)) {
      List<ByteBuffer> thriftEncoderMeta = datachunkThrift.getEncoder_meta();
      List<ValueEncoderMeta> encodeMetaList =
          new ArrayList<ValueEncoderMeta>(thriftEncoderMeta.size());
      for (int i = 0; i < thriftEncoderMeta.size(); i++) {
        encodeMetaList.add(CarbonUtil.deserializeEncoderMetaV2(thriftEncoderMeta.get(i).array()));
      }
      dataChunk.setValueEncoderMeta(encodeMetaList);
    }
    return dataChunk;
  }

}
