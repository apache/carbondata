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
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.blocklet.BlockletEncodedColumnPage;
import org.apache.carbondata.core.datastore.blocklet.EncodedBlocklet;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.datastore.page.statistics.TablePageStatistics;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletBTreeIndex;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.ChunkCompressionMeta;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.carbondata.format.CompressionCodec;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.format.LocalDictionaryChunk;
import org.apache.carbondata.format.SegmentInfo;

import org.apache.log4j.Logger;

/**
 * Util class to convert to thrift metdata classes
 */
public class CarbonMetadataUtil {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonMetadataUtil.class.getName());

  private CarbonMetadataUtil() {
  }

  /**
   * Below method prepares the file footer object for carbon data file version 3
   *
   * @param infoList
   * @param blockletIndexs
   * @param cardinalities
   * @param numberOfColumns
   * @return FileFooter
   */
  public static FileFooter3 convertFileFooterVersion3(List<BlockletInfo3> infoList,
      List<BlockletIndex> blockletIndexs, int[] cardinalities, int numberOfColumns)
      throws IOException {
    FileFooter3 footer = getFileFooter3(infoList, blockletIndexs, cardinalities, numberOfColumns);
    for (BlockletInfo3 info : infoList) {
      footer.addToBlocklet_info_list3(info);
    }
    return footer;
  }

  /**
   * Below method will be used to get the file footer object
   *
   * @param infoList         blocklet info
   * @param blockletIndexs
   * @param cardinalities    cardinlaity of dimension columns
   * @param numberOfColumns
   * @return file footer
   */
  private static FileFooter3 getFileFooter3(List<BlockletInfo3> infoList,
      List<BlockletIndex> blockletIndexs, int[] cardinalities, int numberOfColumns) {
    SegmentInfo segmentInfo = new SegmentInfo();
    segmentInfo.setNum_cols(numberOfColumns);
    segmentInfo.setColumn_cardinalities(CarbonUtil.convertToIntegerList(cardinalities));
    FileFooter3 footer = new FileFooter3();
    footer.setNum_rows(getNumberOfRowForFooter(infoList));
    footer.setSegment_info(segmentInfo);
    for (BlockletIndex info : blockletIndexs) {
      footer.addToBlocklet_index_list(info);
    }
    return footer;
  }

  /**
   * convert external thrift BlockletMinMaxIndex to BlockletMinMaxIndex of carbon metadata
   */
  public static org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex
      convertExternalMinMaxIndex(BlockletMinMaxIndex minMaxIndex) {
    if (minMaxIndex == null) {
      return null;
    }
    List<Boolean> isMinMaxSet = null;
    if (minMaxIndex.isSetMin_max_presence()) {
      isMinMaxSet = minMaxIndex.getMin_max_presence();
    } else {
      Boolean[] minMaxFlag = new Boolean[minMaxIndex.getMax_values().size()];
      Arrays.fill(minMaxFlag, true);
      isMinMaxSet = Arrays.asList(minMaxFlag);
    }
    return new org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex(
        minMaxIndex.getMin_values(), minMaxIndex.getMax_values(), isMinMaxSet);
  }

  /**
   * convert BlockletMinMaxIndex of carbon metadata to external thrift BlockletMinMaxIndex
   */
  public static BlockletMinMaxIndex convertMinMaxIndex(
      org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex minMaxIndex) {
    if (minMaxIndex == null) {
      return null;
    }

    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();

    for (int i = 0; i < minMaxIndex.getMaxValues().length; i++) {
      blockletMinMaxIndex.addToMax_values(ByteBuffer.wrap(minMaxIndex.getMaxValues()[i]));
      blockletMinMaxIndex.addToMin_values(ByteBuffer.wrap(minMaxIndex.getMinValues()[i]));
      blockletMinMaxIndex.addToMin_max_presence(minMaxIndex.getIsMinMaxSet()[i]);
    }

    return blockletMinMaxIndex;
  }

  public static BlockletIndex getBlockletIndex(
      org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex info) {
    BlockletMinMaxIndex blockletMinMaxIndex = convertMinMaxIndex(info.getMinMaxIndex());
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStart_key(info.getBtreeIndex().getStartKey());
    blockletBTreeIndex.setEnd_key(info.getBtreeIndex().getEndKey());
    BlockletIndex blockletIndex = new BlockletIndex();
    blockletIndex.setMin_max_index(blockletMinMaxIndex);
    blockletIndex.setB_tree_index(blockletBTreeIndex);
    return blockletIndex;
  }

  /**
   * Get total number of rows for the file.
   *
   * @param infoList
   * @return
   */
  private static long getNumberOfRowForFooter(List<BlockletInfo3> infoList) {
    long numberOfRows = 0;
    for (BlockletInfo3 info : infoList) {
      numberOfRows += info.num_rows;
    }
    return numberOfRows;
  }

  private static EncodedColumnPage[] getEncodedColumnPages(EncodedBlocklet encodedBlocklet,
      boolean isDimension, int pageIndex) {
    int size =
        isDimension ? encodedBlocklet.getNumberOfDimension() : encodedBlocklet.getNumberOfMeasure();
    EncodedColumnPage [] encodedPages = new EncodedColumnPage[size];

    for (int i = 0; i < size; i++) {
      if (isDimension) {
        encodedPages[i] =
            encodedBlocklet.getEncodedDimensionColumnPages().get(i).getEncodedColumnPageList()
                .get(pageIndex);
      } else {
        encodedPages[i] =
            encodedBlocklet.getEncodedMeasureColumnPages().get(i).getEncodedColumnPageList()
                .get(pageIndex);
      }
    }
    return encodedPages;
  }
  public static BlockletIndex getBlockletIndex(EncodedBlocklet encodedBlocklet,
      List<CarbonMeasure> carbonMeasureList) {
    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    // merge writeMinMax flag for all the dimensions
    List<Boolean> writeMinMaxFlag =
        mergeWriteMinMaxFlagForAllPages(blockletMinMaxIndex, encodedBlocklet);
    // Calculating min/max for every each column.
    TablePageStatistics stats =
        new TablePageStatistics(getEncodedColumnPages(encodedBlocklet, true, 0),
            getEncodedColumnPages(encodedBlocklet, false, 0));
    byte[][] minCol = stats.getDimensionMinValue().clone();
    byte[][] maxCol = stats.getDimensionMaxValue().clone();
    for (int pageIndex = 0; pageIndex < encodedBlocklet.getNumberOfPages(); pageIndex++) {
      stats = new TablePageStatistics(getEncodedColumnPages(encodedBlocklet, true, pageIndex),
          getEncodedColumnPages(encodedBlocklet, false, pageIndex));
      byte[][] columnMaxData = stats.getDimensionMaxValue();
      byte[][] columnMinData = stats.getDimensionMinValue();
      for (int i = 0; i < maxCol.length; i++) {
        // if writeMonMaxFlag is set to false for the dimension at index i, then update the page
        // and blocklet min/max with empty byte array
        if (!writeMinMaxFlag.get(i)) {
          maxCol[i] = new byte[0];
          minCol[i] = new byte[0];
          continue;
        }
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(columnMaxData[i], maxCol[i]) > 0) {
          maxCol[i] = columnMaxData[i];
        }
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(columnMinData[i], minCol[i]) < 0) {
          minCol[i] = columnMinData[i];
        }
      }
    }
    // Writing min/max to thrift file
    for (byte[] max : maxCol) {
      blockletMinMaxIndex.addToMax_values(ByteBuffer.wrap(max));
    }
    for (byte[] min : minCol) {
      blockletMinMaxIndex.addToMin_values(ByteBuffer.wrap(min));
    }

    stats = new TablePageStatistics(getEncodedColumnPages(encodedBlocklet, true, 0),
        getEncodedColumnPages(encodedBlocklet, false, 0));
    byte[][] measureMaxValue = stats.getMeasureMaxValue().clone();
    byte[][] measureMinValue = stats.getMeasureMinValue().clone();
    byte[] minVal = null;
    byte[] maxVal = null;
    for (int i = 1; i < encodedBlocklet.getNumberOfPages(); i++) {
      for (int j = 0; j < measureMinValue.length; j++) {
        stats = new TablePageStatistics(getEncodedColumnPages(encodedBlocklet, true, i),
            getEncodedColumnPages(encodedBlocklet, false, i));
        minVal = stats.getMeasureMinValue()[j];
        maxVal = stats.getMeasureMaxValue()[j];
        if (compareMeasureData(measureMaxValue[j], maxVal, carbonMeasureList.get(j).getDataType())
            < 0) {
          measureMaxValue[j] = maxVal.clone();
        }
        if (compareMeasureData(measureMinValue[j], minVal, carbonMeasureList.get(j).getDataType())
            > 0) {
          measureMinValue[j] = minVal.clone();
        }
      }
    }

    for (byte[] max : measureMaxValue) {
      blockletMinMaxIndex.addToMax_values(ByteBuffer.wrap(max));
    }
    for (byte[] min : measureMinValue) {
      blockletMinMaxIndex.addToMin_values(ByteBuffer.wrap(min));
    }
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    byte[] startKey = encodedBlocklet.getPageMetadataList().get(0).serializeStartKey();
    blockletBTreeIndex.setStart_key(startKey);
    byte[] endKey =
        encodedBlocklet.getPageMetadataList().get(encodedBlocklet.getPageMetadataList().size() - 1)
            .serializeEndKey();
    blockletBTreeIndex.setEnd_key(endKey);
    BlockletIndex blockletIndex = new BlockletIndex();
    blockletIndex.setMin_max_index(blockletMinMaxIndex);
    blockletIndex.setB_tree_index(blockletBTreeIndex);
    return blockletIndex;
  }

  /**
   * This method will combine the writeMinMax flag from all the pages. If any page for a given
   * dimension has writeMinMax flag set to false then min max for that dimension will nto be
   * written in any of the page and metadata
   *
   * @param blockletMinMaxIndex
   * @param encodedBlocklet
   */
  private static List<Boolean> mergeWriteMinMaxFlagForAllPages(
      BlockletMinMaxIndex blockletMinMaxIndex, EncodedBlocklet encodedBlocklet) {
    Boolean[] mergedWriteMinMaxFlag =
        new Boolean[encodedBlocklet.getNumberOfDimension() + encodedBlocklet.getNumberOfMeasure()];
    // set writeMinMax flag to true for all the columns by default and then update if stats object
    // has the this flag set to false
    Arrays.fill(mergedWriteMinMaxFlag, true);
    for (int i = 0; i < encodedBlocklet.getNumberOfDimension(); i++) {
      for (int pageIndex = 0; pageIndex < encodedBlocklet.getNumberOfPages(); pageIndex++) {
        EncodedColumnPage encodedColumnPage =
            encodedBlocklet.getEncodedDimensionColumnPages().get(i).getEncodedColumnPageList()
                .get(pageIndex);
        SimpleStatsResult stats = encodedColumnPage.getStats();
        if (!stats.writeMinMax()) {
          mergedWriteMinMaxFlag[i] = stats.writeMinMax();
          String columnName = encodedColumnPage.getActualPage().getColumnSpec().getFieldName();
          LOGGER.info("Min Max writing of blocklet ignored for column with name " + columnName);
          break;
        }
      }
    }
    List<Boolean> min_max_presence = Arrays.asList(mergedWriteMinMaxFlag);
    blockletMinMaxIndex.setMin_max_presence(min_max_presence);
    return min_max_presence;
  }

  /**
   * Right now it is set to default values. We may use this in future
   * set the compressor.
   * before 1.5.0, we set a enum 'compression_codec';
   * after 1.5.0, we use string 'compressor_name' instead
   */
  public static ChunkCompressionMeta getChunkCompressorMeta(
      ColumnPage inputPage, long encodedDataLength) throws IOException {
    ChunkCompressionMeta chunkCompressionMeta = new ChunkCompressionMeta();
    // we will not use this field any longer and will use compressor_name instead,
    // but in thrift definition, this field is required so we cannot set it to null, otherwise
    // it will cause deserialization error in runtime (required field cannot be null).
    chunkCompressionMeta.setCompression_codec(CompressionCodec.DEPRECATED);
    chunkCompressionMeta.setCompressor_name(inputPage.getColumnCompressorName());
    chunkCompressionMeta.setTotal_compressed_size(encodedDataLength);
    chunkCompressionMeta.setTotal_uncompressed_size(inputPage.getPageLengthInBytes());
    return chunkCompressionMeta;
  }

  /**
   * get the compressor name from chunk meta
   * before 1.5.0, we only support snappy and do not have compressor_name field;
   * after 1.5.0, we directly get the compressor from the compressor_name field
   */
  public static String getCompressorNameFromChunkMeta(ChunkCompressionMeta chunkCompressionMeta) {
    if (chunkCompressionMeta.isSetCompressor_name()) {
      return chunkCompressionMeta.getCompressor_name();
    } else {
      // this is for legacy store before 1.5.0
      return CompressorFactory.NativeSupportedCompressor.SNAPPY.getName();
    }
  }
  /**
   * Below method will be used to get the index header
   *
   * @param columnCardinality cardinality of each column
   * @param columnSchemaList  list of column present in the table
   * @param bucketNumber
   * @param schemaTimeStamp current timestamp of schema
   * @return Index header object
   */
  public static IndexHeader getIndexHeader(int[] columnCardinality,
      List<ColumnSchema> columnSchemaList, int bucketNumber, long schemaTimeStamp) {
    // create segment info object
    SegmentInfo segmentInfo = new SegmentInfo();
    // set the number of columns
    segmentInfo.setNum_cols(columnSchemaList.size());
    // setting the column cardinality
    segmentInfo.setColumn_cardinalities(CarbonUtil.convertToIntegerList(columnCardinality));
    // create index header object
    IndexHeader indexHeader = new IndexHeader();
    ColumnarFormatVersion version = CarbonProperties.getInstance().getFormatVersion();
    indexHeader.setVersion(version.number());
    // set the segment info
    indexHeader.setSegment_info(segmentInfo);
    // set the column names
    indexHeader.setTable_columns(columnSchemaList);
    // set the bucket number
    indexHeader.setBucket_id(bucketNumber);
    // set the current schema time stamp which will used for deciding the restructured block
    indexHeader.setSchema_time_stamp(schemaTimeStamp);
    return indexHeader;
  }

  /**
   * Below method will be used to get the block index info thrift object for
   * each block present in the segment
   *
   * @param blockIndexInfoList block index info list
   * @return list of block index
   */
  public static List<BlockIndex> getBlockIndexInfo(List<BlockIndexInfo> blockIndexInfoList) {
    List<BlockIndex> thriftBlockIndexList = new ArrayList<BlockIndex>();
    BlockIndex blockIndex = null;
    // below code to create block index info object for each block
    for (BlockIndexInfo blockIndexInfo : blockIndexInfoList) {
      blockIndex = new BlockIndex();
      blockIndex.setNum_rows(blockIndexInfo.getNumberOfRows());
      blockIndex.setOffset(blockIndexInfo.getOffset());
      blockIndex.setFile_name(blockIndexInfo.getFileName());
      blockIndex.setBlock_index(getBlockletIndex(blockIndexInfo.getBlockletIndex()));
      if (blockIndexInfo.getBlockletInfo() != null) {
        blockIndex.setBlocklet_info(getBlocletInfo3(blockIndexInfo.getBlockletInfo()));
      }
      blockIndex.setFile_size(blockIndexInfo.getFileSize());
      thriftBlockIndexList.add(blockIndex);
    }
    return thriftBlockIndexList;
  }

  public static BlockletInfo3 getBlocletInfo3(
      org.apache.carbondata.core.metadata.blocklet.BlockletInfo blockletInfo) {
    List<Long> dimensionChunkOffsets = blockletInfo.getDimensionChunkOffsets();
    dimensionChunkOffsets.addAll(blockletInfo.getMeasureChunkOffsets());
    List<Integer> dimensionChunksLength = blockletInfo.getDimensionChunksLength();
    dimensionChunksLength.addAll(blockletInfo.getMeasureChunksLength());
    BlockletInfo3 blockletInfo3 =
        new BlockletInfo3(blockletInfo.getNumberOfRows(), dimensionChunkOffsets,
            dimensionChunksLength, blockletInfo.getDimensionOffset(),
            blockletInfo.getMeasureOffsets(), blockletInfo.getNumberOfPages());
    List<Integer> rowsPerPage = new ArrayList<>();
    if (null != blockletInfo.getNumberOfRowsPerPage()) {
      for (int i = 0; i < blockletInfo.getNumberOfRowsPerPage().length; i++) {
        rowsPerPage.add(blockletInfo.getNumberOfRowsPerPage()[i]);
      }
      blockletInfo3.setRow_count_in_page(rowsPerPage);
    }
    return blockletInfo3;
  }

  /**
   * return DataChunk3 that contains the input DataChunk2 list
   */
  public static DataChunk3 getDataChunk3(List<DataChunk2> dataChunksList,
      LocalDictionaryChunk encodedDictionary) {
    int offset = 0;
    DataChunk3 dataChunk = new DataChunk3();
    List<Integer> pageOffsets = new ArrayList<>();
    List<Integer> pageLengths = new ArrayList<>();
    int length = 0;
    for (DataChunk2 dataChunk2 : dataChunksList) {
      pageOffsets.add(offset);
      length = dataChunk2.getData_page_length() + dataChunk2.getRle_page_length() +
          dataChunk2.getRowid_page_length();
      pageLengths.add(length);
      offset += length;
    }
    dataChunk.setLocal_dictionary(encodedDictionary);
    dataChunk.setData_chunk_list(dataChunksList);
    dataChunk.setPage_length(pageLengths);
    dataChunk.setPage_offset(pageOffsets);
    return dataChunk;
  }

  /**
   * return DataChunk3 for the dimension column (specifed by `columnIndex`)
   * in `encodedTablePageList`
   */
  public static DataChunk3 getDimensionDataChunk3(EncodedBlocklet encodedBlocklet,
      int columnIndex) {
    List<DataChunk2> dataChunksList = new ArrayList<>();
    BlockletEncodedColumnPage blockletEncodedColumnPage =
        encodedBlocklet.getEncodedDimensionColumnPages().get(columnIndex);
    for (EncodedColumnPage encodedColumnPage : blockletEncodedColumnPage
        .getEncodedColumnPageList()) {
      dataChunksList.add(encodedColumnPage.getPageMetadata());
    }
    return CarbonMetadataUtil
        .getDataChunk3(dataChunksList, blockletEncodedColumnPage.getEncodedDictionary());
  }

  /**
   * return DataChunk3 for the measure column (specifed by `columnIndex`)
   * in `encodedTablePageList`
   */
  public static DataChunk3 getMeasureDataChunk3(EncodedBlocklet encodedBlocklet, int columnIndex) {
    List<DataChunk2> dataChunksList = new ArrayList<>();
    BlockletEncodedColumnPage blockletEncodedColumnPage =
        encodedBlocklet.getEncodedMeasureColumnPages().get(columnIndex);
    for (EncodedColumnPage encodedColumnPage : blockletEncodedColumnPage
        .getEncodedColumnPageList()) {
      dataChunksList.add(encodedColumnPage.getPageMetadata());
    }
    return CarbonMetadataUtil.getDataChunk3(dataChunksList, null);
  }

  private static int compareMeasureData(byte[] first, byte[] second, DataType dataType) {
    ByteBuffer firstBuffer = null;
    ByteBuffer secondBuffer = null;
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      if (first[0] > second[0]) {
        return 1;
      } else if (first[0] < second[0]) {
        return -1;
      }
      return 0;
    } else if (dataType == DataTypes.DOUBLE) {
      double firstValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(first).flip())).getDouble();
      double secondValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(second).flip())).getDouble();
      if (firstValue > secondValue) {
        return 1;
      } else if (firstValue < secondValue) {
        return -1;
      }
      return 0;
    } else if (dataType == DataTypes.FLOAT) {
      float firstValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(first).flip())).getFloat();
      float secondValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(second).flip())).getFloat();
      if (firstValue > secondValue) {
        return 1;
      } else if (firstValue < secondValue) {
        return -1;
      }
      return 0;
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.INT
        || dataType == DataTypes.SHORT) {
      long firstValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(first).flip())).getLong();
      long secondValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(second).flip())).getLong();
      if (firstValue > secondValue) {
        return 1;
      } else if (firstValue < secondValue) {
        return -1;
      }
      return 0;
    } else if (DataTypes.isDecimal(dataType)) {
      return DataTypeUtil.byteToBigDecimal(first).compareTo(DataTypeUtil.byteToBigDecimal(second));
    } else {
      throw new IllegalArgumentException("Invalid data type:" + dataType);
    }
  }

  /**
   * Below method will be used to prepare the file header object for carbondata file
   *
   * @param isFooterPresent  is footer present in carbon data file
   * @param columnSchemaList list of column schema
   * @param schemaUpdatedTimeStamp  schema updated time stamp to be used for restructure scenarios
   * @return file header thrift object
   */
  public static FileHeader getFileHeader(boolean isFooterPresent,
      List<ColumnSchema> columnSchemaList, long schemaUpdatedTimeStamp) {
    FileHeader fileHeader = new FileHeader();
    ColumnarFormatVersion version = CarbonProperties.getInstance().getFormatVersion();
    fileHeader.setIs_footer_present(isFooterPresent);
    fileHeader.setColumn_schema(columnSchemaList);
    fileHeader.setVersion(version.number());
    fileHeader.setTime_stamp(schemaUpdatedTimeStamp);
    return fileHeader;
  }

}
