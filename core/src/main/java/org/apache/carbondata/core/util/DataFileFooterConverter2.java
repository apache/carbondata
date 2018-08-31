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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReader;
import org.apache.carbondata.format.FileFooter;

import org.apache.hadoop.conf.Configuration;

/**
 * Below class will be used to convert the thrift object of data file
 * meta data to wrapper object for version 2 data file
 */

public class DataFileFooterConverter2 extends AbstractDataFileFooterConverter {

  public DataFileFooterConverter2(Configuration configuration) {
    super(configuration);
  }

  public DataFileFooterConverter2() {
    super(FileFactory.getConfiguration());
  }

  /**
   * Below method will be used to convert thrift file meta to wrapper file meta
   */
  @Override public DataFileFooter readDataFileFooter(TableBlockInfo tableBlockInfo)
      throws IOException {
    DataFileFooter dataFileFooter = new DataFileFooter();
    CarbonFooterReader reader =
        new CarbonFooterReader(tableBlockInfo.getFilePath(), tableBlockInfo.getBlockOffset());
    FileFooter footer = reader.readFooter();
    dataFileFooter.setVersionId(ColumnarFormatVersion.valueOf((short) footer.getVersion()));
    dataFileFooter.setNumberOfRows(footer.getNum_rows());
    dataFileFooter.setSegmentInfo(getSegmentInfo(footer.getSegment_info()));
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    List<org.apache.carbondata.format.ColumnSchema> table_columns = footer.getTable_columns();
    for (int i = 0; i < table_columns.size(); i++) {
      columnSchemaList.add(thriftColumnSchemaToWrapperColumnSchema(table_columns.get(i)));
    }
    dataFileFooter.setColumnInTable(columnSchemaList);

    List<org.apache.carbondata.format.BlockletIndex> leaf_node_indices_Thrift =
        footer.getBlocklet_index_list();
    List<BlockletIndex> blockletIndexList = new ArrayList<BlockletIndex>();
    for (int i = 0; i < leaf_node_indices_Thrift.size(); i++) {
      BlockletIndex blockletIndex = getBlockletIndex(leaf_node_indices_Thrift.get(i));
      blockletIndexList.add(blockletIndex);
    }
    List<org.apache.carbondata.format.BlockletInfo2> leaf_node_infos_Thrift =
        footer.getBlocklet_info_list2();
    List<BlockletInfo> blockletInfoList = new ArrayList<BlockletInfo>();
    for (int i = 0; i < leaf_node_infos_Thrift.size(); i++) {
      BlockletInfo blockletInfo = getBlockletInfo(leaf_node_infos_Thrift.get(i),
          getNumberOfDimensionColumns(columnSchemaList));
      blockletInfo.setBlockletIndex(blockletIndexList.get(i));
      blockletInfoList.add(blockletInfo);
    }
    dataFileFooter.setBlockletList(blockletInfoList);
    dataFileFooter.setBlockletIndex(getBlockletIndexForDataFileFooter(blockletIndexList));
    return dataFileFooter;
  }

  /**
   * Below method is to convert the blocklet info of the thrift to wrapper
   * blocklet info
   *
   * @param blockletInfoThrift blocklet info of the thrift
   * @return blocklet info wrapper
   */
  private BlockletInfo getBlockletInfo(
      org.apache.carbondata.format.BlockletInfo2 blockletInfoThrift, int numberOfDimensionColumns) {
    BlockletInfo blockletInfo = new BlockletInfo();
    List<Long> dimensionColumnChunkOffsets =
        blockletInfoThrift.getColumn_data_chunks_offsets().subList(0, numberOfDimensionColumns);
    List<Long> measureColumnChunksOffsets = blockletInfoThrift.getColumn_data_chunks_offsets()
        .subList(numberOfDimensionColumns,
            blockletInfoThrift.getColumn_data_chunks_offsets().size());
    List<Short> dimensionColumnChunkLength =
        blockletInfoThrift.getColumn_data_chunks_length().subList(0, numberOfDimensionColumns);
    List<Short> measureColumnChunksLength = blockletInfoThrift.getColumn_data_chunks_length()
        .subList(numberOfDimensionColumns,
            blockletInfoThrift.getColumn_data_chunks_offsets().size());
    blockletInfo.setDimensionChunkOffsets(dimensionColumnChunkOffsets);
    blockletInfo.setMeasureChunkOffsets(measureColumnChunksOffsets);

    List<Integer> dimensionColumnChunkLengthInteger = new ArrayList<Integer>();
    List<Integer> measureColumnChunkLengthInteger = new ArrayList<Integer>();
    for (int i = 0; i < dimensionColumnChunkLength.size(); i++) {
      dimensionColumnChunkLengthInteger.add(dimensionColumnChunkLength.get(i).intValue());
    }
    for (int i = 0; i < measureColumnChunksLength.size(); i++) {
      measureColumnChunkLengthInteger.add(measureColumnChunksLength.get(i).intValue());
    }
    blockletInfo.setDimensionChunksLength(dimensionColumnChunkLengthInteger);
    blockletInfo.setMeasureChunksLength(measureColumnChunkLengthInteger);
    blockletInfo.setNumberOfRows(blockletInfoThrift.getNum_rows());
    return blockletInfo;
  }

  /**
   * Below method will be used to get the number of dimension column
   * in carbon column schema
   *
   * @param columnSchemaList column schema list
   * @return number of dimension column
   */
  private int getNumberOfDimensionColumns(List<ColumnSchema> columnSchemaList) {
    int numberOfDimensionColumns = 0;
    int previousColumnGroupId = -1;
    ColumnSchema columnSchema = null;
    for (int i = 0; i < columnSchemaList.size(); i++) {
      columnSchema = columnSchemaList.get(i);
      if (columnSchema.isDimensionColumn()) {
        numberOfDimensionColumns++;
      } else {
        break;
      }
    }
    return numberOfDimensionColumns;
  }

  @Override public List<ColumnSchema> getSchema(TableBlockInfo tableBlockInfo) throws IOException {
    return new DataFileFooterConverter(configuration).getSchema(tableBlockInfo);
  }
}
