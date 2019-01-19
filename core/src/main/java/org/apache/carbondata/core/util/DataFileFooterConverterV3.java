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
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.FileHeader;

import org.apache.hadoop.conf.Configuration;

public class DataFileFooterConverterV3 extends AbstractDataFileFooterConverter {

  public DataFileFooterConverterV3(Configuration configuration) {
    super(configuration);
  }

  public DataFileFooterConverterV3() {
    super(FileFactory.getConfiguration());
  }

  /**
   * Below method will be used to convert thrift file meta to wrapper file meta
   * This method will read the footer from footer offset present in the data file
   * 1. It will read the header from carbon data file, header starts from 0 offset
   * 2. It will set the stream offset
   * 3. It will read the footer data from file
   * 4. parse the footer to thrift object
   * 5. convert to wrapper object
   *
   * @param tableBlockInfo
   *        table block info
   * @return data file footer
   */
  @Override public DataFileFooter readDataFileFooter(TableBlockInfo tableBlockInfo)
      throws IOException {
    CarbonHeaderReader carbonHeaderReader = new CarbonHeaderReader(tableBlockInfo.getFilePath());
    FileHeader fileHeader = carbonHeaderReader.readHeader();
    CarbonFooterReaderV3 reader =
        new CarbonFooterReaderV3(tableBlockInfo.getFilePath(), tableBlockInfo.getBlockOffset());
    FileFooter3 footer = reader.readFooterVersion3();
    return convertDataFileFooter(fileHeader, footer);
  }

  public DataFileFooter convertDataFileFooter(FileHeader fileHeader, FileFooter3 footer) {
    DataFileFooter dataFileFooter = new DataFileFooter();
    dataFileFooter.setVersionId(ColumnarFormatVersion.valueOf((short) fileHeader.getVersion()));
    dataFileFooter.setNumberOfRows(footer.getNum_rows());
    dataFileFooter.setSegmentInfo(getSegmentInfo(footer.getSegment_info()));
    dataFileFooter.setSchemaUpdatedTimeStamp(fileHeader.getTime_stamp());
    if (footer.isSetIs_sort()) {
      dataFileFooter.setSorted(footer.isIs_sort());
    } else {
      dataFileFooter.setSorted(null);
    }
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    List<org.apache.carbondata.format.ColumnSchema> table_columns = fileHeader.getColumn_schema();
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
    List<org.apache.carbondata.format.BlockletInfo3> leaf_node_infos_Thrift =
        footer.getBlocklet_info_list3();
    List<BlockletInfo> blockletInfoList = new ArrayList<BlockletInfo>();
    for (int i = 0; i < leaf_node_infos_Thrift.size(); i++) {
      BlockletInfo blockletInfo = getBlockletInfo(leaf_node_infos_Thrift.get(i),
          CarbonUtil.getNumberOfDimensionColumns(columnSchemaList));
      blockletInfo.setBlockletIndex(blockletIndexList.get(i));
      blockletInfoList.add(blockletInfo);
    }
    dataFileFooter.setBlockletList(blockletInfoList);
    dataFileFooter.setBlockletIndex(getBlockletIndexForDataFileFooter(blockletIndexList));
    return dataFileFooter;
  }

  @Override public List<ColumnSchema> getSchema(TableBlockInfo tableBlockInfo) throws IOException {
    CarbonHeaderReader carbonHeaderReader = new CarbonHeaderReader(tableBlockInfo.getFilePath());
    FileHeader fileHeader = carbonHeaderReader.readHeader();
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    List<org.apache.carbondata.format.ColumnSchema> table_columns = fileHeader.getColumn_schema();
    for (int i = 0; i < table_columns.size(); i++) {
      columnSchemaList.add(thriftColumnSchemaToWrapperColumnSchema(table_columns.get(i)));
    }
    return columnSchemaList;
  }

  /**
   * Below method is to convert the blocklet info of the thrift to wrapper
   * blocklet info
   *
   * @param blockletInfoThrift blocklet info of the thrift
   * @return blocklet info wrapper
   */
  public BlockletInfo getBlockletInfo(
      org.apache.carbondata.format.BlockletInfo3 blockletInfoThrift, int numberOfDimensionColumns) {
    BlockletInfo blockletInfo = new BlockletInfo();
    List<Long> dimensionColumnChunkOffsets =
        blockletInfoThrift.getColumn_data_chunks_offsets().subList(0, numberOfDimensionColumns);
    List<Long> measureColumnChunksOffsets = blockletInfoThrift.getColumn_data_chunks_offsets()
        .subList(numberOfDimensionColumns,
            blockletInfoThrift.getColumn_data_chunks_offsets().size());
    List<Integer> dimensionColumnChunkLength =
        blockletInfoThrift.getColumn_data_chunks_length().subList(0, numberOfDimensionColumns);
    List<Integer> measureColumnChunksLength = blockletInfoThrift.getColumn_data_chunks_length()
        .subList(numberOfDimensionColumns,
            blockletInfoThrift.getColumn_data_chunks_offsets().size());
    blockletInfo.setDimensionChunkOffsets(dimensionColumnChunkOffsets);
    blockletInfo.setMeasureChunkOffsets(measureColumnChunksOffsets);
    blockletInfo.setDimensionChunksLength(dimensionColumnChunkLength);
    blockletInfo.setMeasureChunksLength(measureColumnChunksLength);
    blockletInfo.setNumberOfRows(blockletInfoThrift.getNum_rows());
    blockletInfo.setDimensionOffset(blockletInfoThrift.getDimension_offsets());
    blockletInfo.setMeasureOffsets(blockletInfoThrift.getMeasure_offsets());
    blockletInfo.setNumberOfPages(blockletInfoThrift.getNumber_number_of_pages());
    return blockletInfo;
  }

}

