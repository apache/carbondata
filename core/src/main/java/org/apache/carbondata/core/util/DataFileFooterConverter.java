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
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReader;
import org.apache.carbondata.format.FileFooter;

/**
 * Below class will be used to convert the thrift object of data file
 * meta data to wrapper object
 */
public class DataFileFooterConverter extends AbstractDataFileFooterConverter {

  /**
   * Below method will be used to convert thrift file meta to wrapper file meta
   */
  @Override public DataFileFooter readDataFileFooter(TableBlockInfo tableBlockInfo)
      throws IOException {
    DataFileFooter dataFileFooter = new DataFileFooter();
    FileReader fileReader = null;
    try {
      long completeBlockLength = tableBlockInfo.getBlockLength();
      long footerPointer = completeBlockLength - 8;
      fileReader = FileFactory.getFileHolder(FileFactory.getFileType(tableBlockInfo.getFilePath()));
      long actualFooterOffset = fileReader.readLong(tableBlockInfo.getFilePath(), footerPointer);
      CarbonFooterReader reader =
          new CarbonFooterReader(tableBlockInfo.getFilePath(), actualFooterOffset);
      FileFooter footer = reader.readFooter();
      dataFileFooter.setVersionId(ColumnarFormatVersion.valueOf((short) footer.getVersion()));
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

  @Override public List<ColumnSchema> getSchema(TableBlockInfo tableBlockInfo) throws IOException {
    FileReader fileReader = null;
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    try {
      long completeBlockLength = tableBlockInfo.getBlockLength();
      long footerPointer = completeBlockLength - 8;
      fileReader = FileFactory.getFileHolder(FileFactory.getFileType(tableBlockInfo.getFilePath()));
      long actualFooterOffset = fileReader.readLong(tableBlockInfo.getFilePath(), footerPointer);
      CarbonFooterReader reader =
          new CarbonFooterReader(tableBlockInfo.getFilePath(), actualFooterOffset);
      FileFooter footer = reader.readFooter();
      List<org.apache.carbondata.format.ColumnSchema> table_columns = footer.getTable_columns();
      for (int i = 0; i < table_columns.size(); i++) {
        columnSchemaList.add(thriftColumnSchmeaToWrapperColumnSchema(table_columns.get(i)));
      }
    } finally {
      if (null != fileReader) {
        fileReader.finish();
      }
    }
    return columnSchemaList;
  }
}
