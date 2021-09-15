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
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockIndex;

import org.apache.hadoop.conf.Configuration;

/**
 * Footer reader class
 */
public abstract class AbstractDataFileFooterConverter {

  protected Configuration configuration;

  AbstractDataFileFooterConverter(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Below method will be used to get the index info from index file
   *
   * @param filePath           file path of the index file
   * @param tableBlockInfoList table block index
   * @return list of index info
   * @throws IOException problem while reading the index file
   */
  public List<DataFileFooter> getIndexInfo(String filePath, List<TableBlockInfo>
      tableBlockInfoList)
      throws IOException {
    CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
    List<DataFileFooter> dataFileFooters = new ArrayList<DataFileFooter>();
    try {
      // open the reader
      indexReader.openThriftReader(filePath);
      // get the index header
      org.apache.carbondata.format.IndexHeader readIndexHeader = indexReader.readIndexHeader();
      List<ColumnSchema> columnSchemaList =
          convertColumnSchemaList(readIndexHeader.getTable_columns());
      // get the segment info
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
          dataFileFooter.setBlockletIndex(blockletIndex);
          dataFileFooter.setColumnInTable(columnSchemaList);
          dataFileFooter.setNumberOfRows(readBlockIndexInfo.getNum_rows());
          dataFileFooter.setBlockInfo(tableBlockInfo);
          if (readIndexHeader.isSetIs_sort()) {
            dataFileFooter.setSorted(readIndexHeader.isIs_sort());
          } else {
            if (tableBlockInfo.getVersion() == ColumnarFormatVersion.V3) {
              dataFileFooter.setSorted(null);
            }
          }
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

  protected List<ColumnSchema> convertColumnSchemaList(
      List<org.apache.carbondata.format.ColumnSchema> tableColumns) {
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    for (org.apache.carbondata.format.ColumnSchema tableColumn : tableColumns) {
      columnSchemaList.add(schemaConverter.fromExternalToWrapperColumnSchema(tableColumn));
    }
    return columnSchemaList;
  }

  /**
   * Below method will be used to get the index info from index file
   *
   * @param filePath           file path of the index file
   * @return list of index info
   * @throws IOException problem while reading the index file
   */
  public List<DataFileFooter> getIndexInfo(String filePath, byte[] fileData) throws IOException {
    return getIndexInfo(filePath, fileData, true);
  }

  /**
   * Below method will be used to get the index info from index file
   */
  public List<DataFileFooter> getIndexInfo(String filePath, byte[] fileData,
      boolean isTransactionalTable) throws IOException {
    CarbonIndexFileReader indexReader = new CarbonIndexFileReader(configuration);
    List<DataFileFooter> dataFileFooters = new ArrayList<DataFileFooter>();
    String formattedPath = filePath.replace("\\", "/");
    String parentPath = formattedPath.substring(0, formattedPath.lastIndexOf("/"));
    try {
      // open the reader
      if (fileData != null) {
        indexReader.openThriftReader(fileData);
      } else {
        indexReader.openThriftReader(filePath);
      }
      // get the index header
      org.apache.carbondata.format.IndexHeader readIndexHeader = indexReader.readIndexHeader();
      List<ColumnSchema> columnSchemaList =
          convertColumnSchemaList(readIndexHeader.getTable_columns());
      if (!isTransactionalTable) {
        QueryUtil.updateColumnUniqueIdForNonTransactionTable(columnSchemaList);
      }
      BlockletIndex blockletIndex = null;
      DataFileFooter dataFileFooter = null;
      // read the block info from file
      while (indexReader.hasNext()) {
        BlockIndex readBlockIndexInfo = indexReader.readBlockIndexInfo();
        blockletIndex = getBlockletIndex(readBlockIndexInfo.getBlock_index());
        dataFileFooter = new DataFileFooter();
        TableBlockInfo tableBlockInfo =
            getTableBlockInfo(readBlockIndexInfo, readIndexHeader, parentPath);
        dataFileFooter.setBlockletIndex(blockletIndex);
        dataFileFooter.setColumnInTable(columnSchemaList);
        dataFileFooter.setNumberOfRows(readBlockIndexInfo.getNum_rows());
        dataFileFooter.setBlockInfo(tableBlockInfo);
        dataFileFooter.setVersionId(tableBlockInfo.getVersion());
        // In case of old schema time stamp will not be found in the index header
        if (readIndexHeader.isSetSchema_time_stamp()) {
          dataFileFooter.setSchemaUpdatedTimeStamp(readIndexHeader.getSchema_time_stamp());
        }
        if (readBlockIndexInfo.isSetBlocklet_info()) {
          List<BlockletInfo> blockletInfoList = new ArrayList<BlockletInfo>();
          BlockletInfo blockletInfo = new DataFileFooterConverterV3(configuration)
              .getBlockletInfo(readBlockIndexInfo.getBlocklet_info(),
                  CarbonUtil.getNumberOfDimensionColumns(columnSchemaList));
          blockletInfo.setBlockletIndex(blockletIndex);
          blockletInfoList.add(blockletInfo);
          dataFileFooter.setBlockletList(blockletInfoList);
        }
        dataFileFooters.add(dataFileFooter);
      }
    } finally {
      indexReader.closeThriftReader();
    }
    return dataFileFooters;
  }

  /**
   * This method will create a table block info object from index file info
   *
   * @param readBlockIndexInfo
   * @param readIndexHeader
   * @param parentPath
   * @return
   */
  public TableBlockInfo getTableBlockInfo(BlockIndex readBlockIndexInfo,
      org.apache.carbondata.format.IndexHeader readIndexHeader, String parentPath) {
    TableBlockInfo tableBlockInfo = new TableBlockInfo();
    tableBlockInfo.setBlockOffset(readBlockIndexInfo.getOffset());
    ColumnarFormatVersion version =
        ColumnarFormatVersion.valueOf((short) readIndexHeader.getVersion());
    tableBlockInfo.setVersion(version);
    String fileName = readBlockIndexInfo.file_name;
    // Take only name of file.
    if (fileName.lastIndexOf("/") > 0) {
      fileName = fileName.substring(fileName.lastIndexOf("/"));
    }
    fileName = (CarbonCommonConstants.FILE_SEPARATOR + fileName).replaceAll("//", "/");
    tableBlockInfo.setFilePath(parentPath + fileName);
    if (readBlockIndexInfo.isSetFile_size()) {
      tableBlockInfo.setFileSize(readBlockIndexInfo.getFile_size());
    }
    return tableBlockInfo;
  }

  /**
   * Below method will be used to convert thrift file meta to wrapper file meta
   */
  public abstract DataFileFooter readDataFileFooter(TableBlockInfo tableBlockInfo)
      throws IOException;

  public abstract List<ColumnSchema> getSchema(TableBlockInfo tableBlockInfo) throws IOException;

  /**
   * Below method will be used to get blocklet index for data file meta
   *
   * @param blockletIndexList
   * @return blocklet index
   */
  protected BlockletIndex getBlockletIndexForDataFileFooter(List<BlockletIndex> blockletIndexList,
        List<ColumnSchema> columnSchemaList) {
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
    boolean[] blockletMinMaxFlag = null;
    // flag at block level
    boolean[] blockMinMaxFlag = blockletIndexList.get(0).getMinMaxIndex().getIsMinMaxSet();
    for (int i = 1; i < blockletIndexList.size(); i++) {
      minValue = blockletIndexList.get(i).getMinMaxIndex().getMinValues();
      maxValue = blockletIndexList.get(i).getMinMaxIndex().getMaxValues();
      blockletMinMaxFlag = blockletIndexList.get(i).getMinMaxIndex().getIsMinMaxSet();
      for (int j = 0; j < maxValue.length; j++) {
        // can be null for stores < 1.5.0 version
        if (null != blockletMinMaxFlag && !blockletMinMaxFlag[j]) {
          blockMinMaxFlag[j] = blockletMinMaxFlag[j];
          currentMaxValue[j] = new byte[0];
          currentMinValue[j] = new byte[0];
          continue;
        }
        if (columnSchemaList.get(j).isDimensionColumn()) {
          if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMinValue[j], minValue[j]) > 0) {
            currentMinValue[j] = minValue[j].clone();
          }
          if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMaxValue[j], maxValue[j]) < 0) {
            currentMaxValue[j] = maxValue[j].clone();
          }
        } else {
          if (CarbonUtil.compareMeasureData(currentMinValue[j], minValue[j],
                  columnSchemaList.get(j).getDataType()) > 0) {
            currentMinValue[j] = minValue[j].clone();
          }
          if (CarbonUtil.compareMeasureData(currentMaxValue[j], maxValue[j],
                  columnSchemaList.get(j).getDataType()) < 0) {
            currentMaxValue[j] = maxValue[j].clone();
          }
        }
      }
    }
    if (null == blockMinMaxFlag) {
      blockMinMaxFlag = new boolean[currentMaxValue.length];
      Arrays.fill(blockMinMaxFlag, true);
    }
    BlockletMinMaxIndex minMax = new BlockletMinMaxIndex();
    minMax.setMaxValues(currentMaxValue);
    minMax.setMinValues(currentMinValue);
    minMax.setIsMinMaxSet(blockMinMaxFlag);
    blockletIndex.setMinMaxIndex(minMax);
    return blockletIndex;
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
    List<Boolean> isMinMaxSet = null;
    // Below logic is added to handle backward compatibility
    if (minMaxIndex.isSetMin_max_presence()) {
      isMinMaxSet = minMaxIndex.getMin_max_presence();
    } else {
      Boolean[] minMaxFlag = new Boolean[minMaxIndex.getMax_values().size()];
      Arrays.fill(minMaxFlag, true);
      isMinMaxSet = Arrays.asList(minMaxFlag);
    }
    return new BlockletIndex(
        new BlockletBTreeIndex(btreeIndex.getStart_key(), btreeIndex.getEnd_key()),
        new BlockletMinMaxIndex(minMaxIndex.getMin_values(), minMaxIndex.getMax_values(),
            isMinMaxSet));
  }

}
