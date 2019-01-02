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
package org.apache.carbondata.core.metadata.blocklet;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.datastore.block.BlockInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Information of one data file
 */
public class DataFileFooter implements Serializable {

  /**
   * serialization id
   */
  private static final long serialVersionUID = -7284319972734500751L;

  /**
   * version used for data compatibility
   */
  private ColumnarFormatVersion versionId;

  /**
   * total number of rows in this file
   */
  private long numberOfRows;

  /**
   * Segment info (will be same/repeated for all block in this segment)
   */
  private SegmentInfo segmentInfo;

  /**
   * Information about leaf nodes of all columns in this file
   */
  private List<BlockletInfo> blockletList;

  /**
   * blocklet index of all blocklets in this file
   */
  private BlockletIndex blockletIndex;

  /**
   * Description of columns in this file
   */
  private List<ColumnSchema> columnInTable;

  /**
   * to store the block info detail like file name block index and locations
   */
  private BlockInfo blockInfo;

  /**
   * schema updated time stamp to be used for restructure scenarios
   */
  private long schemaUpdatedTimeStamp;

  /**
   * boolean representing if the file is sorted or not.
   */
  private Boolean isSorted = true;

  /**
   * @return the versionId
   */
  public ColumnarFormatVersion getVersionId() {
    return versionId;
  }

  /**
   * @param versionId the versionId to set
   */
  public void setVersionId(ColumnarFormatVersion versionId) {
    this.versionId = versionId;
  }

  /**
   * @return the numberOfRows
   */
  public long getNumberOfRows() {
    return numberOfRows;
  }

  /**
   * @param numberOfRows the numberOfRows to set
   */
  public void setNumberOfRows(long numberOfRows) {
    this.numberOfRows = numberOfRows;
  }

  /**
   * @return the segmentInfo
   */
  public SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  /**
   * @param segmentInfo the segmentInfo to set
   */
  public void setSegmentInfo(SegmentInfo segmentInfo) {
    this.segmentInfo = segmentInfo;
  }

  /**
   * @return the List of Blocklet
   */
  public List<BlockletInfo> getBlockletList() {
    return blockletList;
  }

  /**
   * @param blockletList the blockletList to set
   */
  public void setBlockletList(List<BlockletInfo> blockletList) {
    this.blockletList = blockletList;
  }

  /**
   * @return the blockletIndex
   */
  public BlockletIndex getBlockletIndex() {
    return blockletIndex;
  }

  /**
   * @param blockletIndex the blockletIndex to set
   */
  public void setBlockletIndex(BlockletIndex blockletIndex) {
    this.blockletIndex = blockletIndex;
  }

  /**
   * @return the columnInTable
   */
  public List<ColumnSchema> getColumnInTable() {
    return columnInTable;
  }

  /**
   * @param columnInTable the columnInTable to set
   */
  public void setColumnInTable(List<ColumnSchema> columnInTable) {
    this.columnInTable = columnInTable;
  }

  /**
   * @return the tableBlockInfo
   */
  public BlockInfo getBlockInfo() {
    return blockInfo;
  }

  /**
   * @param tableBlockInfo the tableBlockInfo to set
   */
  public void setBlockInfo(BlockInfo tableBlockInfo) {
    this.blockInfo = tableBlockInfo;
  }

  public long getSchemaUpdatedTimeStamp() {
    return schemaUpdatedTimeStamp;
  }

  public void setSchemaUpdatedTimeStamp(long schemaUpdatedTimeStamp) {
    this.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
  }

  public void setSorted(Boolean isSorted) {
    this.isSorted = isSorted;
  }

  public Boolean isSorted() {
    return isSorted;
  }
}
