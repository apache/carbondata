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

package org.apache.carbondata.core.indexstore.blockletindex;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.row.IndexRow;
import org.apache.carbondata.core.indexstore.row.IndexRowImpl;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.BlockletIndexUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Index implementation for blocklet.
 */
public class BlockletIndex extends BlockIndex implements Serializable {

  private static final long serialVersionUID = -2170289352240810993L;
  // total block number in this index
  private int blockNum = 0;

  @Override
  public void init(IndexModel indexModel) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    super.init(indexModel);
  }

  /**
   * Method to check the cache level and load metadata based on that information
   *
   * @param blockletIndexModel
   * @param indexInfo
   */
  @Override
  protected IndexRowImpl loadMetadata(CarbonRowSchema[] taskSummarySchema,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      SegmentProperties segmentProperties, BlockletIndexModel blockletIndexModel,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      List<DataFileFooter> indexInfo) {
    return loadBlockletMetaInfo(taskSummarySchema, segmentProperties, blockletIndexModel,
        indexInfo);
  }

  @Override
  protected CarbonRowSchema[] getTaskSummarySchema() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    return segmentPropertiesWrapper.getTaskSummarySchemaForBlocklet(false, isFilePathStored);
  }

  @Override
  protected CarbonRowSchema[] getFileFooterEntrySchema() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3482
    return segmentPropertiesWrapper.getBlockletFileFooterEntrySchema();
  }

  /**
   * Method to load blocklet metadata information
   *
   * @param blockletIndexModel
   * @param indexInfo
   */
  private IndexRowImpl loadBlockletMetaInfo(CarbonRowSchema[] taskSummarySchema,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      SegmentProperties segmentProperties, BlockletIndexModel blockletIndexModel,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      List<DataFileFooter> indexInfo) {
    String tempFilePath = null;
    IndexRowImpl summaryRow = null;
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    boolean[] summaryRowMinMaxFlag = new boolean[segmentProperties.getNumberOfColumns()];
    Arrays.fill(summaryRowMinMaxFlag, true);
    // Relative blocklet ID is the id assigned to a blocklet within a part file
    int relativeBlockletId = 0;
    for (DataFileFooter fileFooter : indexInfo) {
      // update the min max flag for summary row
      updateMinMaxFlag(fileFooter, summaryRowMinMaxFlag);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      TableBlockInfo blockInfo = fileFooter.getBlockInfo();
      BlockMetaInfo blockMetaInfo =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
          blockletIndexModel.getBlockMetaInfoMap().get(blockInfo.getFilePath());
      // Here it loads info about all blocklets of index
      // Only add if the file exists physically. There are scenarios which index file exists inside
      // merge index but related carbondata files are deleted. In that case we first check whether
      // the file exists physically or not
      if (blockMetaInfo != null) {
        // this case is for CACHE_LEVEL = BLOCKLET
        // blocklet ID will start from 0 again only when part file path is changed
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2039
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2645
        if (null == tempFilePath || !tempFilePath.equals(blockInfo.getFilePath())) {
          tempFilePath = blockInfo.getFilePath();
          relativeBlockletId = 0;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2929
          blockNum++;
        }
        summaryRow = loadToUnsafe(schema, taskSummarySchema, fileFooter, segmentProperties,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
            getMinMaxCacheColumns(), blockInfo.getFilePath(), summaryRow,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2071
            blockMetaInfo, relativeBlockletId);
        // this is done because relative blocklet id need to be incremented based on the
        // total number of blocklets
        relativeBlockletId += fileFooter.getBlockletList().size();
      }
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3293
    summaryRow.setLong(0L, TASK_ROW_COUNT);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
    setMinMaxFlagForTaskSummary(summaryRow, taskSummarySchema, segmentProperties,
        summaryRowMinMaxFlag);
    return summaryRow;
  }

  private IndexRowImpl loadToUnsafe(CarbonRowSchema[] schema, CarbonRowSchema[] taskSummarySchema,
      DataFileFooter fileFooter, SegmentProperties segmentProperties,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      List<CarbonColumn> minMaxCacheColumns, String filePath, IndexRowImpl summaryRow,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2071
      BlockMetaInfo blockMetaInfo, int relativeBlockletId) {
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    // Add one row to maintain task level min max for segment pruning
    if (!blockletList.isEmpty() && summaryRow == null) {
      summaryRow = new IndexRowImpl(taskSummarySchema);
    }
    for (int index = 0; index < blockletList.size(); index++) {
      IndexRow row = new IndexRowImpl(schema);
      int ordinal = 0;
      int taskMinMaxOrdinal = 1;
      BlockletInfo blockletInfo = blockletList.get(index);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3200
      blockletInfo.setSorted(fileFooter.isSorted());
      BlockletMinMaxIndex minMaxIndex = blockletInfo.getBlockletIndex().getMinMaxIndex();
      // get min max values for columns to be cached
      byte[][] minValuesForColumnsToBeCached = BlockletIndexUtil
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
          .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns,
              minMaxIndex.getMinValues());
      byte[][] maxValuesForColumnsToBeCached = BlockletIndexUtil
          .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns,
              minMaxIndex.getMaxValues());
      boolean[] minMaxFlagValuesForColumnsToBeCached = BlockletIndexUtil
          .getMinMaxFlagValuesForColumnsToBeCached(segmentProperties, minMaxCacheColumns,
              fileFooter.getBlockletIndex().getMinMaxIndex().getIsMinMaxSet());
      row.setRow(addMinMax(schema[ordinal], minValuesForColumnsToBeCached), ordinal);
      // compute and set task level min values
      addTaskMinMaxValues(summaryRow, taskSummarySchema, taskMinMaxOrdinal,
          minValuesForColumnsToBeCached, TASK_MIN_VALUES_INDEX, true);
      ordinal++;
      taskMinMaxOrdinal++;
      row.setRow(addMinMax(schema[ordinal], maxValuesForColumnsToBeCached), ordinal);
      // compute and set task level max values
      addTaskMinMaxValues(summaryRow, taskSummarySchema, taskMinMaxOrdinal,
          maxValuesForColumnsToBeCached, TASK_MAX_VALUES_INDEX, false);
      ordinal++;
      row.setInt(blockletInfo.getNumberOfRows(), ordinal++);
      // add file name
      byte[] filePathBytes =
          CarbonTablePath.getCarbonDataFileName(filePath)
              .getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
      row.setByteArray(filePathBytes, ordinal++);
      // add version number
      row.setShort(fileFooter.getVersionId().number(), ordinal++);
      // add schema updated time
      row.setLong(fileFooter.getSchemaUpdatedTimeStamp(), ordinal++);
      byte[] serializedData;
      try {
        // Add block footer offset, it is used if we need to read footer of block
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
        row.setLong(fileFooter.getBlockInfo().getBlockOffset(), ordinal++);
        setLocations(blockMetaInfo.getLocationInfo(), row, ordinal++);
        // Store block size
        row.setLong(blockMetaInfo.getSize(), ordinal++);
        // add min max flag for all the dimension columns
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3036
        addMinMaxFlagValues(row, schema[ordinal], minMaxFlagValuesForColumnsToBeCached, ordinal);
        ordinal++;
        // add blocklet info
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(stream);
        blockletInfo.write(dataOutput);
        serializedData = stream.toByteArray();
        row.setByteArray(serializedData, ordinal++);
        // add pages
        row.setShort((short) blockletInfo.getNumberOfPages(), ordinal++);
        // for relative blocklet id i.e blocklet id that belongs to a particular carbondata file
        row.setShort((short) relativeBlockletId++, ordinal);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2701
        memoryDMStore.addIndexRow(schema, row);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return summaryRow;
  }

  @Override
  public ExtendedBlocklet getDetailedBlocklet(String blockletId) {
    int absoluteBlockletId = Integer.parseInt(blockletId);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    IndexRow row = memoryDMStore.getIndexRow(getFileFooterEntrySchema(), absoluteBlockletId);
    short relativeBlockletId = row.getShort(BLOCKLET_ID_INDEX);
    String filePath = getFilePath();
    return createBlocklet(row, getFileNameWithFilePath(row, filePath), relativeBlockletId,
        false);
  }

  @Override
  protected short getBlockletId(IndexRow indexRow) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    return indexRow.getShort(BLOCKLET_ID_INDEX);
  }

  protected boolean useMinMaxForExecutorPruning(FilterResolverIntf filterResolverIntf) {
    return BlockletIndexUtil
        .useMinMaxForBlockletPruning(filterResolverIntf, getMinMaxCacheColumns());
  }

  @Override
  protected ExtendedBlocklet createBlocklet(IndexRow row, String fileName, short blockletId,
      boolean useMinMaxForPruning) {
    short versionNumber = row.getShort(VERSION_INDEX);
    ExtendedBlocklet blocklet = new ExtendedBlocklet(fileName, blockletId + "",
        ColumnarFormatVersion.valueOf(versionNumber));
    blocklet.setColumnSchema(getColumnSchema());
    blocklet.setUseMinMaxForPruning(useMinMaxForPruning);
    blocklet.setIsBlockCache(false);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    blocklet.setIndexRow(row);
    return blocklet;
  }

  @Override
  protected short getBlockletNumOfEntry(int index) {
    //in blocklet index, each entry contains info of one blocklet
    return 1;
  }

  @Override
  public int getTotalBlocks() {
    return blockNum;
  }

  @Override
  protected int getTotalBlocklets() {
    return memoryDMStore.getRowCount();
  }

}
