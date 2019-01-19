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
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.row.DataMapRowImpl;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.BlockletDataMapUtil;

/**
 * Datamap implementation for blocklet.
 */
public class BlockletDataMap extends BlockDataMap implements Serializable {

  private static final long serialVersionUID = -2170289352240810993L;
  // total block number in this datamap
  private int blockNum = 0;

  @Override
  public void init(DataMapModel dataMapModel) throws IOException, MemoryException {
    super.init(dataMapModel);
  }

  /**
   * Method to check the cache level and load metadata based on that information
   *
   * @param blockletDataMapInfo
   * @param indexInfo
   * @throws IOException
   * @throws MemoryException
   */
  @Override
  protected DataMapRowImpl loadMetadata(CarbonRowSchema[] taskSummarySchema,
      SegmentProperties segmentProperties, BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    if (isLegacyStore) {
      return loadBlockInfoForOldStore(taskSummarySchema, segmentProperties, blockletDataMapInfo,
          indexInfo);
    } else {
      return loadBlockletMetaInfo(taskSummarySchema, segmentProperties, blockletDataMapInfo,
          indexInfo);
    }
  }

  @Override
  protected CarbonRowSchema[] getTaskSummarySchema() {
    if (isLegacyStore) {
      return super.getTaskSummarySchema();
    }
    SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper segmentPropertiesWrapper =
        SegmentPropertiesAndSchemaHolder.getInstance()
            .getSegmentPropertiesWrapper(segmentPropertiesIndex);
    try {
      return segmentPropertiesWrapper.getTaskSummarySchemaForBlocklet(false, isFilePathStored);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CarbonRowSchema[] getFileFooterEntrySchema() {
    if (isLegacyStore) {
      return super.getFileFooterEntrySchema();
    }
    return SegmentPropertiesAndSchemaHolder.getInstance()
        .getSegmentPropertiesWrapper(segmentPropertiesIndex).getBlockletFileFooterEntrySchema();
  }

  /**
   * Method to load blocklet metadata information
   *
   * @param blockletDataMapInfo
   * @param indexInfo
   * @throws IOException
   * @throws MemoryException
   */
  private DataMapRowImpl loadBlockletMetaInfo(CarbonRowSchema[] taskSummarySchema,
      SegmentProperties segmentProperties, BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    String tempFilePath = null;
    DataMapRowImpl summaryRow = null;
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    boolean[] summaryRowMinMaxFlag = new boolean[segmentProperties.getColumnsValueSize().length];
    Arrays.fill(summaryRowMinMaxFlag, true);
    // Relative blocklet ID is the id assigned to a blocklet within a part file
    int relativeBlockletId = 0;
    for (DataFileFooter fileFooter : indexInfo) {
      // update the min max flag for summary row
      updateMinMaxFlag(fileFooter, summaryRowMinMaxFlag);
      TableBlockInfo blockInfo = fileFooter.getBlockInfo().getTableBlockInfo();
      BlockMetaInfo blockMetaInfo =
          blockletDataMapInfo.getBlockMetaInfoMap().get(blockInfo.getFilePath());
      // Here it loads info about all blocklets of index
      // Only add if the file exists physically. There are scenarios which index file exists inside
      // merge index but related carbondata files are deleted. In that case we first check whether
      // the file exists physically or not
      if (blockMetaInfo != null) {
        // this case is for CACHE_LEVEL = BLOCKLET
        // blocklet ID will start from 0 again only when part file path is changed
        if (null == tempFilePath || !tempFilePath.equals(blockInfo.getFilePath())) {
          tempFilePath = blockInfo.getFilePath();
          relativeBlockletId = 0;
          blockNum++;
        }
        summaryRow = loadToUnsafe(schema, taskSummarySchema, fileFooter, segmentProperties,
            getMinMaxCacheColumns(), blockInfo.getFilePath(), summaryRow,
            blockMetaInfo, relativeBlockletId);
        // this is done because relative blocklet id need to be incremented based on the
        // total number of blocklets
        relativeBlockletId += fileFooter.getBlockletList().size();
      }
    }
    setMinMaxFlagForTaskSummary(summaryRow, taskSummarySchema, segmentProperties,
        summaryRowMinMaxFlag);
    return summaryRow;
  }

  private DataMapRowImpl loadToUnsafe(CarbonRowSchema[] schema, CarbonRowSchema[] taskSummarySchema,
      DataFileFooter fileFooter, SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns, String filePath, DataMapRowImpl summaryRow,
      BlockMetaInfo blockMetaInfo, int relativeBlockletId) {
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    // Add one row to maintain task level min max for segment pruning
    if (!blockletList.isEmpty() && summaryRow == null) {
      summaryRow = new DataMapRowImpl(taskSummarySchema);
    }
    for (int index = 0; index < blockletList.size(); index++) {
      DataMapRow row = new DataMapRowImpl(schema);
      int ordinal = 0;
      int taskMinMaxOrdinal = 0;
      BlockletInfo blockletInfo = blockletList.get(index);
      blockletInfo.setSorted(fileFooter.isSorted());
      BlockletMinMaxIndex minMaxIndex = blockletInfo.getBlockletIndex().getMinMaxIndex();
      // get min max values for columns to be cached
      byte[][] minValuesForColumnsToBeCached = BlockletDataMapUtil
          .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns,
              minMaxIndex.getMinValues());
      byte[][] maxValuesForColumnsToBeCached = BlockletDataMapUtil
          .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns,
              minMaxIndex.getMaxValues());
      boolean[] minMaxFlagValuesForColumnsToBeCached = BlockletDataMapUtil
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
          getFileNameFromPath(filePath).getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
      row.setByteArray(filePathBytes, ordinal++);
      // add version number
      row.setShort(fileFooter.getVersionId().number(), ordinal++);
      // add schema updated time
      row.setLong(fileFooter.getSchemaUpdatedTimeStamp(), ordinal++);
      byte[] serializedData;
      try {
        // Add block footer offset, it is used if we need to read footer of block
        row.setLong(fileFooter.getBlockInfo().getTableBlockInfo().getBlockOffset(), ordinal++);
        setLocations(blockMetaInfo.getLocationInfo(), row, ordinal++);
        // Store block size
        row.setLong(blockMetaInfo.getSize(), ordinal++);
        // add min max flag for all the dimension columns
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
        memoryDMStore.addIndexRow(schema, row);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return summaryRow;
  }

  @Override
  public ExtendedBlocklet getDetailedBlocklet(String blockletId) {
    if (isLegacyStore) {
      return super.getDetailedBlocklet(blockletId);
    }
    int absoluteBlockletId = Integer.parseInt(blockletId);
    DataMapRow safeRow = memoryDMStore.getDataMapRow(getFileFooterEntrySchema(), absoluteBlockletId)
        .convertToSafeRow();
    short relativeBlockletId = safeRow.getShort(BLOCKLET_ID_INDEX);
    String filePath = getFilePath();
    return createBlocklet(safeRow, getFileNameWithFilePath(safeRow, filePath), relativeBlockletId,
        false);
  }

  @Override
  protected short getBlockletId(DataMapRow dataMapRow) {
    if (isLegacyStore) {
      return super.getBlockletId(dataMapRow);
    }
    return dataMapRow.getShort(BLOCKLET_ID_INDEX);
  }

  protected boolean useMinMaxForExecutorPruning(FilterResolverIntf filterResolverIntf) {
    if (isLegacyStore) {
      return super.useMinMaxForExecutorPruning(filterResolverIntf);
    }
    return BlockletDataMapUtil
        .useMinMaxForBlockletPruning(filterResolverIntf, getMinMaxCacheColumns());
  }

  @Override
  protected ExtendedBlocklet createBlocklet(DataMapRow row, String fileName, short blockletId,
      boolean useMinMaxForPruning) {
    if (isLegacyStore) {
      return super.createBlocklet(row, fileName, blockletId, useMinMaxForPruning);
    }
    ExtendedBlocklet blocklet = new ExtendedBlocklet(fileName, blockletId + "");
    BlockletDetailInfo detailInfo = getBlockletDetailInfo(row, blockletId, blocklet);
    detailInfo.setColumnSchemas(getColumnSchema());
    detailInfo.setBlockletInfoBinary(row.getByteArray(BLOCKLET_INFO_INDEX));
    detailInfo.setPagesCount(row.getShort(BLOCKLET_PAGE_COUNT_INDEX));
    detailInfo.setUseMinMaxForPruning(useMinMaxForPruning);
    blocklet.setDetailInfo(detailInfo);
    return blocklet;
  }

  @Override
  protected short getBlockletNumOfEntry(int index) {
    if (isLegacyStore) {
      return super.getBlockletNumOfEntry(index);
    } else {
      //in blocklet datamap, each entry contains info of one blocklet
      return 1;
    }
  }

  @Override public int getTotalBlocks() {
    if (isLegacyStore) {
      return super.getTotalBlocklets();
    } else {
      return blockNum;
    }
  }

  @Override
  protected int getTotalBlocklets() {
    if (isLegacyStore) {
      return super.getTotalBlocklets();
    } else {
      return memoryDMStore.getRowCount();
    }
  }

}
