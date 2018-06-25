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
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.row.DataMapRowImpl;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.indexstore.schema.SchemaGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;

/**
 * Datamap implementation for blocklet.
 */
public class BlockletDataMap extends BlockDataMap implements Serializable {

  private static final long serialVersionUID = -2170289352240810993L;

  @Override public void init(DataMapModel dataMapModel) throws IOException, MemoryException {
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
  protected DataMapRowImpl loadMetadata(BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    if (isLegacyStore) {
      return loadBlockInfoForOldStore(blockletDataMapInfo, indexInfo);
    } else {
      return loadBlockletMetaInfo(blockletDataMapInfo, indexInfo);
    }
  }

  /**
   * Method to create blocklet schema
   *
   * @param segmentProperties
   * @param addToUnsafe
   * @throws MemoryException
   */
  protected void createSchema(SegmentProperties segmentProperties, boolean addToUnsafe)
      throws MemoryException {
    CarbonRowSchema[] schema = SchemaGenerator.createBlockletSchema(segmentProperties);
    memoryDMStore = getMemoryDMStore(schema, addToUnsafe);
  }

  /**
   * Creates the schema to store summary information or the information which can be stored only
   * once per datamap. It stores datamap level max/min of each column and partition information of
   * datamap
   *
   * @param segmentProperties
   * @throws MemoryException
   */
  protected void createSummarySchema(SegmentProperties segmentProperties, byte[] schemaBinary,
      byte[] filePath, byte[] fileName, byte[] segmentId, boolean addToUnsafe)
      throws MemoryException {
    CarbonRowSchema[] taskSummarySchema = SchemaGenerator
        .createTaskSummarySchema(segmentProperties, schemaBinary, filePath, fileName, segmentId,
            false);
    taskSummaryDMStore = getMemoryDMStore(taskSummarySchema, addToUnsafe);
  }

  /**
   * Method to load blocklet metadata information
   *
   * @param blockletDataMapInfo
   * @param indexInfo
   * @throws IOException
   * @throws MemoryException
   */
  private DataMapRowImpl loadBlockletMetaInfo(BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    String tempFilePath = null;
    DataMapRowImpl summaryRow = null;
    // Relative blocklet ID is the id assigned to a blocklet within a part file
    int relativeBlockletId = 0;
    for (DataFileFooter fileFooter : indexInfo) {
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
        }
        summaryRow =
            loadToUnsafe(fileFooter, segmentProperties, blockInfo.getFilePath(), summaryRow,
                blockMetaInfo, relativeBlockletId);
        // this is done because relative blocklet id need to be incremented based on the
        // total number of blocklets
        relativeBlockletId += fileFooter.getBlockletList().size();
      }
    }
    return summaryRow;
  }

  private DataMapRowImpl loadToUnsafe(DataFileFooter fileFooter,
      SegmentProperties segmentProperties, String filePath, DataMapRowImpl summaryRow,
      BlockMetaInfo blockMetaInfo, int relativeBlockletId) {
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    CarbonRowSchema[] schema = memoryDMStore.getSchema();
    // Add one row to maintain task level min max for segment pruning
    if (!blockletList.isEmpty() && summaryRow == null) {
      summaryRow = new DataMapRowImpl(taskSummaryDMStore.getSchema());
    }
    for (int index = 0; index < blockletList.size(); index++) {
      DataMapRow row = new DataMapRowImpl(schema);
      int ordinal = 0;
      int taskMinMaxOrdinal = 0;
      BlockletInfo blockletInfo = blockletList.get(index);
      BlockletMinMaxIndex minMaxIndex = blockletInfo.getBlockletIndex().getMinMaxIndex();
      row.setRow(addMinMax(minMaxLen, schema[ordinal], minMaxIndex.getMinValues()), ordinal);
      // compute and set task level min values
      addTaskMinMaxValues(summaryRow, minMaxLen, taskSummaryDMStore.getSchema(), taskMinMaxOrdinal,
          minMaxIndex.getMinValues(), TASK_MIN_VALUES_INDEX, true);
      ordinal++;
      taskMinMaxOrdinal++;
      row.setRow(addMinMax(minMaxLen, schema[ordinal], minMaxIndex.getMaxValues()), ordinal);
      // compute and set task level max values
      addTaskMinMaxValues(summaryRow, minMaxLen, taskSummaryDMStore.getSchema(), taskMinMaxOrdinal,
          minMaxIndex.getMaxValues(), TASK_MAX_VALUES_INDEX, false);
      ordinal++;
      row.setInt(blockletInfo.getNumberOfRows(), ordinal++);
      // add file path
      byte[] filePathBytes = filePath.getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
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
        memoryDMStore.addIndexRow(row);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return summaryRow;
  }

  public ExtendedBlocklet getDetailedBlocklet(String blockletId) {
    if (isLegacyStore) {
      super.getDetailedBlocklet(blockletId);
    }
    int absoluteBlockletId = Integer.parseInt(blockletId);
    DataMapRow safeRow = memoryDMStore.getDataMapRow(absoluteBlockletId).convertToSafeRow();
    short relativeBlockletId = safeRow.getShort(BLOCKLET_ID_INDEX);
    return createBlocklet(safeRow, relativeBlockletId);
  }

  protected short getBlockletId(DataMapRow dataMapRow) {
    return dataMapRow.getShort(BLOCKLET_ID_INDEX);
  }

  protected ExtendedBlocklet createBlocklet(DataMapRow row, short blockletId) {
    ExtendedBlocklet blocklet = new ExtendedBlocklet(
        new String(row.getByteArray(FILE_PATH_INDEX), CarbonCommonConstants.DEFAULT_CHARSET_CLASS),
        blockletId + "");
    BlockletDetailInfo detailInfo = getBlockletDetailInfo(row, blockletId, blocklet);
    detailInfo.setBlockletInfoBinary(row.getByteArray(BLOCKLET_INFO_INDEX));
    detailInfo.setPagesCount(row.getShort(BLOCKLET_PAGE_COUNT_INDEX));
    blocklet.setDetailInfo(detailInfo);
    return blocklet;
  }

}
