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
package org.apache.carbondata.core.scan.filter.executer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * It checks if filter is required on given block and if required, it does
 * linear search on block data and set the bitset.
 */
public class IncludeColGroupFilterExecuterImpl extends IncludeFilterExecuterImpl {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(IncludeColGroupFilterExecuterImpl.class.getName());

  /**
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   */
  public IncludeColGroupFilterExecuterImpl(DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties) {
    super(dimColResolvedFilterInfo, null, segmentProperties, false);
  }

  /**
   * It fills BitSet with row index which matches filter key
   */
  protected BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);

    try {
      KeyStructureInfo keyStructureInfo = getKeyStructureInfo();
      byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      for (int i = 0; i < filterValues.length; i++) {
        byte[] filterVal = filterValues[i];
        for (int rowId = 0; rowId < numerOfRows; rowId++) {
          byte[] colData = new byte[keyStructureInfo.getMaskByteRanges().length];
          dimensionColumnPage.fillRawData(rowId, 0, colData, keyStructureInfo);
          if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, colData) == 0) {
            bitSet.set(rowId);
          }
        }
      }

    } catch (Exception e) {
      LOGGER.error(e);
    }

    return bitSet;
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws IOException {
    int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
        .get(dimColumnEvaluatorInfo.getColumnIndex());
    if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
      rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
          rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
              rawBlockletColumnChunks.getFileReader(), chunkIndex);
    }
    DimensionRawColumnChunk dimensionRawColumnChunk =
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
    BitSetGroup bitSetGroup = new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
    for (int i = 0; i < dimensionRawColumnChunk.getPagesCount(); i++) {
      if (dimensionRawColumnChunk.getMaxValues() != null) {
        BitSet bitSet = getFilteredIndexes(dimensionRawColumnChunk.decodeColumnPage(i),
            dimensionRawColumnChunk.getRowCount()[i]);
        bitSetGroup.setBitSet(bitSet, i);
      }
    }
    return bitSetGroup;
  }

  /**
   * It is required for extracting column data from columngroup chunk
   *
   * @return
   * @throws KeyGenException
   */
  private KeyStructureInfo getKeyStructureInfo() throws KeyGenException {
    int colGrpId = getColumnGroupId(dimColumnEvaluatorInfo.getColumnIndex());
    KeyGenerator keyGenerator = segmentProperties.getColumnGroupAndItsKeygenartor().get(colGrpId);
    List<Integer> mdKeyOrdinal = new ArrayList<Integer>();
    mdKeyOrdinal.add(getMdkeyOrdinal(dimColumnEvaluatorInfo.getColumnIndex(), colGrpId));
    int[] maskByteRanges = QueryUtil.getMaskedByteRangeBasedOrdinal(mdKeyOrdinal, keyGenerator);
    byte[] maxKey = QueryUtil.getMaxKeyBasedOnOrinal(mdKeyOrdinal, keyGenerator);
    KeyStructureInfo restructureInfos = new KeyStructureInfo();
    restructureInfos.setKeyGenerator(keyGenerator);
    restructureInfos.setMaskByteRanges(maskByteRanges);
    restructureInfos.setMaxKey(maxKey);
    return restructureInfos;
  }

  /**
   * Check if scan is required on given block based on min and max value
   */
  public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    int columnIndex = dimColumnEvaluatorInfo.getColumnIndex();
    int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping().get(columnIndex);
    int[] cols = getAllColumns(columnIndex);
    byte[] maxValue = getMinMaxData(cols, blkMaxVal[chunkIndex], columnIndex);
    byte[] minValue = getMinMaxData(cols, blkMinVal[chunkIndex], columnIndex);
    boolean isScanRequired = false;
    for (int k = 0; k < filterValues.length; k++) {
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      int maxCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], maxValue);
      // and filter-min should be positive
      int minCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], minValue);

      // if any filter value is in range than this block needs to be
      // scanned
      if (maxCompare <= 0 && minCompare >= 0) {
        isScanRequired = true;
        break;
      }
    }
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  /**
   * It extract min and max data for given column from stored min max value
   *
   * @param colGrpColumns
   * @param minMaxData
   * @param columnIndex
   * @return
   */
  private byte[] getMinMaxData(int[] colGrpColumns, byte[] minMaxData, int columnIndex) {
    int startIndex = 0;
    int endIndex = 0;
    if (null != colGrpColumns) {
      for (int i = 0; i < colGrpColumns.length; i++) {
        int colGrpId = getColumnGroupId(colGrpColumns[i]);
        int mdKeyOrdinal = getMdkeyOrdinal(colGrpColumns[i], colGrpId);
        int[] byteRange = getKeyGenerator(colGrpId).getKeyByteOffsets(mdKeyOrdinal);
        int colSize = 0;
        for (int j = byteRange[0]; j <= byteRange[1]; j++) {
          colSize++;
        }
        if (colGrpColumns[i] == columnIndex) {
          endIndex = startIndex + colSize;
          break;
        }
        startIndex += colSize;
      }
    }
    byte[] data = new byte[endIndex - startIndex];
    System.arraycopy(minMaxData, startIndex, data, 0, data.length);
    return data;
  }

  /**
   * It returns column groups which have provided column ordinal
   *
   * @param columnIndex
   * @return column group array
   */
  private int[] getAllColumns(int columnIndex) {
    int[][] colGroups = segmentProperties.getColumnGroups();
    for (int i = 0; i < colGroups.length; i++) {
      if (QueryUtil.searchInArray(colGroups[i], columnIndex)) {
        return colGroups[i];
      }
    }
    return null;
  }

  private int getMdkeyOrdinal(int ordinal, int colGrpId) {
    return segmentProperties.getColumnGroupMdKeyOrdinal(colGrpId, ordinal);
  }

  private int getColumnGroupId(int ordinal) {
    int[][] columnGroups = segmentProperties.getColumnGroups();
    int colGrpId = -1;
    for (int i = 0; i < columnGroups.length; i++) {
      if (columnGroups[i].length > 1) {
        colGrpId++;
        if (QueryUtil.searchInArray(columnGroups[i], ordinal)) {
          break;
        }
      }
    }
    return colGrpId;
  }

  public KeyGenerator getKeyGenerator(int colGrpId) {
    return segmentProperties.getColumnGroupAndItsKeygenartor().get(colGrpId);
  }
}