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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * It checks if filter is required on given block and if required, it does
 * linear search on block data and set the bitset.
 */
public class ExcludeColGroupFilterExecuterImpl extends ExcludeFilterExecuterImpl {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ExcludeColGroupFilterExecuterImpl.class.getName());

  /**
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   */
  public ExcludeColGroupFilterExecuterImpl(DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties) {
    super(dimColResolvedFilterInfo, null, segmentProperties, false);
  }

  /**
   * It fills BitSet with row index which matches filter key
   */
  protected BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    try {
      KeyStructureInfo keyStructureInfo = getKeyStructureInfo();
      byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      for (int i = 0; i < filterValues.length; i++) {
        byte[] filterVal = filterValues[i];
        for (int rowId = 0; rowId < numerOfRows; rowId++) {
          byte[] colData = new byte[keyStructureInfo.getMaskByteRanges().length];
          dimensionColumnDataChunk.fillChunkData(colData, 0, rowId, keyStructureInfo);
          if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, colData) == 0) {
            bitSet.flip(rowId);
          }
        }
      }

    } catch (KeyGenException e) {
      LOGGER.error(e);
    }

    return bitSet;
  }

  /**
   * It is required for extracting column data from columngroup chunk
   *
   * @return
   * @throws KeyGenException
   */
  private KeyStructureInfo getKeyStructureInfo() throws KeyGenException {
    int colGrpId = getColumnGroupId(dimColEvaluatorInfo.getColumnIndex());
    KeyGenerator keyGenerator = segmentProperties.getColumnGroupAndItsKeygenartor().get(colGrpId);
    List<Integer> mdKeyOrdinal = new ArrayList<Integer>();
    mdKeyOrdinal.add(getMdkeyOrdinal(dimColEvaluatorInfo.getColumnIndex(), colGrpId));
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
    bitSet.flip(0, 1);
    return bitSet;
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