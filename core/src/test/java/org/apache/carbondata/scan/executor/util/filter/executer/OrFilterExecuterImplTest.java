/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.scan.executor.util.filter.executer;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.executer.IncludeFilterExecuterImpl;
import org.apache.carbondata.scan.filter.executer.OrFilterExecuterImpl;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertEquals;

public class OrFilterExecuterImplTest {

  static OrFilterExecuterImpl orFilterExecuter;
  static BlocksChunkHolder blocksChunkHolder;

  @BeforeClass public static void setUp() {
    ColumnSchema columnSchema = new ColumnSchema();
    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    columnSchemaList.add(columnSchema);
    int[] cardinality = { 1, 2, 3, 4, 5 };
    SegmentProperties segmentProperties = new SegmentProperties(columnSchemaList, cardinality);
    blocksChunkHolder = new BlocksChunkHolder(3, 3);
    List<Integer> integerList = new ArrayList<>();
    integerList.add(new Integer("1"));
    integerList.add(new Integer("2"));
    integerList.add(new Integer("3"));
    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setFilterList(integerList);
    CarbonDimension carbonDimension = new CarbonDimension(columnSchema, 1, 2, 3, 4);
    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setColumnIndex(6);
    final int[] arr = { 1, 2, 3, 4, 5, 6 };
    byte val = 6;
    final MultiDimKeyVarLengthEquiSplitGenerator multiDimKeyVarLengthEquiSplitGenerator =
        new MultiDimKeyVarLengthEquiSplitGenerator(arr, val);
    final Map<Integer, Integer> dimensionOrdinal = new HashMap<>();
    Integer i = 1;
    dimensionOrdinal.put(i, i);
    dimensionOrdinal.put(i, i);
    dimensionOrdinal.put(i, i);
    new MockUp<SegmentProperties>() {
      @SuppressWarnings("unused") @Mock public int[] getDimColumnsCardinality() {
        return arr;
      }

      @SuppressWarnings("unused") @Mock public KeyGenerator getDimensionKeyGenerator() {
        return multiDimKeyVarLengthEquiSplitGenerator;
      }

      @SuppressWarnings("unused") @Mock
      public Map<Integer, Integer> getDimensionOrdinalToBlockMapping() {
        return dimensionOrdinal;
      }

    };

    new MockUp<CarbonDimension>() {

      @SuppressWarnings("unused") @Mock public int getKeyOrdinal() {
        return 5;
      }

      @SuppressWarnings("unused") @Mock public boolean hasEncoding(Encoding encoding) {
        return true;
      }
    };
    final BitSet bitSet = new BitSet();
    bitSet.flip(0);
    bitSet.flip(1);
    new MockUp<IncludeFilterExecuterImpl>() {
      @SuppressWarnings("unused") @Mock
      public BitSet applyFilter(BlocksChunkHolder blockChunkHolder) {
        return bitSet;
      }

      @SuppressWarnings("unused") @Mock
      public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
        return bitSet;
      }
    };
    final int[] intArr = { 1, 2, 3, 4, 5 };
    new MockUp<MultiDimKeyVarLengthGenerator>() {
      @SuppressWarnings("unused") @Mock public int[] getKeyByteOffsets(int index) {
        return intArr;
      }

    };
    IncludeFilterExecuterImpl leftExecuter =
        new IncludeFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);
    IncludeFilterExecuterImpl rightExecuter =
        new IncludeFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);
    ;

    orFilterExecuter = new OrFilterExecuterImpl(leftExecuter, rightExecuter);
    final byte[][] byteArr = { { 1, 2 }, { 2, 3 }, { 4, 5 }, { 5, 2 }, { 3, 7 } };
    new MockUp<FilterUtil>() {
      @SuppressWarnings("unused") @Mock
      public byte[][] getKeyArray(DimColumnFilterInfo dimColumnFilterInfo,
          CarbonDimension carbonDimension, SegmentProperties segmentProperties) {
        return byteArr;
      }
    };

  }

  @Test public void testApplyFilter() throws Exception {
    BitSet expected = new BitSet();
    expected.flip(0);
    expected.flip(1);
    BitSet result = orFilterExecuter.applyFilter(blocksChunkHolder);
    assertEquals(expected, result);
  }

  @Test public void testIsScanRequired() {
    byte[][] byteArr = { { 1, 2 }, { 2, 3 }, { 4, 5 }, { 5, 2 }, { 3, 7 } };
    BitSet expected = new BitSet();
    expected.flip(0);
    expected.flip(1);
    BitSet result = orFilterExecuter.isScanRequired(byteArr, byteArr);
    assertEquals(expected, result);
  }

}
