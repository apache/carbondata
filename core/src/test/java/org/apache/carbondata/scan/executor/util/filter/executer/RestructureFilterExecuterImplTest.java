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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.executer.RestructureFilterExecuterImpl;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;

public class RestructureFilterExecuterImplTest {
  static BlocksChunkHolder blocksChunkHolder;
  static BTreeBuilderInfo bTreeBuilderInfo;
  static BlockBTreeLeafNode blockBTreeLeafNode;
  static RestructureFilterExecuterImpl restructureFilterExecuter;

  @BeforeClass public static void setUp() {
    byte[] bytArr = { 1, 2, 3, 4, 5, 6 };
    byte[] bytArr1 = { 8, 8, 9, 6, 5, 6 };
    byte[] startKey = { 1, 2, 3, 4, 5, 6 };
    byte[] endKey = { 1, 2, 3, 4, 5, 6 };
    int[] intArr = { 1, 2, 3, 4, 5, 6 };

    List<ByteBuffer> minValues = new ArrayList<>();
    minValues.add(ByteBuffer.wrap(bytArr));
    minValues.add(ByteBuffer.wrap(bytArr));

    List<ByteBuffer> maxValues = new ArrayList<>();
    maxValues.add(ByteBuffer.wrap(bytArr1));
    maxValues.add(ByteBuffer.wrap(bytArr1));

    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex(minValues, maxValues);
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex(startKey, endKey);
    BlockletIndex blockletIndex = new BlockletIndex(blockletBTreeIndex, blockletMinMaxIndex);

    DataFileFooter dataFileFooter = new DataFileFooter();
    dataFileFooter.setBlockletIndex(blockletIndex);
    final List<DataFileFooter> dataFileFooterList = new ArrayList<>();
    dataFileFooterList.add(dataFileFooter);
    dataFileFooterList.add(dataFileFooter);
    bTreeBuilderInfo = new BTreeBuilderInfo(dataFileFooterList, intArr);

    new MockUp<FilterUtil>() {
      byte[][] byteArr = { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 4, 5 }, { 5, 6 }, { 6, 7 } };

      @SuppressWarnings("unused") @Mock
      public byte[][] getKeyArray(DimColumnFilterInfo dimColumnFilterInfo,
          CarbonDimension carbonDimension, SegmentProperties segmentProperties) {
        return byteArr;
      }
    };

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    ColumnSchema columnSchema = new ColumnSchema();
    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    columnSchemaList.add(columnSchema);
    int[] cardinality = { 1, 2, 3, 4, 5, 6 };

    SegmentProperties segmentProperties = new SegmentProperties(columnSchemaList, cardinality);

    restructureFilterExecuter =
        new RestructureFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);
    blockBTreeLeafNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 1, 1);

  }

  @Test public void testApplyFilter() {
    blocksChunkHolder = new BlocksChunkHolder(4, 4);
    blocksChunkHolder.setDataBlock(blockBTreeLeafNode);
    BitSet result = restructureFilterExecuter.applyFilter(blocksChunkHolder);
    assertFalse(result.get(1));
  }

  @Test public void testIsScanRequired() {
    byte[][] maxValue = { { 32, 22 }, { 12, 13 }, { 13, 14 }, { 14, 15 }, { 15, 16 }, { 61, 71 } };
    byte[][] minValue = { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 4, 5 }, { 5, 6 }, { 6, 7 } };
    BitSet expected = new BitSet();
    expected.flip(0);
    BitSet result = restructureFilterExecuter.isScanRequired(maxValue, minValue);
    assertEquals(result, expected);
  }

}
