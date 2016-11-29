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

package org.apache.carbondata.scan.filter.executer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.block.BlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockletBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.ChunkCompressorMeta;
import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.CompressionCodec;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.sort.SortState;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.impl.FileHolderImpl;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IncludeFilterExecuterImplTest {

  private SegmentProperties segmentProperties;
  private ColumnSchema columnSchema1, columnSchema2, columnSchema3, columnSchema4;
  private CarbonDimension carbonDimension;
  private DimColumnFilterInfo dimColumnFilterInfo;
  private DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo;
  private IncludeFilterExecuterImpl includeFilterExecuter;

  @Before
  public void init() {

    List<Encoding> encodeList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);

    columnSchema1 =
        getWrapperDimensionColumn(DataType.INT, "ID", true, Collections.<Encoding>emptyList(),
            false, -1);
    columnSchema2 = getWrapperDimensionColumn(DataType.INT, "salary", true,
        Collections.<Encoding>emptyList(), false, -1);
    columnSchema3 =
        getWrapperDimensionColumn(DataType.STRING, "country", false, encodeList, true, 0);
    columnSchema4 =
        getWrapperDimensionColumn(DataType.STRING, "serialname", false, encodeList, true, 0);

    List<ColumnSchema> wrapperColumnSchema =
        Arrays.asList(columnSchema1, columnSchema2, columnSchema3, columnSchema4);

    segmentProperties = new SegmentProperties(wrapperColumnSchema, new int[] { 3, 11 });

    dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(true);
    dimColumnFilterInfo.setFilterList(Arrays.asList(2));

    carbonDimension = new CarbonDimension(columnSchema3, 0, 0, 0, -1);

    dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setRowIndex(-1);
    dimColumnResolvedFilterInfo.setDimensionExistsInCurrentSilce(true);
    dimColumnResolvedFilterInfo.setNeedCompressedData(false);
    dimColumnResolvedFilterInfo.setDimensionExistsInCurrentSilce(true);
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);

    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    includeFilterExecuter = new IncludeFilterExecuterImpl(dimColumnResolvedFilterInfo,
        segmentProperties);
  }

  @Test
  public void testApplyFilter() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };

    List<DataFileFooter> footerList = getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { null, null });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = includeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);
    expectedResult.flip(1);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(4);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testApplyFilterWithFixedLengthDimensionDataChunkI() {

    new MockUp<CarbonUtil>() {
      @Mock
      public int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
          int low, int high, byte[] compareValue, boolean matchUpLimit){
        return 0;
      }
    };

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };


    List<DataFileFooter> footerList = getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 3 }, attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = includeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);
    expectedResult.flip(1);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(4);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testApplyFilterWithFixedLengthDimensionDataChunkII() {

    new MockUp<CarbonUtil>() {
      @Mock
      public int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
          int low, int high, byte[] compareValue, boolean matchUpLimit){
        return -1;
      }
    };

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };


    List<DataFileFooter> footerList = getDataFileFooterList();

    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 3 }, attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = includeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testApplyFilterWithFixedLengthDimensionDataChunkIII() {

    new MockUp<CarbonUtil>() {
      @Mock
      public int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
          int low, int high, byte[] compareValue, boolean matchUpLimit){
        return 0;
      }
    };

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };


    List<DataFileFooter> footerList = getDataFileFooterList();
    footerList.get(0).setNumberOfRows(0);

    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });
    bTreeBuilderInfo.getFooterList().get(0).getBlockletList().get(0).setNumberOfRows(0);

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 3 }, attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = includeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testApplyFilterWithVariableLengthDimensionDataChunkI() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };


    List<DataFileFooter> footerList = getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    attributes.setInvertedIndexesReverse(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    byte[] data1 = { 99, 104, 105, 110, 97 };
    byte[] data2 = { 117, 115, 97 };

    VariableLengthDimensionDataChunk dataChunk = new VariableLengthDimensionDataChunk(
        Arrays.asList(data1, data1, data1, data1, data1, data1, data1, data1, data1, data2),
        attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = includeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);
    expectedResult.flip(1);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(4);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testApplyFilterWithVariableLengthDimensionDataChunkII() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };


    List<DataFileFooter> footerList = getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);

    byte[] data1 = { 99, 104, 105, 110, 97 };
    byte[] data2 = { 117, 115, 97 };

    VariableLengthDimensionDataChunk dataChunk = new VariableLengthDimensionDataChunk(
        Arrays.asList(data1, data1, data1, data1, data1, data1, data1, data1, data1, data2),
        attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = includeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);
    expectedResult.flip(1);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(4);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testApplyFilterWithVariableLengthDimensionDataChunkIII() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };


    List<DataFileFooter> footerList = getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexesReverse(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    byte[] data1 = { 99, 104, 105, 110, 97 };
    byte[] data2 = { 117, 115, 97 };

    VariableLengthDimensionDataChunk dataChunk = new VariableLengthDimensionDataChunk(
        Arrays.asList(data1, data1, data1, data1, data1, data1, data1, data1, data1, data2),
        attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = includeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);
    expectedResult.flip(1);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(4);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testIsScanRequiredI() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };

    byte[][] blkMaxVal = {{3}, {11}};
    byte[][] blkMinVal = {{2}, {2}};

    BitSet result = includeFilterExecuter.isScanRequired(blkMaxVal, blkMinVal);

    BitSet expectedResult = new BitSet(1);
    expectedResult.set(0);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testIsScanRequiredII() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 1;
      }
    };

    byte[][] blkMaxVal = {{3}, {11}};
    byte[][] blkMinVal = {{2}, {2}};

    BitSet result = includeFilterExecuter.isScanRequired(blkMaxVal, blkMinVal);

    BitSet expectedResult = new BitSet(1);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test
  public void testIsScanRequiredIII() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock
      public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 1;
      }
    };

    dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(true);
    dimColumnFilterInfo.setFilterList(Collections.<Integer>emptyList());

    carbonDimension = new CarbonDimension(columnSchema3, 0, 0, 0, -1);

    dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setRowIndex(-1);
    dimColumnResolvedFilterInfo.setDimensionExistsInCurrentSilce(true);
    dimColumnResolvedFilterInfo.setNeedCompressedData(false);
    dimColumnResolvedFilterInfo.setDimensionExistsInCurrentSilce(true);
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);

    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    includeFilterExecuter = new IncludeFilterExecuterImpl(dimColumnResolvedFilterInfo,
        segmentProperties);

    byte[][] blkMaxVal = {{3}, {11}};
    byte[][] blkMinVal = {{2}, {2}};

    BitSet result = includeFilterExecuter.isScanRequired(blkMaxVal, blkMinVal);

    BitSet expectedResult = new BitSet(1);

    assertThat(result, is(equalTo(expectedResult)));
  }

  private ColumnSchema getWrapperDimensionColumn(DataType dataType, String columnName,
      boolean columnar, List<Encoding> encodeList, boolean dimensionColumn, int columnGroup) {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setDataType(dataType);
    dimColumn.setColumnName(columnName);
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setColumnar(columnar);

    dimColumn.setEncodingList(encodeList);
    dimColumn.setDimensionColumn(dimensionColumn);
    dimColumn.setUseInvertedIndex(true);
    dimColumn.setColumnGroup(columnGroup);
    dimColumn.setColumnProperties(new HashMap<String, String>());
    return dimColumn;
  }

  private DataChunk getDataChunk(int dataPageOffset, int dataLengthPage, int rowIdPageOffset,
      int rowIdPageLength, int rlePageOffset, int rlePageLength, SortState sortState,
      List<Encoding> encodingList) {
    DataChunk dataChunk = new DataChunk();
    ChunkCompressorMeta chunkCompressorMeta = new ChunkCompressorMeta();
    chunkCompressorMeta.setCompressor(CompressionCodec.SNAPPY);

    dataChunk.setChunkCompressionMeta(chunkCompressorMeta);
    dataChunk.setRowMajor(false);
    dataChunk.setColumnUniqueIdList(Collections.EMPTY_LIST);
    dataChunk.setDataPageOffset(dataPageOffset);
    dataChunk.setDataPageLength(dataLengthPage);
    dataChunk.setRowIdPageOffset(rowIdPageOffset);
    dataChunk.setRowIdPageLength(rowIdPageLength);
    dataChunk.setRlePageOffset(rlePageOffset);
    dataChunk.setRlePageLength(rlePageLength);
    dataChunk.setRleApplied(false);
    dataChunk.setSortState(sortState);
    dataChunk.setEncoderList(encodingList);
    dataChunk.setNoDictonaryColumn(false);

    BitSet bitSet = new BitSet(1);
    bitSet.flip(1);

    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setBitSet(bitSet);
    presenceMeta.setRepresentNullValues(false);

    dataChunk.setNullValueIndexForColumn(presenceMeta);

    return dataChunk;
  }

  private List<DataFileFooter> getDataFileFooterList() {
    DataFileFooter fileFooter = new DataFileFooter();

    fileFooter.setVersionId(1);
    fileFooter.setNumberOfRows(10);

    SegmentInfo segmentInfo = new SegmentInfo();
    segmentInfo.setNumberOfColumns(4);
    segmentInfo.setColumnCardinality(new int[] { 3, 11 });
    fileFooter.setSegmentInfo(segmentInfo);

    BlockletIndex blockletIndex = new BlockletIndex();
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStartKey(new byte[] { 0, 0, 0, 2, 0, 0, 0, 0, 2, 2 });
    blockletBTreeIndex.setEndKey(new byte[] { 0, 0, 0, 2, 0, 0, 0, 0, 3, 3 });
    blockletIndex.setBtreeIndex(blockletBTreeIndex);

    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    blockletMinMaxIndex.setMaxValues(new byte[][] { { 3 }, { 11 } });
    blockletMinMaxIndex.setMinValues(new byte[][] { { 2 }, { 2 } });
    blockletIndex.setMinMaxIndex(blockletMinMaxIndex);

    fileFooter.setBlockletIndex(blockletIndex);
    fileFooter.setColumnInTable(
        Arrays.asList(columnSchema1, columnSchema2, columnSchema3, columnSchema4));

    DataChunk dimenssionDataChunk1, dimenssionDataChunk2, measureDataChunk1, measureDataChunk2;
    dimenssionDataChunk1 = getDataChunk(0, 4, 0, 0, 136, 9, SortState.SORT_EXPLICT,
        Arrays.asList(Encoding.DICTIONARY, Encoding.RLE));
    dimenssionDataChunk2 = getDataChunk(4, 12, 120, 16, 145, 0, SortState.SORT_NATIVE,
        Arrays.asList(Encoding.DICTIONARY, Encoding.RLE, Encoding.INVERTED_INDEX));
    measureDataChunk1 =
        getDataChunk(16, 52, 0, 0, 0, 0, SortState.SORT_NONE, Arrays.asList(Encoding.DELTA));

    ValueEncoderMeta measureMeta1 = new ValueEncoderMeta();
    measureMeta1.setMaxValue(10l);
    measureMeta1.setMinValue(1l);
    measureMeta1.setUniqueValue(0l);
    measureMeta1.setDecimal(1);
    measureMeta1.setDataTypeSelected((byte) 0);
    measureMeta1.setType('l');
    measureDataChunk1.setValueEncoderMeta(Arrays.asList(measureMeta1));

    measureDataChunk2 =
        getDataChunk(68, 52, 0, 0, 0, 0, SortState.SORT_NONE, Arrays.asList(Encoding.DELTA));

    ValueEncoderMeta measureMeta2 = new ValueEncoderMeta();
    measureMeta2.setMaxValue(15009l);
    measureMeta2.setMinValue(15000l);
    measureMeta2.setUniqueValue(14999l);
    measureMeta2.setDecimal(1);
    measureMeta2.setDataTypeSelected((byte) 0);
    measureMeta2.setType('l');
    measureDataChunk2.setValueEncoderMeta(Arrays.asList(measureMeta2));

    BlockletInfo blockletInfo = new BlockletInfo();
    blockletInfo.setDimensionColumnChunk(Arrays.asList(dimenssionDataChunk1, dimenssionDataChunk2));
    blockletInfo.setMeasureColumnChunk(Arrays.asList(measureDataChunk1, measureDataChunk2));
    blockletInfo.setNumberOfRows(10);
    blockletInfo.setBlockletIndex(blockletIndex);

    new MockUp<DataFileFooter>() {

      @Mock public BlockInfo getBlockInfo() {

        final String filePath =
            this.getClass().getClassLoader().getResource("include.carbondata").getPath();
        return new BlockInfo(
            new TableBlockInfo(filePath, 0, "0", new String[] { "localhost" }, 1324));
      }
    };

    fileFooter.setBlockletList(Arrays.asList(blockletInfo));

    return Arrays.asList(fileFooter);
  }
}
