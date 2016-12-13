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

import org.apache.carbondata.core.carbon.ColumnarFormatVersion;
import org.apache.carbondata.core.carbon.datastore.block.BlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.dimension.v1.CompressedDimensionChunkFileBasedReaderV1;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.measure.v1.CompressedMeasureChunkFileBasedReaderV1;
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
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IncludeColGroupFilterExecuterImplTest {

  private SegmentProperties segmentProperties;
  private ColumnSchema columnSchema1, columnSchema2, columnSchema3, columnSchema4;
  private CarbonDimension carbonDimension;
  private DimColumnFilterInfo dimColumnFilterInfo;
  private DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo;
  private IncludeColGroupFilterExecuterImpl includeFilterExecuter;

  @Before public void init() {

    List<Encoding> encodeList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);

    columnSchema1 =
        getWrapperDimensionColumn(DataType.INT, "ID", true, Collections.<Encoding>emptyList(),
            false, -1);
    columnSchema2 =
        getWrapperDimensionColumn(DataType.INT, "salary", true, Collections.<Encoding>emptyList(),
            false, -1);
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

    includeFilterExecuter =
        new IncludeColGroupFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);
  }

  @Test public void testIsScanRequiredI() {
    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };

    byte[][] blkMaxVal = { { 3 }, { 11 } };
    byte[][] blkMinVal = { { 2 }, { 2 } };

    BitSet result = includeFilterExecuter.isScanRequired(blkMaxVal, blkMinVal);

    BitSet expectedResult = new BitSet(1);
    expectedResult.set(0);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testIsScanRequiredII() {
    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
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

    includeFilterExecuter =
        new IncludeColGroupFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);

    byte[][] blkMaxVal = { { 3 }, { 11 } };
    byte[][] blkMinVal = { { 2 }, { 2 } };

    BitSet result = includeFilterExecuter.isScanRequired(blkMaxVal, blkMinVal);

    BitSet expectedResult = new BitSet(1);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testIsScanRequiredIII() {
    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 1;
      }
    };

    byte[][] blkMaxVal = { { 3 }, { 11 } };
    byte[][] blkMinVal = { { 2 }, { 2 } };

    BitSet result = includeFilterExecuter.isScanRequired(blkMaxVal, blkMinVal);

    BitSet expectedResult = new BitSet(1);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGetFilteredIndexesI() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };

    new MockUp<SegmentProperties>() {
      @Mock public int getColumnGroupMdKeyOrdinal(int colGrpId, int ordinal) {
        return 0;
      }
    };

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(false);
    attributes.setEachRowSize(1);

    ColumnGroupDimensionDataChunk dataChunk =
        new ColumnGroupDimensionDataChunk(new byte[] { 66, 68, 69, 70, 71, 72, 73, 74, 75, 99 },
            attributes);

    BitSet result = includeFilterExecuter.getFilteredIndexes(dataChunk, 10);

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

  @Test public void testGetFilteredIndexesII() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 1;
      }
    };

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(false);
    attributes.setEachRowSize(1);

    ColumnGroupDimensionDataChunk dataChunk =
        new ColumnGroupDimensionDataChunk(new byte[] { 66, 68, 69, 70, 71, 72, 73, 74, 75, 99 },
            attributes);

    BitSet result = includeFilterExecuter.getFilteredIndexes(dataChunk, 10);

    BitSet expectedResult = new BitSet();

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGetFilteredIndexesWithException() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };

    BitSet result = includeFilterExecuter.getFilteredIndexes(null, 10);

    BitSet expectedResult = new BitSet();

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

    fileFooter.setVersionId(ColumnarFormatVersion.V1);
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
    measureMeta1.setDataTypeSelected((byte) 0);
    measureMeta1.setType('l');
    measureDataChunk1.setValueEncoderMeta(Arrays.asList(measureMeta1));

    measureDataChunk2 =
        getDataChunk(68, 52, 0, 0, 0, 0, SortState.SORT_NONE, Arrays.asList(Encoding.DELTA));

    ValueEncoderMeta measureMeta2 = new ValueEncoderMeta();
    measureMeta2.setMaxValue(15009l);
    measureMeta2.setMinValue(15000l);
    measureMeta2.setUniqueValue(14999l);
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

        return new BlockInfo(new TableBlockInfo("file", 0, "0", new String[] { "localhost" }, 1324,
            ColumnarFormatVersion.V1));
      }
    };

    new MockUp<CompressedDimensionChunkFileBasedReaderV1>() {
      @Mock
      public DimensionColumnDataChunk readDimensionChunk(FileHolder fileReader, int blockIndex) {
        DimensionChunkAttributes chunkAttributes = new DimensionChunkAttributes();
        chunkAttributes.setEachRowSize(1);
        byte[] dataChunk = { 2, 2, 2, 2, 2, 2, 2, 2, 2, 3 };
        return new FixedLengthDimensionDataChunk(dataChunk, chunkAttributes);
      }
    };

    new MockUp<CompressedMeasureChunkFileBasedReaderV1>() {
      @Mock public MeasureColumnDataChunk readMeasureChunk(final FileHolder fileReader,
          final int blockIndex) {
        CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
        dataHolder.setReadableDoubleValues(
            new double[] { 7.414E-320, 7.412E-320, 7.4115E-320, 7.4144E-320, 7.4135E-320,
                7.4125E-320, 7.411E-320, 7.415E-320, 7.413E-320, 7.4154E-320 });

        PresenceMeta presenceMeta = new PresenceMeta();
        presenceMeta.setRepresentNullValues(false);
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        presenceMeta.setBitSet(bitSet);

        MeasureColumnDataChunk dataChunk = new MeasureColumnDataChunk();
        dataChunk.setMeasureDataHolder(dataHolder);
        dataChunk.setNullValueIndexHolder(presenceMeta);
        return dataChunk;
      }
    };

    fileFooter.setBlockletList(Arrays.asList(blockletInfo));

    return Arrays.asList(fileFooter);
  }
}
