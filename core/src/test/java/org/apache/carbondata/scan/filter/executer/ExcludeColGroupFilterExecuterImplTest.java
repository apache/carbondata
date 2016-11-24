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

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExcludeColGroupFilterExecuterImplTest {

  private ExcludeColGroupFilterExecuterImpl excludeColGroupFilterExecuter;
  private ColumnSchema columnSchema1, columnSchema2, columnSchema3, columnSchema4;
  private SegmentProperties segmentProperties;

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
  }

  @Test public void testGetFilteredIndexes() {

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(Arrays.asList(1, 4, 7));

    CarbonDimension carbonDimension = new CarbonDimension(columnSchema4, 1, 1, 1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(1);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeColGroupFilterExecuter =
        new ExcludeColGroupFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);

    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
    dimensionChunkAttributes.setNoDictionary(false);
    dimensionChunkAttributes.setEachRowSize(1);

    byte[] dataChunk = { 66, 68, 69, 70, 71, 72, 73, 74, 75, 99 };

    ColumnGroupDimensionDataChunk columnGroupDimensionDataChunk =
        new ColumnGroupDimensionDataChunk(dataChunk, dimensionChunkAttributes);

    BitSet result =
        excludeColGroupFilterExecuter.getFilteredIndexes(columnGroupDimensionDataChunk, 10);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testGetFilteredIndexesException() {

    CarbonDimension carbonDimension = new CarbonDimension(columnSchema4, 1, 1, 1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, new DimColumnFilterInfo());
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);

    excludeColGroupFilterExecuter =
        new ExcludeColGroupFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);

    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
    dimensionChunkAttributes.setNoDictionary(false);
    dimensionChunkAttributes.setEachRowSize(1);

    byte[] dataChunk = { 66, 68, 69, 70, 71, 72, 73, 74, 75, 99 };

    ColumnGroupDimensionDataChunk columnGroupDimensionDataChunk =
        new ColumnGroupDimensionDataChunk(dataChunk, dimensionChunkAttributes);

    BitSet result =
        excludeColGroupFilterExecuter.getFilteredIndexes(columnGroupDimensionDataChunk, 10);

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

  @Test public void testIsScanRequired() {
    CarbonDimension carbonDimension = new CarbonDimension(columnSchema4, 1, 1, 1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, new DimColumnFilterInfo());
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);

    excludeColGroupFilterExecuter =
        new ExcludeColGroupFilterExecuterImpl(dimColumnResolvedFilterInfo, segmentProperties);

    BitSet result = excludeColGroupFilterExecuter
        .isScanRequired(new byte[][] { { 96, 11 } }, new byte[][] { { 64, 2 } });

    BitSet expectedResult = new BitSet(1);
    expectedResult.flip(0, 1);

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
    return dimColumn;
  }
}
