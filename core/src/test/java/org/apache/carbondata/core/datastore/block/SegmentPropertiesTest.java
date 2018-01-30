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
package org.apache.carbondata.core.datastore.block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class SegmentPropertiesTest extends TestCase {

  private SegmentProperties blockMetadataInfos;

  @BeforeClass public void setUp() {
    List<ColumnSchema> columnSchema = new ArrayList<ColumnSchema>();
    columnSchema.add(getDimensionColumn1());
    columnSchema.add(getDimensionColumn2());
    columnSchema.add(getDimensionColumn3());
    columnSchema.add(getDimensionColumn4());
    columnSchema.add(getDimensionColumn5());
    columnSchema.add(getDimensionColumn9());
    columnSchema.add(getDimensionColumn10());
    columnSchema.add(getDimensionColumn11());
    columnSchema.add(getDimensionColumn6());
    columnSchema.add(getDimensionColumn7());
    columnSchema.add(getMeasureColumn());
    columnSchema.add(getMeasureColumn1());
    int[] cardinality = new int[columnSchema.size()];
    int x = 100;
    for (int i = 0; i < columnSchema.size(); i++) {
      cardinality[i] = x;
      x++;
    }
    blockMetadataInfos = new SegmentProperties(columnSchema, cardinality);
  }

  @Test public void testBlockMetadataHasProperDimensionCardinality() {
    int[] cardinality = { 100, 102, 103, 105, 106, 107 };
    boolean isProper = true;
    for (int i = 0; i < cardinality.length; i++) {
      isProper = cardinality[i] == blockMetadataInfos.getDimColumnsCardinality()[i];
      if (!isProper) {
        assertTrue(false);
      }
    }
    assertTrue(true);
  }

  @Test public void testBlockMetadataHasProperComplesDimensionCardinality() {
    int[] cardinality = { 108, 109 };
    boolean isProper = true;
    for (int i = 0; i < cardinality.length; i++) {
      isProper = cardinality[i] == blockMetadataInfos.getComplexDimColumnCardinality()[i];
      if (!isProper) {
        assertTrue(false);
      }
    }
    assertTrue(true);
  }

  @Test public void testBlockMetadataHasProperDimensionChunkMapping() {
    Map<Integer, Integer> dimensionOrdinalToBlockMapping = new HashMap<Integer, Integer>();
    dimensionOrdinalToBlockMapping.put(0, 0);
    dimensionOrdinalToBlockMapping.put(1, 1);
    dimensionOrdinalToBlockMapping.put(2, 2);
    dimensionOrdinalToBlockMapping.put(3, 2);
    dimensionOrdinalToBlockMapping.put(4, 3);
    dimensionOrdinalToBlockMapping.put(5, 4);
    dimensionOrdinalToBlockMapping.put(6, 4);
    dimensionOrdinalToBlockMapping.put(7, 4);
    dimensionOrdinalToBlockMapping.put(8, 5);
    dimensionOrdinalToBlockMapping.put(9, 6);
    Map<Integer, Integer> dimensionOrdinalToBlockMappingActual =
        blockMetadataInfos.getDimensionOrdinalToChunkMapping();
    assertEquals(dimensionOrdinalToBlockMapping.size(),
        dimensionOrdinalToBlockMappingActual.size());
    Iterator<Entry<Integer, Integer>> iterator =
        dimensionOrdinalToBlockMapping.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Integer, Integer> next = iterator.next();
      Integer integer = dimensionOrdinalToBlockMappingActual.get(next.getKey());
      if (integer != next.getValue()) {
        assertTrue(false);
      }
    }
    assertTrue(true);
  }

  @Test public void testBlockMetadataHasProperMeasureChunkMapping() {
    Map<Integer, Integer> measureOrdinalToBlockMapping = new HashMap<Integer, Integer>();
    measureOrdinalToBlockMapping.put(0, 0);
    measureOrdinalToBlockMapping.put(1, 1);
    Map<Integer, Integer> measureOrdinalToBlockMappingActual =
        blockMetadataInfos.getMeasuresOrdinalToChunkMapping();
    assertEquals(measureOrdinalToBlockMapping.size(), measureOrdinalToBlockMappingActual.size());
    Iterator<Entry<Integer, Integer>> iterator = measureOrdinalToBlockMapping.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Integer, Integer> next = iterator.next();
      Integer integer = measureOrdinalToBlockMappingActual.get(next.getKey());
      if (integer != next.getValue()) {
        assertTrue(false);
      }
    }
    assertTrue(true);
  }

  @Test public void testNumberOfDimensionsIsCorrect() {
    assertEquals(8, blockMetadataInfos.getDimensions().size());
  }

  @Test public void testNumberOfMeasuesIsCorrect() {
    assertEquals(2, blockMetadataInfos.getMeasures().size());
  }

  @Test public void testNumberOfComplexDimensionIsCorrect() {
    assertEquals(1, blockMetadataInfos.getComplexDimensions().size());
  }

  @Test public void testEachColumnValueSizeHasProperValue() {
    int[] size = { 1, -1, 2, -1, 3 };
    int[] eachDimColumnValueSize = blockMetadataInfos.getEachDimColumnValueSize();
    boolean isEqual = false;
    for (int i = 0; i < size.length; i++) {
      isEqual = size[i] == eachDimColumnValueSize[i];
      if (!isEqual) {
        assertTrue(false);
      }
    }
    assertTrue(true);
  }

  @Test public void testEachComplexColumnValueSizeHasProperValue() {
    int[] size = { 1, 1 };
    int[] eachDimColumnValueSize = blockMetadataInfos.getEachComplexDimColumnValueSize();
    boolean isEqual = false;
    for (int i = 0; i < size.length; i++) {
      isEqual = size[i] == eachDimColumnValueSize[i];
      if (!isEqual) {
        assertTrue(false);
      }
    }
    assertTrue(true);
  }

  private ColumnSchema getDimensionColumn1() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn2() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI1");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn3() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(false);
    dimColumn.setColumnName("IMEI2");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setColumnGroup(0);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn4() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(false);
    dimColumn.setColumnName("IMEI3");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    dimColumn.setColumnGroup(0);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn5() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI4");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn9() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(false);
    dimColumn.setColumnName("IMEI9");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setColumnGroup(1);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn10() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(false);
    dimColumn.setColumnName("IMEI10");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    dimColumn.setColumnGroup(1);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn11() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(false);
    dimColumn.setColumnName("IMEI11");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    dimColumn.setColumnGroup(1);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn6() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI5");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.createDefaultArrayType());
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(1);
    return dimColumn;
  }

  private ColumnSchema getDimensionColumn7() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("IMEI6");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema getMeasureColumn() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI_COUNT");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DELTA);
    dimColumn.setEncodingList(encodeList);
    return dimColumn;
  }

  private ColumnSchema getMeasureColumn1() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI_COUNT1");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataTypes.STRING);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DELTA);
    dimColumn.setEncodingList(encodeList);
    return dimColumn;
  }

}
