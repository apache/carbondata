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
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

public class SegmentPropertiesTestUtil {

  public static SegmentProperties getSegmentProperties() {
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
    return new SegmentProperties(columnSchema, cardinality);
  }

  public static ColumnSchema getDimensionColumn1() {
    ColumnSchema dimColumn = new ColumnSchema();
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

  public static ColumnSchema getDimensionColumn2() {
    ColumnSchema dimColumn = new ColumnSchema();
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

  public static ColumnSchema getDimensionColumn3() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI2");
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

  public static ColumnSchema getDimensionColumn4() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI3");
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

  public static ColumnSchema getDimensionColumn5() {
    ColumnSchema dimColumn = new ColumnSchema();
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

  public static ColumnSchema getDimensionColumn9() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI9");
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

  public static ColumnSchema getDimensionColumn10() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI10");
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

  public static ColumnSchema getDimensionColumn11() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("IMEI11");
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

  public static ColumnSchema getDimensionColumn6() {
    ColumnSchema dimColumn = new ColumnSchema();
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

  public static ColumnSchema getDimensionColumn7() {
    ColumnSchema dimColumn = new ColumnSchema();
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

  public static ColumnSchema getMeasureColumn() {
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

  public static ColumnSchema getMeasureColumn1() {
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
