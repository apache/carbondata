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

package org.apache.carbondata.core.metadata.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ThriftWrapperSchemaConverterImplTest {

  private static ThriftWrapperSchemaConverterImpl thriftWrapperSchemaConverter = null;
  private static org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolEntry = null;
  private static org.apache.carbondata.format.ColumnSchema thriftColumnSchema = null;
  private static List<ColumnSchema> columnSchemas = null;
  private static List<SchemaEvolutionEntry> schemaEvolutionEntries = null;
  private static List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchemas = null;
  private static List<org.apache.carbondata.format.SchemaEvolutionEntry>
      thriftSchemaEvolutionEntries = null;
  private static org.apache.carbondata.format.SchemaEvolution schemaEvol = null;
  private static List<org.apache.carbondata.format.Encoding> encoders = null;
  private static List<Encoding> encodings = null;
  private static Map columnPropertyMap = null;
  private static org.apache.carbondata.format.TableSchema tabSchema = null;

  @BeforeClass public static void setUp() {

    thriftWrapperSchemaConverter = new ThriftWrapperSchemaConverterImpl();
    schemaEvolEntry = new org.apache.carbondata.format.SchemaEvolutionEntry();
    schemaEvolutionEntries = new ArrayList();
    schemaEvolutionEntries.add(new SchemaEvolutionEntry());
    columnSchemas = new ArrayList();
    columnSchemas.add(new ColumnSchema());
    encodings = new ArrayList<>();
    encodings.add(Encoding.INVERTED_INDEX);
    encodings.add(Encoding.DELTA);
    encodings.add(Encoding.BIT_PACKED);
    encodings.add(Encoding.DICTIONARY);
    encodings.add(Encoding.RLE);
    encodings.add(Encoding.DIRECT_DICTIONARY);
    encoders = new ArrayList<org.apache.carbondata.format.Encoding>();
    encoders.add(org.apache.carbondata.format.Encoding.INVERTED_INDEX);
    encoders.add(org.apache.carbondata.format.Encoding.DELTA);
    encoders.add(org.apache.carbondata.format.Encoding.BIT_PACKED);
    encoders.add(org.apache.carbondata.format.Encoding.DICTIONARY);
    encoders.add(org.apache.carbondata.format.Encoding.RLE);
    encoders.add(org.apache.carbondata.format.Encoding.DIRECT_DICTIONARY);

    columnPropertyMap = new HashMap<String, String>();
    columnPropertyMap.put("property", "value");
    thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.STRING,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    thriftColumnSchemas = new ArrayList<org.apache.carbondata.format.ColumnSchema>();
    thriftColumnSchemas.add(thriftColumnSchema);
    thriftSchemaEvolutionEntries = new ArrayList<>();
    thriftSchemaEvolutionEntries.add(schemaEvolEntry);
    schemaEvol = new org.apache.carbondata.format.SchemaEvolution(thriftSchemaEvolutionEntries);

    new MockUp<SchemaEvolution>() {
      @Mock public List<SchemaEvolutionEntry> getSchemaEvolutionEntryList() {
        return schemaEvolutionEntries;
      }
    };

    new MockUp<org.apache.carbondata.format.SchemaEvolutionEntry>() {
      @Mock public org.apache.carbondata.format.SchemaEvolutionEntry setAdded(
          List<org.apache.carbondata.format.ColumnSchema> added) {
        return schemaEvolEntry;
      }

      @Mock public org.apache.carbondata.format.SchemaEvolutionEntry setRemoved(
          List<org.apache.carbondata.format.ColumnSchema> removed) {
        return schemaEvolEntry;
      }

      @Mock public long getTime_stamp() {
        return 1112745600000L;
      }

      @Mock public List<org.apache.carbondata.format.ColumnSchema> getAdded() {
        return thriftColumnSchemas;
      }

      @Mock public List<org.apache.carbondata.format.ColumnSchema> getRemoved() {
        return thriftColumnSchemas;
      }

    };

    new MockUp<org.apache.carbondata.format.ColumnSchema>() {
      @Mock
      public org.apache.carbondata.format.ColumnSchema setColumn_group_id(int column_group_id) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setScale(int scale) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setPrecision(int precision) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setNum_child(int num_child) {
        return thriftColumnSchema;
      }

      @Mock
      public org.apache.carbondata.format.ColumnSchema setDefault_value(byte[] default_value) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setColumnProperties(
          Map<String, String> columnProperties) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setInvisible(boolean invisible) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setColumnReferenceId(
          String columnReferenceId) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setIndexColumn(boolean indexColumn) {
        return thriftColumnSchema;
      }

      @Mock public String getColumn_id() {
        return "1";
      }

      @Mock public String getColumn_name() {
        return "columnName";
      }

      @Mock public boolean isColumnar() {
        return true;
      }

      @Mock public boolean isDimension() {
        return true;
      }

      @Mock public List<org.apache.carbondata.format.Encoding> getEncoders() {
        return encoders;
      }

      @Mock public int getNum_child() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getColumn_group_id() {
        return 1;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public byte[] getDefault_value() {
        return new byte[] { 1, 2 };
      }

      @Mock public String getAggregate_function() {
        return "";
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }

    };

    final Map mapTableProperties = new HashMap<String, String>();
    tabSchema = new org.apache.carbondata.format.TableSchema();

    new MockUp<org.apache.carbondata.format.TableSchema>() {
      @Mock public org.apache.carbondata.format.TableSchema setTableProperties(
          Map<String, String> tableProperties) {
        return tabSchema;
      }

      @Mock public String getTable_id() {
        return "1";
      }

      @Mock public Map<String, String> getTableProperties() {
        return mapTableProperties;
      }

      @Mock public List<org.apache.carbondata.format.ColumnSchema> getTable_columns() {
        return thriftColumnSchemas;
      }

      @Mock public org.apache.carbondata.format.SchemaEvolution getSchema_evolution() {
        return schemaEvol;
      }
    };
    new MockUp<org.apache.carbondata.format.SchemaEvolution>() {
      @Mock
      public List<org.apache.carbondata.format.SchemaEvolutionEntry> getSchema_evolution_history() {
        return thriftSchemaEvolutionEntries;
      }
    };
  }

  @Test public void testFromWrapperToExternalSchemaEvolutionEntry() {
    final SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();

    new MockUp<SchemaEvolutionEntry>() {
      @Mock public List<ColumnSchema> getAdded() {
        return columnSchemas;
      }

      @Mock public List<ColumnSchema> getRemoved() {
        return columnSchemas;
      }
    };

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public DataType getDataType() {
        return DataTypes.BOOLEAN;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }
    };

    org.apache.carbondata.format.SchemaEvolutionEntry actualResult = thriftWrapperSchemaConverter
        .fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry);
    assertEquals(schemaEvolEntry, actualResult);
  }

  @Test public void testFromWrapperToExternalSchemaEvolution() {
    SchemaEvolution schemaEvolution = new SchemaEvolution();

    new MockUp<SchemaEvolutionEntry>() {
      @Mock public List<ColumnSchema> getAdded() {
        return columnSchemas;
      }

      @Mock public List<ColumnSchema> getRemoved() {
        return columnSchemas;
      }
    };

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.BOOLEAN;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    List<org.apache.carbondata.format.SchemaEvolutionEntry> thriftSchemaEvolutionEntries =
        new ArrayList<>();
    thriftSchemaEvolutionEntries.add(schemaEvolEntry);

    org.apache.carbondata.format.SchemaEvolution actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalSchemaEvolution(schemaEvolution);

    org.apache.carbondata.format.SchemaEvolution expectedResult =
        new org.apache.carbondata.format.SchemaEvolution(thriftSchemaEvolutionEntries);
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForBooleanDataType() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
            new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.BOOLEAN,
                    "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    ColumnSchema wrapperColumnSchema = new ColumnSchema();

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.BOOLEAN;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }

      @Mock public String getAggFunction() {return "" ;}
    };

    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForStringDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.STRING,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.STRING;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForIntDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.INT,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.INT;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForShortDatatype() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.SHORT,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.SHORT;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForLongDatatype() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.LONG,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.LONG;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForDoubleDatatype() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.DOUBLE,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.DOUBLE;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForDecimalDatatype() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.DECIMAL,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.createDefaultDecimalType();
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }

      @Mock public String getAggFunction() {
        return "";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForTimestampDatatype() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(
            org.apache.carbondata.format.DataType.TIMESTAMP, "columnName", "1", true, encoders,
            true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.TIMESTAMP;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForArrayDatatype() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.ARRAY,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.createDefaultArrayType();
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForStructDatatype() {
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.STRUCT,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.createDefaultStructType();
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaForDatatypeNullCase() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(
            org.apache.carbondata.format.DataType.INT,
            "columnName",
            "1",
            true,
            encoders,
            true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.INT;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchemaWhenEncoderIsNull() {

    final List<Encoding> encoding = new ArrayList<>();
    encoding.add(Encoding.INVERTED_INDEX);
    encoding.add(null);

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encoding;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.BOOLEAN;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    List<Encoding> encodings = null;
    encodings = new ArrayList<>();
    encodings.add(Encoding.INVERTED_INDEX);
    encodings.add(null);
    List<org.apache.carbondata.format.Encoding> encoders = null;
    encoders = new ArrayList<org.apache.carbondata.format.Encoding>();
    encoders.add(org.apache.carbondata.format.Encoding.INVERTED_INDEX);
    encoders.add(null);
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema = null;
    thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.BOOLEAN,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);
    thriftColumnSchema.setAggregate_function("");
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalTableSchema() {
    TableSchema wrapperTableSchema = new TableSchema();

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.STRING;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    new MockUp<SchemaEvolutionEntry>() {
      @Mock public List<ColumnSchema> getAdded() {
        return columnSchemas;
      }

      @Mock public List<ColumnSchema> getRemoved() {
        return columnSchemas;
      }
    };

    final SchemaEvolution schemaEvolution = new SchemaEvolution();
    final Map mapTableProperties = new HashMap<String, String>();

    new MockUp<TableSchema>() {
      @Mock public List<ColumnSchema> getListOfColumns() {
        return columnSchemas;
      }

      @Mock public SchemaEvolution getSchemaEvolution() {
        return schemaEvolution;
      }

      @Mock public String getTableId() {
        return "tableId";
      }

      @Mock public Map<String, String> getTableProperties() {
        return mapTableProperties;
      }

    };
    org.apache.carbondata.format.TableSchema expectedResult =
        new org.apache.carbondata.format.TableSchema("tableId", thriftColumnSchemas, schemaEvol);
    org.apache.carbondata.format.TableSchema actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalTableSchema(wrapperTableSchema);
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testFromWrapperToExternalTableInfo() {
    TableInfo wrapperTableInfo = new TableInfo();
    String dbName = "dbName";
    String tableName = "TableName";
    final TableSchema wrapperTableSchema = new TableSchema();
    final List<TableSchema> tableSchemas = new ArrayList<>();
    tableSchemas.add(wrapperTableSchema);

    new MockUp<SchemaEvolutionEntry>() {
      @Mock public List<ColumnSchema> getAdded() {
        return columnSchemas;
      }

      @Mock public List<ColumnSchema> getRemoved() {
        return columnSchemas;
      }
    };

    new MockUp<ColumnSchema>() {
      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public int getSchemaOrdinal() {
        return 1;
      }

      @Mock public DataType getDataType() {
        return DataTypes.STRING;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }

      @Mock public String getAggFunction() { return "";}
    };

    new MockUp<TableInfo>() {
      @Mock public TableSchema getFactTable() {
        return wrapperTableSchema;
      }
    };

    new MockUp<TableSchema>() {
      @Mock public List<ColumnSchema> getListOfColumns() {
        return columnSchemas;
      }

      final SchemaEvolution schemaEvolution = new SchemaEvolution();
      final Map mapTableProperties = new HashMap<String, String>();

      @Mock public SchemaEvolution getSchemaEvolution() {
        return schemaEvolution;
      }

      @Mock public String getTableId() {
        return "tableId";
      }

      @Mock public Map<String, String> getTableProperties() {
        return mapTableProperties;
      }

    };
    org.apache.carbondata.format.TableSchema thriftFactTable =
        new org.apache.carbondata.format.TableSchema("tableId", thriftColumnSchemas, schemaEvol);
    org.apache.carbondata.format.TableInfo actualResult = thriftWrapperSchemaConverter
        .fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName);
    org.apache.carbondata.format.TableInfo expectedResult =
        new org.apache.carbondata.format.TableInfo(thriftFactTable, new ArrayList<org.apache
            .carbondata.format.TableSchema>());
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testFromExternalToWrapperSchemaEvolutionEntry() {
    long time =1112745600000L;
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    wrapperColumnSchema.setColumnUniqueId("1");
    wrapperColumnSchema.setColumnName("columnName");
    wrapperColumnSchema.setDataType(DataTypes.STRING);
    wrapperColumnSchema.setDimensionColumn(true);
    wrapperColumnSchema.setEncodingList(encodings);
    wrapperColumnSchema.setNumberOfChild(1);
    wrapperColumnSchema.setDefaultValue(new byte[] { 1, 2 });
    wrapperColumnSchema.setColumnProperties(columnPropertyMap);
    wrapperColumnSchema.setInvisible(true);
    wrapperColumnSchema.setColumnReferenceId("1");
    List<ColumnSchema> wrapperAddedColumns = new ArrayList<ColumnSchema>();
    wrapperAddedColumns.add(wrapperColumnSchema);
    SchemaEvolutionEntry wrapperSchemaEvolutionEntry = new SchemaEvolutionEntry();

    List<ColumnSchema> wrapperRemovedColumns = new ArrayList<ColumnSchema>();
    wrapperRemovedColumns.add(wrapperColumnSchema);

    wrapperSchemaEvolutionEntry.setTimeStamp(time);
    wrapperSchemaEvolutionEntry.setAdded(wrapperAddedColumns);
    wrapperSchemaEvolutionEntry.setRemoved(wrapperRemovedColumns);
    SchemaEvolutionEntry actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperSchemaEvolutionEntry(schemaEvolEntry);
    assertEquals(wrapperSchemaEvolutionEntry.getAdded().get(0), actualResult.getAdded().get(0));
  }

  @Test public void testFromExternalToWrapperSchemaEvolution() {

    new MockUp<SchemaEvolutionEntry>() {
      @Mock public List<ColumnSchema> getAdded() {
        return columnSchemas;
      }

      @Mock public List<ColumnSchema> getRemoved() {
        return columnSchemas;
      }
    };

    SchemaEvolution actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperSchemaEvolution(schemaEvol);
    assertEquals(columnSchemas, actualResult.getSchemaEvolutionEntryList().get(0).getAdded());
  }

  @Test public void testFromExternalToWrapperColumnSchema() {
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForIntDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.INT,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForShortDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.SHORT,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForLongDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.LONG,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForDoubleDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.DOUBLE,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForDecimalDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.DECIMAL,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForTimestampDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(
            org.apache.carbondata.format.DataType.TIMESTAMP, "columnName", "1", true, encoders,
            true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForArrayDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.ARRAY,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForStructDatatype() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.STRUCT,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaForDatatypeNullCase() {

    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(
            org.apache.carbondata.format.DataType.STRING,
            "columnName", "1", true, encoders, true);
    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColumnSchema);
    Boolean expectedResult = true;
    assertEquals(expectedResult, actualResult.hasEncoding(encodings.get(0)));
  }

  @Test public void testFromExternalToWrapperColumnSchemaEncodingNullCase() {

    final List<org.apache.carbondata.format.Encoding> encoders =
        new ArrayList<org.apache.carbondata.format.Encoding>();
    encoders.add(org.apache.carbondata.format.Encoding.INVERTED_INDEX);
    encoders.add(null);

    List<Encoding> encodings = new ArrayList<>();
    encodings.add(Encoding.INVERTED_INDEX);
    encodings.add(null);

    final org.apache.carbondata.format.ColumnSchema thriftColSchema =
        new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.STRING,
            "columnName", "1", true, encoders, true);
    thriftColumnSchema.setSchemaOrdinal(1);

    new MockUp<org.apache.carbondata.format.ColumnSchema>() {
      @Mock
      public org.apache.carbondata.format.ColumnSchema setColumn_group_id(int column_group_id) {
        return thriftColSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setScale(int scale) {
        return thriftColSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setPrecision(int precision) {
        return thriftColSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setNum_child(int num_child) {
        return thriftColSchema;
      }

      @Mock
      public org.apache.carbondata.format.ColumnSchema setDefault_value(byte[] default_value) {
        return thriftColSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setColumnProperties(
          Map<String, String> columnProperties) {
        return thriftColSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setInvisible(boolean invisible) {
        return thriftColSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setColumnReferenceId(
          String columnReferenceId) {
        return thriftColSchema;
      }

      @Mock public String getColumn_id() {
        return "1";
      }

      @Mock public String getColumn_name() {
        return "columnName";
      }

      @Mock public boolean isColumnar() {
        return true;
      }

      @Mock public boolean isDimension() {
        return true;
      }

      @Mock public List<org.apache.carbondata.format.Encoding> getEncoders() {
        return encoders;
      }

      @Mock public int getNum_child() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getColumn_group_id() {
        return 1;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public byte[] getDefault_value() {
        return new byte[] { 1, 2 };
      }

      @Mock public String getAggregate_function() {
        return "";
      }

      @Mock public Map<String, String> getColumnProperties() {
        return columnPropertyMap;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }

    };

    ColumnSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperColumnSchema(thriftColSchema);
    assertEquals("columnName", actualResult.getColumnName());
  }

  @Test public void testFromExternalToWrapperTableSchema() {
    String tableId = "1";
    String tableName = "tablename";
    TableSchema actualResult =
        thriftWrapperSchemaConverter.fromExternalToWrapperTableSchema(tabSchema, "tableName");
    assertEquals(tableId, actualResult.getTableId());
    assertEquals(tableName, actualResult.getTableName());
  }

  @Test public void testFromExternalToWrapperTableInfo() {
    final List<org.apache.carbondata.format.TableSchema> tableSchemas = new ArrayList();
    long time = 1112745600000L;
    tableSchemas.add(tabSchema);
    new MockUp<org.apache.carbondata.format.TableInfo>() {
      @Mock public org.apache.carbondata.format.TableSchema getFact_table() {
        return tabSchema;
      }

      @Mock public List<org.apache.carbondata.format.TableSchema> getAggregate_table_list() {
        return tableSchemas;
      }
    };
    org.apache.carbondata.format.TableInfo externalTableInfo =
        new org.apache.carbondata.format.TableInfo();
    TableInfo actualResult = thriftWrapperSchemaConverter
        .fromExternalToWrapperTableInfo(externalTableInfo, "dbName", "tableName", "/path");
    assertEquals(time, actualResult.getLastUpdatedTime());
    assertEquals("dbname_tablename", actualResult.getTableUniqueName());
  }

}