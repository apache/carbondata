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
package org.carbondata.core.carbon.metadata.converter;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolution;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolutionEntry;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.carbondata.core.carbon.metadata.schema.table.TableSchema;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

/**
 * Thrift schema to carbon schema converter and vice versa
 */
public class ThriftWrapperSchemaConverterImpl implements SchemaConverter {

  /* (non-Javadoc)
   * Converts  from wrapper to thrift schema evolution entry
   */
  @Override
  public org.carbondata.format.SchemaEvolutionEntry fromWrapperToExternalSchemaEvolutionEntry(
      SchemaEvolutionEntry wrapperSchemaEvolutionEntry) {
    org.carbondata.format.SchemaEvolutionEntry thriftSchemaEvolutionEntry =
        new org.carbondata.format.SchemaEvolutionEntry(wrapperSchemaEvolutionEntry.getTimeStamp());

    List<org.carbondata.format.ColumnSchema> thriftAddedColumns =
        new ArrayList<org.carbondata.format.ColumnSchema>();
    for (ColumnSchema wrapperColumnSchema : wrapperSchemaEvolutionEntry.getAdded()) {
      thriftAddedColumns.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
    }

    List<org.carbondata.format.ColumnSchema> thriftRemovedColumns =
        new ArrayList<org.carbondata.format.ColumnSchema>();
    for (ColumnSchema wrapperColumnSchema : wrapperSchemaEvolutionEntry.getRemoved()) {
      thriftRemovedColumns.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
    }

    thriftSchemaEvolutionEntry.setAdded(thriftAddedColumns);
    thriftSchemaEvolutionEntry.setRemoved(thriftRemovedColumns);
    return thriftSchemaEvolutionEntry;
  }

  /* (non-Javadoc)
   * converts from wrapper to thrift schema evolution
   */
  @Override public org.carbondata.format.SchemaEvolution fromWrapperToExternalSchemaEvolution(
      SchemaEvolution wrapperSchemaEvolution) {

    List<org.carbondata.format.SchemaEvolutionEntry> thriftSchemaEvolEntryList =
        new ArrayList<org.carbondata.format.SchemaEvolutionEntry>();
    for (SchemaEvolutionEntry schemaEvolutionEntry : wrapperSchemaEvolution
        .getSchemaEvolutionEntryList()) {
      thriftSchemaEvolEntryList
          .add(fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry));
    }
    return new org.carbondata.format.SchemaEvolution(thriftSchemaEvolEntryList);
  }


  /**
   * converts from wrapper to external encoding
   *
   * @param encoder
   * @return
   */
  private org.carbondata.format.Encoding fromWrapperToExternalEncoding(Encoding encoder) {

    if (null == encoder) {
      return null;
    }

    switch (encoder) {
      case DICTIONARY:
        return org.carbondata.format.Encoding.DICTIONARY;
      case DELTA:
        return org.carbondata.format.Encoding.DELTA;
      case RLE:
        return org.carbondata.format.Encoding.RLE;
      case INVERTED_INDEX:
        return org.carbondata.format.Encoding.INVERTED_INDEX;
      case BIT_PACKED:
        return org.carbondata.format.Encoding.BIT_PACKED;
      case DIRECT_DICTIONARY:
        return org.carbondata.format.Encoding.DIRECT_DICTIONARY;
      default:
        return org.carbondata.format.Encoding.DICTIONARY;
    }
  }

  /**
   * convert from wrapper to external data type
   *
   * @param dataType
   * @return
   */
  private org.carbondata.format.DataType fromWrapperToExternalDataType(DataType dataType) {

    if (null == dataType) {
      return null;
    }
    switch (dataType) {
      case STRING:
        return org.carbondata.format.DataType.STRING;
      case INT:
        return org.carbondata.format.DataType.INT;
      case LONG:
        return org.carbondata.format.DataType.LONG;
      case DOUBLE:
        return org.carbondata.format.DataType.DOUBLE;
      case DECIMAL:
        return org.carbondata.format.DataType.DECIMAL;
      case TIMESTAMP:
        return org.carbondata.format.DataType.TIMESTAMP;
      case ARRAY:
        return org.carbondata.format.DataType.ARRAY;
      case STRUCT:
        return org.carbondata.format.DataType.STRUCT;
      default:
        return org.carbondata.format.DataType.STRING;
    }
  }

  /* (non-Javadoc)
   * convert from wrapper to external column schema
   */
  @Override public org.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
      ColumnSchema wrapperColumnSchema) {

    List<org.carbondata.format.Encoding> encoders = new ArrayList<org.carbondata.format.Encoding>();
    for (Encoding encoder : wrapperColumnSchema.getEncodingList()) {
      encoders.add(fromWrapperToExternalEncoding(encoder));
    }
    org.carbondata.format.ColumnSchema thriftColumnSchema = new org.carbondata.format.ColumnSchema(
        fromWrapperToExternalDataType(wrapperColumnSchema.getDataType()),
        wrapperColumnSchema.getColumnName(), wrapperColumnSchema.getColumnUniqueId(),
        wrapperColumnSchema.isColumnar(), encoders, wrapperColumnSchema.isDimensionColumn());
    thriftColumnSchema.setColumn_group_id(wrapperColumnSchema.getColumnGroupId());
    thriftColumnSchema.setScale(wrapperColumnSchema.getScale());
    thriftColumnSchema.setPrecision(wrapperColumnSchema.getPrecision());
    thriftColumnSchema.setNum_child(wrapperColumnSchema.getNumberOfChild());
    thriftColumnSchema.setDefault_value(wrapperColumnSchema.getDefaultValue());
    return thriftColumnSchema;
  }

  /* (non-Javadoc)
   * convert from wrapper to external tableschema
   */
  @Override public org.carbondata.format.TableSchema fromWrapperToExternalTableSchema(
      TableSchema wrapperTableSchema) {

    List<org.carbondata.format.ColumnSchema> thriftColumnSchema =
        new ArrayList<org.carbondata.format.ColumnSchema>();
    for (ColumnSchema wrapperColumnSchema : wrapperTableSchema.getListOfColumns()) {
      thriftColumnSchema.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
    }
    org.carbondata.format.SchemaEvolution schemaEvolution =
        fromWrapperToExternalSchemaEvolution(wrapperTableSchema.getSchemaEvalution());
    return new org.carbondata.format.TableSchema(wrapperTableSchema.getTableId(),
        thriftColumnSchema, schemaEvolution);
  }

  /* (non-Javadoc)
   * convert from wrapper to external tableinfo
   */
  @Override public org.carbondata.format.TableInfo fromWrapperToExternalTableInfo(
      TableInfo wrapperTableInfo, String dbName, String tableName) {

    org.carbondata.format.TableSchema thriftFactTable =
        fromWrapperToExternalTableSchema(wrapperTableInfo.getFactTable());
    List<org.carbondata.format.TableSchema> thriftAggTables =
        new ArrayList<org.carbondata.format.TableSchema>();
    for (TableSchema wrapperAggTableSchema : wrapperTableInfo.getAggregateTableList()) {
      thriftAggTables.add(fromWrapperToExternalTableSchema(wrapperAggTableSchema));
    }
    return new org.carbondata.format.TableInfo(thriftFactTable, thriftAggTables);
  }

  /* (non-Javadoc)
   * convert from external to wrapper schema evolution entry
   */
  @Override public SchemaEvolutionEntry fromExternalToWrapperSchemaEvolutionEntry(
      org.carbondata.format.SchemaEvolutionEntry externalSchemaEvolutionEntry) {

    SchemaEvolutionEntry wrapperSchemaEvolutionEntry = new SchemaEvolutionEntry();
    wrapperSchemaEvolutionEntry.setTimeStamp(externalSchemaEvolutionEntry.getTime_stamp());

    List<ColumnSchema> wrapperAddedColumns = new ArrayList<ColumnSchema>();
    if (null != externalSchemaEvolutionEntry.getAdded()) {
      for (org.carbondata.format.ColumnSchema externalColumnSchema : externalSchemaEvolutionEntry
          .getAdded()) {
        wrapperAddedColumns.add(fromExternalToWrapperColumnSchema(externalColumnSchema));
      }
    }
    List<ColumnSchema> wrapperRemovedColumns = new ArrayList<ColumnSchema>();
    if (null != externalSchemaEvolutionEntry.getRemoved()) {
      for (org.carbondata.format.ColumnSchema externalColumnSchema : externalSchemaEvolutionEntry
          .getRemoved()) {
        wrapperRemovedColumns.add(fromExternalToWrapperColumnSchema(externalColumnSchema));
      }
    }

    wrapperSchemaEvolutionEntry.setAdded(wrapperAddedColumns);
    wrapperSchemaEvolutionEntry.setRemoved(wrapperRemovedColumns);
    return wrapperSchemaEvolutionEntry;

  }

  /* (non-Javadoc)
   * convert from external to wrapper schema evolution
   */
  @Override public SchemaEvolution fromExternalToWrapperSchemaEvolution(
      org.carbondata.format.SchemaEvolution externalSchemaEvolution) {
    List<SchemaEvolutionEntry> wrapperSchemaEvolEntryList = new ArrayList<SchemaEvolutionEntry>();
    for (org.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry : externalSchemaEvolution
        .getSchema_evolution_history()) {
      wrapperSchemaEvolEntryList
          .add(fromExternalToWrapperSchemaEvolutionEntry(schemaEvolutionEntry));
    }
    SchemaEvolution wrapperSchemaEvolution = new SchemaEvolution();
    wrapperSchemaEvolution.setSchemaEvolutionEntryList(wrapperSchemaEvolEntryList);
    return wrapperSchemaEvolution;
  }

  /**
   * convert from external to wrapper encoding
   *
   * @param encoder
   * @return
   */
  private Encoding fromExternalToWrapperEncoding(org.carbondata.format.Encoding encoder) {
    if (null == encoder) {
      return null;
    }
    switch (encoder) {
      case DICTIONARY:
        return Encoding.DICTIONARY;
      case DELTA:
        return Encoding.DELTA;
      case RLE:
        return Encoding.RLE;
      case INVERTED_INDEX:
        return Encoding.INVERTED_INDEX;
      case BIT_PACKED:
        return Encoding.BIT_PACKED;
      case DIRECT_DICTIONARY:
        return Encoding.DIRECT_DICTIONARY;
      default:
        return Encoding.DICTIONARY;
    }
  }

  /**
   * convert from external to wrapper data type
   *
   * @param dataType
   * @return
   */
  private DataType fromExternalToWrapperDataType(org.carbondata.format.DataType dataType) {
    if (null == dataType) {
      return null;
    }
    switch (dataType) {
      case STRING:
        return DataType.STRING;
      case INT:
        return DataType.INT;
      case LONG:
        return DataType.LONG;
      case DOUBLE:
        return DataType.DOUBLE;
      case DECIMAL:
        return DataType.DECIMAL;
      case TIMESTAMP:
        return DataType.TIMESTAMP;
      case ARRAY:
        return DataType.ARRAY;
      case STRUCT:
        return DataType.STRUCT;
      default:
        return DataType.STRING;
    }
  }

  /* (non-Javadoc)
   * convert from external to wrapper columnschema
   */
  @Override public ColumnSchema fromExternalToWrapperColumnSchema(
      org.carbondata.format.ColumnSchema externalColumnSchema) {
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    wrapperColumnSchema.setColumnUniqueId(externalColumnSchema.getColumn_id());
    wrapperColumnSchema.setColumnName(externalColumnSchema.getColumn_name());
    wrapperColumnSchema.setColumnar(externalColumnSchema.isColumnar());
    wrapperColumnSchema.setDataType(fromExternalToWrapperDataType(externalColumnSchema.data_type));
    wrapperColumnSchema.setDimensionColumn(externalColumnSchema.isDimension());
    List<Encoding> encoders = new ArrayList<Encoding>();
    for (org.carbondata.format.Encoding encoder : externalColumnSchema.getEncoders()) {
      encoders.add(fromExternalToWrapperEncoding(encoder));
    }
    wrapperColumnSchema.setEncodingList(encoders);
    wrapperColumnSchema.setNumberOfChild(externalColumnSchema.getNum_child());
    wrapperColumnSchema.setPrecision(externalColumnSchema.getPrecision());
    wrapperColumnSchema.setColumnGroup(externalColumnSchema.getColumn_group_id());
    wrapperColumnSchema.setScale(externalColumnSchema.getScale());
    wrapperColumnSchema.setDefaultValue(externalColumnSchema.getDefault_value());
    wrapperColumnSchema.setAggregateFunction(externalColumnSchema.getAggregate_function());
    return wrapperColumnSchema;
  }

  /* (non-Javadoc)
   * convert from external to wrapper tableschema
   */
  @Override public TableSchema fromExternalToWrapperTableSchema(
      org.carbondata.format.TableSchema externalTableSchema, String tableName) {
    TableSchema wrapperTableSchema = new TableSchema();
    wrapperTableSchema.setTableId(externalTableSchema.getTable_id());
    wrapperTableSchema.setTableName(tableName);
    List<ColumnSchema> listOfColumns = new ArrayList<ColumnSchema>();
    for (org.carbondata.format.ColumnSchema externalColumnSchema : externalTableSchema
        .getTable_columns()) {
      listOfColumns.add(fromExternalToWrapperColumnSchema(externalColumnSchema));
    }
    wrapperTableSchema.setListOfColumns(listOfColumns);
    wrapperTableSchema.setSchemaEvalution(
        fromExternalToWrapperSchemaEvolution(externalTableSchema.getSchema_evolution()));
    return wrapperTableSchema;
  }

  /* (non-Javadoc)
   * convert from external to wrapper tableinfo
   */
  @Override public TableInfo fromExternalToWrapperTableInfo(
      org.carbondata.format.TableInfo externalTableInfo, String dbName, String tableName) {
    TableInfo wrapperTableInfo = new TableInfo();
    wrapperTableInfo.setLastUpdatedTime(
        externalTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history().get(0)
            .getTime_stamp());
    wrapperTableInfo.setDatabaseName(dbName);
    wrapperTableInfo.setTableUniqueName(dbName + "_" + tableName);
    wrapperTableInfo.setFactTable(
        fromExternalToWrapperTableSchema(externalTableInfo.getFact_table(), tableName));
    List<TableSchema> aggTablesList = new ArrayList<TableSchema>();
    int index = 0;
    for (org.carbondata.format.TableSchema aggTable : externalTableInfo.getAggregate_table_list()) {
      aggTablesList.add(fromExternalToWrapperTableSchema(aggTable, "agg_table_" + index));
      index++;
    }
    return wrapperTableInfo;
  }

}
