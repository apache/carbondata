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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.carbondata.core.carbon.metadata.datatype.ConvertedType;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolution;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolutionEntry;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.carbondata.core.carbon.metadata.schema.table.TableSchema;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

/**
 * Thrift schema to carbon schema converter and vice versa
 *
 */
public class ThriftWrapperSchemaConverterImpl implements SchemaConverter {

  /* (non-Javadoc)
   * Converts  from wrapper to thrift schema evolution entry
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromWrapperToExternalSchemaEvolutionEntry(org.carbondata.core.carbon.metadata.schema.SchemaEvolutionEntry)
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
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromWrapperToExternalSchemaEvolution(org.carbondata.core.carbon.metadata.schema.SchemaEvolution)
   */
  @Override
  public org.carbondata.format.SchemaEvolution fromWrapperToExternalSchemaEvolution(
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
   * converts wrapper to external converter type
   * @param convertedType
   * @return
   */
  private org.carbondata.format.ConvertedType fromWrapperToExternalConvertedType(
      ConvertedType convertedType) {
    
    if(null == convertedType)
    {
      return null;
    }
    switch (convertedType) {
    case UTF8:
      return org.carbondata.format.ConvertedType.UTF8;
    case MAP:
      return org.carbondata.format.ConvertedType.MAP;
    case MAP_KEY_VALUE:
      return org.carbondata.format.ConvertedType.MAP_KEY_VALUE;
    case LIST:
      return org.carbondata.format.ConvertedType.LIST;
    case ENUM:
      return org.carbondata.format.ConvertedType.ENUM;
    case DECIMAL:
      return org.carbondata.format.ConvertedType.DECIMAL;
    case DATE:
      return org.carbondata.format.ConvertedType.DATE;
    case TIME_MILLIS:
      return org.carbondata.format.ConvertedType.TIME_MILLIS;
    case TIMESTAMP_MILLIS:
      return org.carbondata.format.ConvertedType.TIMESTAMP_MILLIS;
    case RESERVED:
      return org.carbondata.format.ConvertedType.RESERVED;
    case UINT_8:
      return org.carbondata.format.ConvertedType.UINT_8;
    case UINT_16:
      return org.carbondata.format.ConvertedType.UINT_16;
    case UINT_32:
      return org.carbondata.format.ConvertedType.UINT_32;
    case UINT_64:
      return org.carbondata.format.ConvertedType.UINT_64;
    case INT_8:
      return org.carbondata.format.ConvertedType.INT_8;
    case INT_16:
      return org.carbondata.format.ConvertedType.INT_16;
    case INT_32:
      return org.carbondata.format.ConvertedType.INT_32;
    case INT_64:
      return org.carbondata.format.ConvertedType.INT_64;
    case JSON:
      return org.carbondata.format.ConvertedType.JSON;
    case BSON:
      return org.carbondata.format.ConvertedType.BSON;
    case INTERVAL:
      return org.carbondata.format.ConvertedType.INTERVAL;
    default:
      return org.carbondata.format.ConvertedType.UTF8;
    }
  }

  /**
   * converts from wrapper to external encoding
   * @param encoder
   * @return
   */
  private org.carbondata.format.Encoding fromWrapperToExternalEncoding(Encoding encoder) {
    
    if(null == encoder)
    {
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
    case BITPACKED:
      return org.carbondata.format.Encoding.BIT_PACKED;
    default:
      return org.carbondata.format.Encoding.DICTIONARY;
    }
  }

  /**
   * convert from wrapper to external data type
   * @param dataType
   * @return
   */
  private org.carbondata.format.DataType fromWrapperToExternalDataType(DataType dataType) {

    if(null == dataType)
    {
      return null;
    }
    switch (dataType) {
      case STRING:
        return org.carbondata.format.DataType.STRING;
      case INTEGER:
        return org.carbondata.format.DataType.INTEGER;
      case LONG:
        return org.carbondata.format.DataType.LONG;
      case DOUBLE:
        return org.carbondata.format.DataType.DOUBLE;
      case BIG_DECIMAL:
        return org.carbondata.format.DataType.BIG_DECIMAL;
      case TIME_STAMP:
        return org.carbondata.format.DataType.TIME_STAMP;
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
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromWrapperToExternalColumnSchema(org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema)
   */
  @Override
  public org.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
      ColumnSchema wrapperColumnSchema) {

    List<org.carbondata.format.Encoding> encoders = new ArrayList<org.carbondata.format.Encoding>();
    for (Encoding encoder : wrapperColumnSchema.getEncodingList()) {
      encoders.add(fromWrapperToExternalEncoding(encoder));
    }
    org.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.carbondata.format.ColumnSchema(
            fromWrapperToExternalDataType(wrapperColumnSchema.getDataType()),
            wrapperColumnSchema.getColumnName(), wrapperColumnSchema.getColumnUniqueId(),
            wrapperColumnSchema.isColumnar(), encoders, wrapperColumnSchema.isDimensionColumn());
    thriftColumnSchema.setConverted_type(fromWrapperToExternalConvertedType(wrapperColumnSchema.getConvertedType()));
    thriftColumnSchema.setColumn_group_id(wrapperColumnSchema.getRowGroupId());
    thriftColumnSchema.setScale(wrapperColumnSchema.getScale());
    thriftColumnSchema.setPrecision(wrapperColumnSchema.getPrecision());
    thriftColumnSchema.setNum_child(wrapperColumnSchema.getNumberOfChild());
    thriftColumnSchema.setDefault_value(wrapperColumnSchema.getDefaultValue());
    return thriftColumnSchema;
  }

  /* (non-Javadoc)
   * convert from wrapper to external tableschema
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromWrapperToExternalTableSchema(org.carbondata.core.carbon.metadata.schema.table.TableSchema)
   */
  @Override
  public org.carbondata.format.TableSchema fromWrapperToExternalTableSchema(
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
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromWrapperToExternalTableInfo(org.carbondata.core.carbon.metadata.schema.table.TableInfo, java.lang.String, java.lang.String)
   */
  @Override
  public org.carbondata.format.TableInfo fromWrapperToExternalTableInfo(TableInfo wrapperTableInfo,
      String dbName, String tableName) {

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
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromExternalToWrapperSchemaEvolutionEntry(org.carbondata.format.SchemaEvolutionEntry)
   */
  @Override
  public SchemaEvolutionEntry fromExternalToWrapperSchemaEvolutionEntry(
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
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromExternalToWrapperSchemaEvolution(org.carbondata.format.SchemaEvolution)
   */
  @Override
  public SchemaEvolution fromExternalToWrapperSchemaEvolution(
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
   * @param encoder
   * @return
   */
  private Encoding fromExternalToWrapperEncoding(org.carbondata.format.Encoding encoder) {
    if(null == encoder)
    {
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
      return Encoding.BITPACKED;
    default:
      return Encoding.DICTIONARY;
    }
  }

  /**
   * convert from external to wrapper data type
   * @param dataType
   * @return
   */
  private DataType fromExternalToWrapperDataType(org.carbondata.format.DataType dataType) {
    if(null == dataType)
    {
      return null;
    }
    switch (dataType) {
    case STRING:
      return DataType.STRING;
    case INTEGER:
      return DataType.INTEGER;
    case LONG:
      return DataType.LONG;
    case DOUBLE:
      return DataType.DOUBLE;
    case BIG_DECIMAL:
      return DataType.BIG_DECIMAL;
    case TIME_STAMP:
      return DataType.TIME_STAMP;
    case ARRAY:
      return DataType.ARRAY;
    case STRUCT:
      return DataType.STRUCT;
    default:
      return DataType.STRING;
    }
  }

  /**
   * convert from external to wrapper converter type
   * @param convertedType
   * @return
   */
  private ConvertedType fromExternalToWrapperConvertedType(
      org.carbondata.format.ConvertedType convertedType) {
    if(null == convertedType)
    {
      return null;
    }
    switch (convertedType) {
      case UTF8:
        return ConvertedType.UTF8;
      case MAP:
        return ConvertedType.MAP;
      case MAP_KEY_VALUE:
        return ConvertedType.MAP_KEY_VALUE;
      case LIST:
        return ConvertedType.LIST;
      case ENUM:
        return ConvertedType.ENUM;
      case DECIMAL:
        return ConvertedType.DECIMAL;
      case DATE:
        return ConvertedType.DATE;
      case TIME_MILLIS:
        return ConvertedType.TIME_MILLIS;
      case TIMESTAMP_MILLIS:
        return ConvertedType.TIMESTAMP_MILLIS;
      case RESERVED:
        return ConvertedType.RESERVED;
      case UINT_8:
        return ConvertedType.UINT_8;
      case UINT_16:
        return ConvertedType.UINT_16;
      case UINT_32:
        return ConvertedType.UINT_32;
      case UINT_64:
        return ConvertedType.UINT_64;
      case INT_8:
        return ConvertedType.INT_8;
      case INT_16:
        return ConvertedType.INT_16;
      case INT_32:
        return ConvertedType.INT_32;
      case INT_64:
        return ConvertedType.INT_64;
      case JSON:
        return ConvertedType.JSON;
      case BSON:
        return ConvertedType.BSON;
      case INTERVAL:
        return ConvertedType.INTERVAL;
      default:
        return ConvertedType.UTF8;
    }
  }

  /* (non-Javadoc)
   * convert from external to wrapper columnschema
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromExternalToWrapperColumnSchema(org.carbondata.format.ColumnSchema)
   */
  @Override
  public ColumnSchema fromExternalToWrapperColumnSchema(
      org.carbondata.format.ColumnSchema externalColumnSchema) {
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    wrapperColumnSchema.setColumnUniqueId(externalColumnSchema.getColumn_id());
    wrapperColumnSchema.setColumnName(externalColumnSchema.getColumn_name());
    wrapperColumnSchema.setColumnar(externalColumnSchema.isColumnar());
    wrapperColumnSchema.setDataType(fromExternalToWrapperDataType(externalColumnSchema.data_type));
    wrapperColumnSchema.setDimensionColumn(externalColumnSchema.isDimension());
    Set<Encoding> encoders = new HashSet<Encoding>();
    for (org.carbondata.format.Encoding encoder : externalColumnSchema.getEncoders()) {
      encoders.add(fromExternalToWrapperEncoding(encoder));
    }
    wrapperColumnSchema.setEncodintList(encoders);
    wrapperColumnSchema.setConvertedType(fromExternalToWrapperConvertedType(externalColumnSchema.getConverted_type()));
    wrapperColumnSchema.setNumberOfChild(externalColumnSchema.getNum_child());
    wrapperColumnSchema.setPrecision(externalColumnSchema.getPrecision());
    wrapperColumnSchema.setRowGroupId(externalColumnSchema.getColumn_group_id());
    wrapperColumnSchema.setScale(externalColumnSchema.getScale());
    wrapperColumnSchema.setDefaultValue(externalColumnSchema.getDefault_value());
    wrapperColumnSchema.setAggregateFunction(externalColumnSchema.getAggregate_function());
    return wrapperColumnSchema;
  }

  /* (non-Javadoc)
   * convert from external to wrapper tableschema
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromExternalToWrapperTableSchema(org.carbondata.format.TableSchema, java.lang.String)
   */
  @Override
  public TableSchema fromExternalToWrapperTableSchema(
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
    wrapperTableSchema.setSchemaEvalution(fromExternalToWrapperSchemaEvolution(externalTableSchema
        .getSchema_evolution()));
    return wrapperTableSchema;
  }

  /* (non-Javadoc)
   * convert from external to wrapper tableinfo
   * @see org.carbondata.core.carbon.metadata.converter.SchemaConverter#fromExternalToWrapperTableInfo(org.carbondata.format.TableInfo, java.lang.String, java.lang.String)
   */
  @Override
  public TableInfo fromExternalToWrapperTableInfo(
      org.carbondata.format.TableInfo externalTableInfo, String dbName, String tableName) {
    TableInfo wrapperTableInfo = new TableInfo();
    wrapperTableInfo.setLastUpdatedTime(externalTableInfo.getFact_table().getSchema_evolution()
        .getSchema_evolution_history().get(0).getTime_stamp());
    wrapperTableInfo.setDatabaseName(dbName);
    wrapperTableInfo.setTableUniqueName(dbName + "_" + tableName);
    wrapperTableInfo.setFactTable(fromExternalToWrapperTableSchema(
      externalTableInfo.getFact_table(), tableName));
    List<TableSchema> aggTablesList = new ArrayList<TableSchema>();
    int index = 0;
    for (org.carbondata.format.TableSchema aggTable : externalTableInfo.getAggregate_table_list()) {
      aggTablesList.add(fromExternalToWrapperTableSchema(aggTable, "agg_table_" + index));
      index++;
    }
    return wrapperTableInfo;
  }

}
