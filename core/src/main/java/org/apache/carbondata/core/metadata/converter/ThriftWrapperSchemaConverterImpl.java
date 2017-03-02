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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Thrift schema to carbon schema converter and vice versa
 */
public class ThriftWrapperSchemaConverterImpl implements SchemaConverter {

  /* (non-Javadoc)
   * Converts  from wrapper to thrift schema evolution entry
   */
  @Override
  public org.apache.carbondata.format.SchemaEvolutionEntry
       fromWrapperToExternalSchemaEvolutionEntry(SchemaEvolutionEntry wrapperSchemaEvolutionEntry) {
    org.apache.carbondata.format.SchemaEvolutionEntry thriftSchemaEvolutionEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry(
            wrapperSchemaEvolutionEntry.getTimeStamp());

    if (null != wrapperSchemaEvolutionEntry.getAdded()) {
      List<org.apache.carbondata.format.ColumnSchema> thriftAddedColumns =
          new ArrayList<org.apache.carbondata.format.ColumnSchema>();
      for (ColumnSchema wrapperColumnSchema : wrapperSchemaEvolutionEntry.getAdded()) {
        thriftAddedColumns.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
      }
      thriftSchemaEvolutionEntry.setAdded(thriftAddedColumns);
    }
    if (null != wrapperSchemaEvolutionEntry.getRemoved()) {
      List<org.apache.carbondata.format.ColumnSchema> thriftRemovedColumns =
          new ArrayList<org.apache.carbondata.format.ColumnSchema>();
      for (ColumnSchema wrapperColumnSchema : wrapperSchemaEvolutionEntry.getRemoved()) {
        thriftRemovedColumns.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
      }
      thriftSchemaEvolutionEntry.setRemoved(thriftRemovedColumns);
    }

    return thriftSchemaEvolutionEntry;
  }

  /* (non-Javadoc)
   * converts from wrapper to thrift schema evolution
   */
  @Override
  public org.apache.carbondata.format.SchemaEvolution fromWrapperToExternalSchemaEvolution(
      SchemaEvolution wrapperSchemaEvolution) {

    List<org.apache.carbondata.format.SchemaEvolutionEntry> thriftSchemaEvolEntryList =
        new ArrayList<org.apache.carbondata.format.SchemaEvolutionEntry>();
    for (SchemaEvolutionEntry schemaEvolutionEntry : wrapperSchemaEvolution
        .getSchemaEvolutionEntryList()) {
      thriftSchemaEvolEntryList
          .add(fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry));
    }
    return new org.apache.carbondata.format.SchemaEvolution(thriftSchemaEvolEntryList);
  }

  /**
   * converts from wrapper to external encoding
   *
   * @param encoder
   * @return
   */
  private org.apache.carbondata.format.Encoding fromWrapperToExternalEncoding(Encoding encoder) {

    if (null == encoder) {
      return null;
    }

    switch (encoder) {
      case DICTIONARY:
        return org.apache.carbondata.format.Encoding.DICTIONARY;
      case DELTA:
        return org.apache.carbondata.format.Encoding.DELTA;
      case RLE:
        return org.apache.carbondata.format.Encoding.RLE;
      case INVERTED_INDEX:
        return org.apache.carbondata.format.Encoding.INVERTED_INDEX;
      case BIT_PACKED:
        return org.apache.carbondata.format.Encoding.BIT_PACKED;
      case DIRECT_DICTIONARY:
        return org.apache.carbondata.format.Encoding.DIRECT_DICTIONARY;
      default:
        return org.apache.carbondata.format.Encoding.DICTIONARY;
    }
  }

  /**
   * convert from wrapper to external data type
   *
   * @param dataType
   * @return
   */
  private org.apache.carbondata.format.DataType fromWrapperToExternalDataType(DataType dataType) {

    if (null == dataType) {
      return null;
    }
    switch (dataType) {
      case STRING:
        return org.apache.carbondata.format.DataType.STRING;
      case INT:
        return org.apache.carbondata.format.DataType.INT;
      case SHORT:
        return org.apache.carbondata.format.DataType.SHORT;
      case LONG:
        return org.apache.carbondata.format.DataType.LONG;
      case DOUBLE:
        return org.apache.carbondata.format.DataType.DOUBLE;
      case DECIMAL:
        return org.apache.carbondata.format.DataType.DECIMAL;
      case DATE:
        return org.apache.carbondata.format.DataType.DATE;
      case TIMESTAMP:
        return org.apache.carbondata.format.DataType.TIMESTAMP;
      case ARRAY:
        return org.apache.carbondata.format.DataType.ARRAY;
      case STRUCT:
        return org.apache.carbondata.format.DataType.STRUCT;
      default:
        return org.apache.carbondata.format.DataType.STRING;
    }
  }

  /* (non-Javadoc)
   * convert from wrapper to external column schema
   */
  @Override public org.apache.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
      ColumnSchema wrapperColumnSchema) {

    List<org.apache.carbondata.format.Encoding> encoders =
        new ArrayList<org.apache.carbondata.format.Encoding>();
    for (Encoding encoder : wrapperColumnSchema.getEncodingList()) {
      encoders.add(fromWrapperToExternalEncoding(encoder));
    }
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(
            fromWrapperToExternalDataType(wrapperColumnSchema.getDataType()),
            wrapperColumnSchema.getColumnName(), wrapperColumnSchema.getColumnUniqueId(),
            wrapperColumnSchema.isColumnar(), encoders, wrapperColumnSchema.isDimensionColumn());
    thriftColumnSchema.setColumn_group_id(wrapperColumnSchema.getColumnGroupId());
    thriftColumnSchema.setScale(wrapperColumnSchema.getScale());
    thriftColumnSchema.setPrecision(wrapperColumnSchema.getPrecision());
    thriftColumnSchema.setNum_child(wrapperColumnSchema.getNumberOfChild());
    thriftColumnSchema.setDefault_value(wrapperColumnSchema.getDefaultValue());
    thriftColumnSchema.setColumnProperties(wrapperColumnSchema.getColumnProperties());
    thriftColumnSchema.setInvisible(wrapperColumnSchema.isInvisible());
    thriftColumnSchema.setColumnReferenceId(wrapperColumnSchema.getColumnReferenceId());
    thriftColumnSchema.setSchemaOrdinal(wrapperColumnSchema.getSchemaOrdinal());

    if (wrapperColumnSchema.isSortColumn()) {
      Map<String, String> properties = new HashMap<String, String>();
      properties.put(CarbonCommonConstants.SORT_COLUMNS, "true");
      thriftColumnSchema.setColumnProperties(properties);
    }

    return thriftColumnSchema;
  }

  /* (non-Javadoc)
   * convert from wrapper to external tableschema
   */
  @Override public org.apache.carbondata.format.TableSchema fromWrapperToExternalTableSchema(
      TableSchema wrapperTableSchema) {

    List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchema =
        new ArrayList<org.apache.carbondata.format.ColumnSchema>();
    for (ColumnSchema wrapperColumnSchema : wrapperTableSchema.getListOfColumns()) {
      thriftColumnSchema.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
    }
    org.apache.carbondata.format.SchemaEvolution schemaEvolution =
        fromWrapperToExternalSchemaEvolution(wrapperTableSchema.getSchemaEvalution());
    org.apache.carbondata.format.TableSchema externalTableSchema =
        new org.apache.carbondata.format.TableSchema(
            wrapperTableSchema.getTableId(), thriftColumnSchema, schemaEvolution);
    externalTableSchema.setTableProperties(wrapperTableSchema.getTableProperties());
    if (wrapperTableSchema.getBucketingInfo() != null) {
      externalTableSchema.setBucketingInfo(
          fromWrapperToExternalBucketingInfo(wrapperTableSchema.getBucketingInfo()));
    }
    return externalTableSchema;
  }

  private org.apache.carbondata.format.BucketingInfo fromWrapperToExternalBucketingInfo(
      BucketingInfo bucketingInfo) {
    List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchema =
        new ArrayList<org.apache.carbondata.format.ColumnSchema>();
    for (ColumnSchema wrapperColumnSchema : bucketingInfo.getListOfColumns()) {
      thriftColumnSchema.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
    }
    return new org.apache.carbondata.format.BucketingInfo(thriftColumnSchema,
        bucketingInfo.getNumberOfBuckets());
  }

  /* (non-Javadoc)
   * convert from wrapper to external tableinfo
   */
  @Override public org.apache.carbondata.format.TableInfo fromWrapperToExternalTableInfo(
      TableInfo wrapperTableInfo, String dbName, String tableName) {

    org.apache.carbondata.format.TableSchema thriftFactTable =
        fromWrapperToExternalTableSchema(wrapperTableInfo.getFactTable());
    List<org.apache.carbondata.format.TableSchema> thriftAggTables =
        new ArrayList<org.apache.carbondata.format.TableSchema>();
    for (TableSchema wrapperAggTableSchema : wrapperTableInfo.getAggregateTableList()) {
      thriftAggTables.add(fromWrapperToExternalTableSchema(wrapperAggTableSchema));
    }
    return new org.apache.carbondata.format.TableInfo(thriftFactTable, thriftAggTables);
  }

  /* (non-Javadoc)
   * convert from external to wrapper schema evolution entry
   */
  @Override public SchemaEvolutionEntry fromExternalToWrapperSchemaEvolutionEntry(
      org.apache.carbondata.format.SchemaEvolutionEntry externalSchemaEvolutionEntry) {

    SchemaEvolutionEntry wrapperSchemaEvolutionEntry = new SchemaEvolutionEntry();
    wrapperSchemaEvolutionEntry.setTimeStamp(externalSchemaEvolutionEntry.getTime_stamp());

    List<ColumnSchema> wrapperAddedColumns = new ArrayList<ColumnSchema>();
    if (null != externalSchemaEvolutionEntry.getAdded()) {
      for (org.apache.carbondata.format.ColumnSchema externalColumnSchema :
          externalSchemaEvolutionEntry.getAdded()) {
        wrapperAddedColumns.add(fromExternalToWrapperColumnSchema(externalColumnSchema));
      }
    }
    List<ColumnSchema> wrapperRemovedColumns = new ArrayList<ColumnSchema>();
    if (null != externalSchemaEvolutionEntry.getRemoved()) {
      for (org.apache.carbondata.format.ColumnSchema externalColumnSchema :
          externalSchemaEvolutionEntry.getRemoved()) {
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
      org.apache.carbondata.format.SchemaEvolution externalSchemaEvolution) {
    List<SchemaEvolutionEntry> wrapperSchemaEvolEntryList = new ArrayList<SchemaEvolutionEntry>();
    for (org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry :
        externalSchemaEvolution.getSchema_evolution_history()) {
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
  private Encoding fromExternalToWrapperEncoding(org.apache.carbondata.format.Encoding encoder) {
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
  private DataType fromExternalToWrapperDataType(org.apache.carbondata.format.DataType dataType) {
    if (null == dataType) {
      return null;
    }
    switch (dataType) {
      case STRING:
        return DataType.STRING;
      case INT:
        return DataType.INT;
      case SHORT:
        return DataType.SHORT;
      case LONG:
        return DataType.LONG;
      case DOUBLE:
        return DataType.DOUBLE;
      case DECIMAL:
        return DataType.DECIMAL;
      case TIMESTAMP:
        return DataType.TIMESTAMP;
      case DATE:
        return DataType.DATE;
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
      org.apache.carbondata.format.ColumnSchema externalColumnSchema) {
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    wrapperColumnSchema.setColumnUniqueId(externalColumnSchema.getColumn_id());
    wrapperColumnSchema.setColumnName(externalColumnSchema.getColumn_name());
    wrapperColumnSchema.setColumnar(externalColumnSchema.isColumnar());
    wrapperColumnSchema.setDataType(fromExternalToWrapperDataType(externalColumnSchema.data_type));
    wrapperColumnSchema.setDimensionColumn(externalColumnSchema.isDimension());
    List<Encoding> encoders = new ArrayList<Encoding>();
    for (org.apache.carbondata.format.Encoding encoder : externalColumnSchema.getEncoders()) {
      encoders.add(fromExternalToWrapperEncoding(encoder));
    }
    wrapperColumnSchema.setEncodingList(encoders);
    wrapperColumnSchema.setNumberOfChild(externalColumnSchema.getNum_child());
    wrapperColumnSchema.setPrecision(externalColumnSchema.getPrecision());
    wrapperColumnSchema.setColumnGroup(externalColumnSchema.getColumn_group_id());
    wrapperColumnSchema.setScale(externalColumnSchema.getScale());
    wrapperColumnSchema.setDefaultValue(externalColumnSchema.getDefault_value());
    wrapperColumnSchema.setInvisible(externalColumnSchema.isInvisible());
    wrapperColumnSchema.setColumnReferenceId(externalColumnSchema.getColumnReferenceId());
    wrapperColumnSchema.setSchemaOrdinal(externalColumnSchema.getSchemaOrdinal());
    wrapperColumnSchema.setSortColumn(false);
    Map<String, String> properties = externalColumnSchema.getColumnProperties();
    if (properties != null) {
      String sortColumns = properties.get(CarbonCommonConstants.SORT_COLUMNS);
      if (sortColumns != null) {
        wrapperColumnSchema.setSortColumn(true);
      }
    }
    return wrapperColumnSchema;
  }

  /* (non-Javadoc)
   * convert from external to wrapper tableschema
   */
  @Override public TableSchema fromExternalToWrapperTableSchema(
      org.apache.carbondata.format.TableSchema externalTableSchema, String tableName) {
    TableSchema wrapperTableSchema = new TableSchema();
    wrapperTableSchema.setTableId(externalTableSchema.getTable_id());
    wrapperTableSchema.setTableName(tableName);
    wrapperTableSchema.setTableProperties(externalTableSchema.getTableProperties());
    List<ColumnSchema> listOfColumns = new ArrayList<ColumnSchema>();
    for (org.apache.carbondata.format.ColumnSchema externalColumnSchema : externalTableSchema
        .getTable_columns()) {
      listOfColumns.add(fromExternalToWrapperColumnSchema(externalColumnSchema));
    }
    wrapperTableSchema.setListOfColumns(listOfColumns);
    wrapperTableSchema.setSchemaEvalution(
        fromExternalToWrapperSchemaEvolution(externalTableSchema.getSchema_evolution()));
    if (externalTableSchema.isSetBucketingInfo()) {
      wrapperTableSchema.setBucketingInfo(
          fromExternalToWarpperBucketingInfo(externalTableSchema.bucketingInfo));
    }
    return wrapperTableSchema;
  }

  private BucketingInfo fromExternalToWarpperBucketingInfo(
      org.apache.carbondata.format.BucketingInfo externalBucketInfo) {
    List<ColumnSchema> listOfColumns = new ArrayList<ColumnSchema>();
    for (org.apache.carbondata.format.ColumnSchema externalColumnSchema :
          externalBucketInfo.table_columns) {
      listOfColumns.add(fromExternalToWrapperColumnSchema(externalColumnSchema));
    }
    return new BucketingInfo(listOfColumns, externalBucketInfo.number_of_buckets);
  }

  /* (non-Javadoc)
   * convert from external to wrapper tableinfo
   */
  @Override public TableInfo fromExternalToWrapperTableInfo(
      org.apache.carbondata.format.TableInfo externalTableInfo, String dbName, String tableName,
      String storePath) {
    TableInfo wrapperTableInfo = new TableInfo();
    List<org.apache.carbondata.format.SchemaEvolutionEntry> schemaEvolutionList =
        externalTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history();
    wrapperTableInfo.setLastUpdatedTime(
        schemaEvolutionList.get(schemaEvolutionList.size() - 1)
            .getTime_stamp());
    wrapperTableInfo.setDatabaseName(dbName);
    wrapperTableInfo.setTableUniqueName(dbName + "_" + tableName);
    wrapperTableInfo.setStorePath(storePath);
    wrapperTableInfo.setFactTable(
        fromExternalToWrapperTableSchema(externalTableInfo.getFact_table(), tableName));
    List<TableSchema> aggTablesList = new ArrayList<TableSchema>();
    int index = 0;
    for (org.apache.carbondata.format.TableSchema aggTable : externalTableInfo
        .getAggregate_table_list()) {
      aggTablesList.add(fromExternalToWrapperTableSchema(aggTable, "agg_table_" + index));
      index++;
    }
    return wrapperTableInfo;
  }

}
