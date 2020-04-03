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

import org.apache.carbondata.common.exceptions.DeprecatedFeatureException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;
import org.apache.carbondata.core.util.CarbonUtil;

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
      case DIRECT_COMPRESS_VARCHAR:
        return org.apache.carbondata.format.Encoding.DIRECT_COMPRESS_VARCHAR;
      case BIT_PACKED:
        return org.apache.carbondata.format.Encoding.BIT_PACKED;
      case DIRECT_DICTIONARY:
        return org.apache.carbondata.format.Encoding.DIRECT_DICTIONARY;
      case INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY:
        return org.apache.carbondata.format.Encoding.INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY;
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
    // data type object maybe created by GSON, use id to compare the type instead of object address
    if (dataType.getId() == DataTypes.BOOLEAN.getId()) {
      return org.apache.carbondata.format.DataType.BOOLEAN;
    } else if (dataType.getId() == DataTypes.STRING.getId()) {
      return org.apache.carbondata.format.DataType.STRING;
    } else if (dataType.getId() == DataTypes.INT.getId()) {
      return org.apache.carbondata.format.DataType.INT;
    } else if (dataType.getId() == DataTypes.SHORT.getId()) {
      return org.apache.carbondata.format.DataType.SHORT;
    } else if (dataType.getId() == DataTypes.LONG.getId()) {
      return org.apache.carbondata.format.DataType.LONG;
    } else if (dataType.getId() == DataTypes.DOUBLE.getId()) {
      return org.apache.carbondata.format.DataType.DOUBLE;
    } else if (DataTypes.isDecimal(dataType)) {
      return org.apache.carbondata.format.DataType.DECIMAL;
    } else if (dataType.getId() == DataTypes.DATE.getId()) {
      return org.apache.carbondata.format.DataType.DATE;
    } else if (dataType.getId() == DataTypes.TIMESTAMP.getId()) {
      return org.apache.carbondata.format.DataType.TIMESTAMP;
    } else if (dataType.getId() == DataTypes.BINARY.getId()) {
      return org.apache.carbondata.format.DataType.BINARY;
    } else if (DataTypes.isArrayType(dataType)) {
      return org.apache.carbondata.format.DataType.ARRAY;
    } else if (DataTypes.isStructType(dataType)) {
      return org.apache.carbondata.format.DataType.STRUCT;
    } else if (DataTypes.isMapType(dataType)) {
      return org.apache.carbondata.format.DataType.MAP;
    } else if (dataType.getId() == DataTypes.VARCHAR.getId()) {
      return org.apache.carbondata.format.DataType.VARCHAR;
    } else if (dataType.getId() == DataTypes.FLOAT.getId()) {
      return org.apache.carbondata.format.DataType.FLOAT;
    } else if (dataType.getId() == DataTypes.BYTE.getId()) {
      return org.apache.carbondata.format.DataType.BYTE;
    } else {
      return org.apache.carbondata.format.DataType.STRING;
    }
  }

  /* (non-Javadoc)
   * convert from wrapper to external column schema
   */
  @Override
  public org.apache.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
      ColumnSchema wrapperColumnSchema) {
    List<org.apache.carbondata.format.Encoding> encoders =
        new ArrayList<org.apache.carbondata.format.Encoding>();
    for (Encoding encoder : wrapperColumnSchema.getEncodingList()) {
      encoders.add(fromWrapperToExternalEncoding(encoder));
    }
    org.apache.carbondata.format.ColumnSchema thriftColumnSchema =
        new org.apache.carbondata.format.ColumnSchema(
            fromWrapperToExternalDataType(
                wrapperColumnSchema.getDataType()),
            wrapperColumnSchema.getColumnName(),
            wrapperColumnSchema.getColumnUniqueId(),
            true,
            encoders,
            wrapperColumnSchema.isDimensionColumn());
    thriftColumnSchema.setColumn_group_id(-1);
    if (DataTypes.isDecimal(wrapperColumnSchema.getDataType())) {
      thriftColumnSchema.setScale(wrapperColumnSchema.getScale());
      thriftColumnSchema.setPrecision(wrapperColumnSchema.getPrecision());
    } else {
      thriftColumnSchema.setScale(-1);
      thriftColumnSchema.setPrecision(-1);
    }
    thriftColumnSchema.setNum_child(wrapperColumnSchema.getNumberOfChild());
    thriftColumnSchema.setDefault_value(wrapperColumnSchema.getDefaultValue());
    thriftColumnSchema.setColumnProperties(wrapperColumnSchema.getColumnProperties());
    thriftColumnSchema.setInvisible(wrapperColumnSchema.isInvisible());
    thriftColumnSchema.setColumnReferenceId(wrapperColumnSchema.getColumnReferenceId());
    thriftColumnSchema.setSchemaOrdinal(wrapperColumnSchema.getSchemaOrdinal());
    thriftColumnSchema.setIndexColumn(wrapperColumnSchema.isIndexColumn());
    if (wrapperColumnSchema.isSortColumn()) {
      Map<String, String> properties = wrapperColumnSchema.getColumnProperties();
      if (null == properties) {
        properties = new HashMap<String, String>();
        thriftColumnSchema.setColumnProperties(properties);
      }
      properties.put(CarbonCommonConstants.SORT_COLUMNS, "true");
    }
    if (null != wrapperColumnSchema.getAggFunction() && !wrapperColumnSchema.getAggFunction()
        .isEmpty()) {
      thriftColumnSchema.setAggregate_function(wrapperColumnSchema.getAggFunction());
    } else if (null != wrapperColumnSchema.getTimeSeriesFunction() && !wrapperColumnSchema
        .getTimeSeriesFunction().isEmpty()) {
      thriftColumnSchema.setAggregate_function(wrapperColumnSchema.getTimeSeriesFunction());
    } else {
      thriftColumnSchema.setAggregate_function("");
    }
    List<ParentColumnTableRelation> parentColumnTableRelations =
        wrapperColumnSchema.getParentColumnTableRelations();
    if (null != parentColumnTableRelations) {
      thriftColumnSchema.setParentColumnTableRelations(
          wrapperToThriftRelationList(parentColumnTableRelations));
    }
    return thriftColumnSchema;
  }

  private org.apache.carbondata.format.PartitionType fromWrapperToExternalPartitionType(
      PartitionType wrapperPartitionType) {
    if (null == wrapperPartitionType) {
      return null;
    }
    switch (wrapperPartitionType) {
      case HASH:
      case LIST:
      case RANGE:
      case RANGE_INTERVAL:
        DeprecatedFeatureException.customPartitionNotSupported();
        return null;
      default:
        return org.apache.carbondata.format.PartitionType.NATIVE_HIVE;
    }
  }

  private org.apache.carbondata.format.PartitionInfo fromWrapperToExternalPartitionInfo(
      PartitionInfo wrapperPartitionInfo) {
    List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchema =
        new ArrayList<org.apache.carbondata.format.ColumnSchema>();
    for (ColumnSchema wrapperColumnSchema : wrapperPartitionInfo.getColumnSchemaList()) {
      thriftColumnSchema.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
    }
    return new org.apache.carbondata.format.PartitionInfo(thriftColumnSchema,
        fromWrapperToExternalPartitionType(wrapperPartitionInfo.getPartitionType()));
  }

  /* (non-Javadoc)
   * convert from wrapper to external tableschema
   */
  @Override
  public org.apache.carbondata.format.TableSchema fromWrapperToExternalTableSchema(
      TableSchema wrapperTableSchema) {

    List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchema =
        new ArrayList<org.apache.carbondata.format.ColumnSchema>();
    for (ColumnSchema wrapperColumnSchema : wrapperTableSchema.getListOfColumns()) {
      thriftColumnSchema.add(fromWrapperToExternalColumnSchema(wrapperColumnSchema));
    }
    org.apache.carbondata.format.SchemaEvolution schemaEvolution =
        fromWrapperToExternalSchemaEvolution(wrapperTableSchema.getSchemaEvolution());
    org.apache.carbondata.format.TableSchema externalTableSchema =
        new org.apache.carbondata.format.TableSchema(
            wrapperTableSchema.getTableId(), thriftColumnSchema, schemaEvolution);
    externalTableSchema.setTableProperties(wrapperTableSchema.getTableProperties());
    if (wrapperTableSchema.getBucketingInfo() != null) {
      externalTableSchema.setBucketingInfo(
          fromWrapperToExternalBucketingInfo(wrapperTableSchema.getBucketingInfo()));
    }
    if (wrapperTableSchema.getPartitionInfo() != null) {
      externalTableSchema.setPartitionInfo(
          fromWrapperToExternalPartitionInfo(wrapperTableSchema.getPartitionInfo()));
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
        bucketingInfo.getNumOfRanges());
  }

  /* (non-Javadoc)
   * convert from wrapper to external tableinfo
   */
  @Override
  public org.apache.carbondata.format.TableInfo fromWrapperToExternalTableInfo(
      TableInfo wrapperTableInfo, String dbName, String tableName) {
    org.apache.carbondata.format.TableSchema thriftFactTable =
        fromWrapperToExternalTableSchema(wrapperTableInfo.getFactTable());
    return new org.apache.carbondata.format.TableInfo(thriftFactTable, new ArrayList<>());
  }

  private List<org.apache.carbondata.format.RelationIdentifier> fromWrapperToExternalRI(
      List<RelationIdentifier> relationIdentifiersList) {
    List<org.apache.carbondata.format.RelationIdentifier> thriftRelationIdentifierList =
        new ArrayList<>();
    for (RelationIdentifier relationIdentifier : relationIdentifiersList) {
      org.apache.carbondata.format.RelationIdentifier thriftRelationIdentifier =
          new org.apache.carbondata.format.RelationIdentifier();
      thriftRelationIdentifier.setDatabaseName(relationIdentifier.getDatabaseName());
      thriftRelationIdentifier.setTableName(relationIdentifier.getTableName());
      thriftRelationIdentifier.setTableId(relationIdentifier.getTableId());
      thriftRelationIdentifierList.add(thriftRelationIdentifier);
    }
    return thriftRelationIdentifierList;
  }

  private List<org.apache.carbondata.format.ParentColumnTableRelation> wrapperToThriftRelationList(
      List<ParentColumnTableRelation> wrapperColumnRelations) {
    List<org.apache.carbondata.format.ParentColumnTableRelation> thriftColumnRelationList =
        new ArrayList<>();

    for (ParentColumnTableRelation wrapperColumnRelation : wrapperColumnRelations) {
      org.apache.carbondata.format.ParentColumnTableRelation thriftColumnTableRelation =
          new org.apache.carbondata.format.ParentColumnTableRelation();
      thriftColumnTableRelation.setColumnId(wrapperColumnRelation.getColumnId());
      thriftColumnTableRelation.setColumnName(wrapperColumnRelation.getColumnName());
      org.apache.carbondata.format.RelationIdentifier thriftRelationIdentifier =
          new org.apache.carbondata.format.RelationIdentifier();
      thriftRelationIdentifier
          .setDatabaseName(wrapperColumnRelation.getRelationIdentifier().getDatabaseName());
      thriftRelationIdentifier
          .setTableName(wrapperColumnRelation.getRelationIdentifier().getTableName());
      thriftRelationIdentifier
          .setTableId(wrapperColumnRelation.getRelationIdentifier().getTableId());
      thriftColumnTableRelation.setRelationIdentifier(thriftRelationIdentifier);
      thriftColumnRelationList.add(thriftColumnTableRelation);
    }
    return thriftColumnRelationList;
  }

  /* (non-Javadoc)
   * convert from external to wrapper schema evolution entry
   */
  @Override
  public SchemaEvolutionEntry fromExternalToWrapperSchemaEvolutionEntry(
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
  @Override
  public SchemaEvolution fromExternalToWrapperSchemaEvolution(
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
      case DIRECT_COMPRESS_VARCHAR:
        return Encoding.DIRECT_COMPRESS_VARCHAR;
      case BIT_PACKED:
        return Encoding.BIT_PACKED;
      case INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY:
        return Encoding.INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY;
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
  private DataType fromExternalToWrapperDataType(org.apache.carbondata.format.DataType dataType,
      int precision, int scale) {
    if (null == dataType) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        return DataTypes.BOOLEAN;
      case STRING:
        return DataTypes.STRING;
      case INT:
        return DataTypes.INT;
      case SHORT:
        return DataTypes.SHORT;
      case LONG:
        return DataTypes.LONG;
      case DOUBLE:
        return DataTypes.DOUBLE;
      case DECIMAL:
        return DataTypes.createDecimalType(precision, scale);
      case TIMESTAMP:
        return DataTypes.TIMESTAMP;
      case DATE:
        return DataTypes.DATE;
      case BINARY:
        return DataTypes.BINARY;
      case ARRAY:
        return DataTypes.createDefaultArrayType();
      case STRUCT:
        return DataTypes.createDefaultStructType();
      case MAP:
        return DataTypes.createDefaultMapType();
      case VARCHAR:
        return DataTypes.VARCHAR;
      case FLOAT:
        return DataTypes.FLOAT;
      case BYTE:
        return DataTypes.BYTE;
      default:
        return DataTypes.STRING;
    }
  }

  /* (non-Javadoc)
   * convert from external to wrapper columnschema
   */
  @Override
  public ColumnSchema fromExternalToWrapperColumnSchema(
      org.apache.carbondata.format.ColumnSchema externalColumnSchema) {
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    wrapperColumnSchema.setColumnUniqueId(externalColumnSchema.getColumn_id());
    wrapperColumnSchema.setColumnName(externalColumnSchema.getColumn_name());
    wrapperColumnSchema.setDataType(
        fromExternalToWrapperDataType(
            externalColumnSchema.data_type,
            externalColumnSchema.precision,
            externalColumnSchema.scale));
    wrapperColumnSchema.setDimensionColumn(externalColumnSchema.isDimension());
    List<Encoding> encoders = new ArrayList<Encoding>();
    for (org.apache.carbondata.format.Encoding encoder : externalColumnSchema.getEncoders()) {
      encoders.add(fromExternalToWrapperEncoding(encoder));
    }
    wrapperColumnSchema.setEncodingList(encoders);
    wrapperColumnSchema.setNumberOfChild(externalColumnSchema.getNum_child());
    wrapperColumnSchema.setPrecision(externalColumnSchema.getPrecision());
    wrapperColumnSchema.setScale(externalColumnSchema.getScale());
    wrapperColumnSchema.setDefaultValue(externalColumnSchema.getDefault_value());
    wrapperColumnSchema.setInvisible(externalColumnSchema.isInvisible());
    wrapperColumnSchema.setColumnReferenceId(externalColumnSchema.getColumnReferenceId());
    wrapperColumnSchema.setSchemaOrdinal(externalColumnSchema.getSchemaOrdinal());
    wrapperColumnSchema.setIndexColumn(externalColumnSchema.isIndexColumn());
    wrapperColumnSchema.setSortColumn(false);
    Map<String, String> properties = externalColumnSchema.getColumnProperties();
    if (properties != null) {
      String sortColumns = properties.get(CarbonCommonConstants.SORT_COLUMNS);
      if (sortColumns != null) {
        wrapperColumnSchema.setSortColumn(true);
      }
      wrapperColumnSchema.setColumnProperties(externalColumnSchema.getColumnProperties());
    }
    wrapperColumnSchema.setFunction(externalColumnSchema.getAggregate_function());
    List<org.apache.carbondata.format.ParentColumnTableRelation> parentColumnTableRelation =
        externalColumnSchema.getParentColumnTableRelations();
    if (null != parentColumnTableRelation) {
      wrapperColumnSchema.setParentColumnTableRelations(
          fromExternalToWrapperParentTableColumnRelations(parentColumnTableRelation));
    }
    return wrapperColumnSchema;
  }

  private PartitionType fromExternalToWrapperPartitionType(
      org.apache.carbondata.format.PartitionType externalPartitionType) {
    if (null == externalPartitionType) {
      return null;
    }
    switch (externalPartitionType) {
      case HASH:
      case LIST:
      case RANGE:
      case RANGE_INTERVAL:
        DeprecatedFeatureException.customPartitionNotSupported();
        return null;
      default:
        return PartitionType.NATIVE_HIVE;
    }
  }

  private PartitionInfo fromExternalToWrapperPartitionInfo(
      org.apache.carbondata.format.PartitionInfo externalPartitionInfo) {
    List<ColumnSchema> wrapperColumnSchema = new ArrayList<ColumnSchema>();
    for (org.apache.carbondata.format.ColumnSchema columnSchema :
        externalPartitionInfo.getPartition_columns()) {
      wrapperColumnSchema.add(fromExternalToWrapperColumnSchema(columnSchema));
    }
    return new PartitionInfo(wrapperColumnSchema,
        fromExternalToWrapperPartitionType(externalPartitionInfo.getPartition_type()));
  }

  /* (non-Javadoc)
   * convert from external to wrapper tableschema
   */
  @Override
  public TableSchema fromExternalToWrapperTableSchema(
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
    if (null != externalTableSchema.tableProperties) {
      CarbonUtil
          .setLocalDictColumnsToWrapperSchema(listOfColumns, externalTableSchema.tableProperties,
              externalTableSchema.tableProperties
                  .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE));
    }
    wrapperTableSchema.setListOfColumns(listOfColumns);
    wrapperTableSchema.setSchemaEvolution(
        fromExternalToWrapperSchemaEvolution(externalTableSchema.getSchema_evolution()));
    if (externalTableSchema.isSetBucketingInfo()) {
      wrapperTableSchema.setBucketingInfo(
          fromExternalToWrapperBucketingInfo(externalTableSchema.bucketingInfo));
    }
    if (externalTableSchema.getPartitionInfo() != null) {
      wrapperTableSchema.setPartitionInfo(
          fromExternalToWrapperPartitionInfo(externalTableSchema.getPartitionInfo()));
    }
    return wrapperTableSchema;
  }

  private BucketingInfo fromExternalToWrapperBucketingInfo(
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
  @Override
  public TableInfo fromExternalToWrapperTableInfo(
      org.apache.carbondata.format.TableInfo externalTableInfo,
      String dbName,
      String tableName,
      String tablePath) {
    TableInfo wrapperTableInfo = new TableInfo();
    List<org.apache.carbondata.format.SchemaEvolutionEntry> schemaEvolutionList =
        externalTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history();
    wrapperTableInfo.setLastUpdatedTime(
        schemaEvolutionList.get(schemaEvolutionList.size() - 1).getTime_stamp());
    wrapperTableInfo.setDatabaseName(dbName);
    wrapperTableInfo.setTableUniqueName(CarbonTable.buildUniqueName(dbName, tableName));
    wrapperTableInfo.setFactTable(
        fromExternalToWrapperTableSchema(externalTableInfo.getFact_table(), tableName));
    wrapperTableInfo.setTablePath(tablePath);
    return wrapperTableInfo;
  }

  private List<ParentColumnTableRelation> fromExternalToWrapperParentTableColumnRelations(
      List<org.apache.carbondata.format.ParentColumnTableRelation> thirftParentColumnRelation) {
    List<ParentColumnTableRelation> parentColumnTableRelationList = new ArrayList<>();
    for (org.apache.carbondata.format.ParentColumnTableRelation carbonTableRelation :
        thirftParentColumnRelation) {
      RelationIdentifier relationIdentifier =
          new RelationIdentifier(carbonTableRelation.getRelationIdentifier().getDatabaseName(),
              carbonTableRelation.getRelationIdentifier().getTableName(),
              carbonTableRelation.getRelationIdentifier().getTableId());
      ParentColumnTableRelation parentColumnTableRelation =
          new ParentColumnTableRelation(relationIdentifier, carbonTableRelation.getColumnId(),
              carbonTableRelation.getColumnName());
      parentColumnTableRelationList.add(parentColumnTableRelation);
    }
    return parentColumnTableRelationList;
  }
}
