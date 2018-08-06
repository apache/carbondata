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

package org.apache.carbondata.presto;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import static org.apache.carbondata.presto.Types.checkType;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static com.google.common.base.Preconditions.checkArgument;

public class CarbondataMetadata implements ConnectorMetadata {
  private final String connectorId;
  private CarbonTableReader carbonTableReader;

  private Map<String, ColumnHandle> columnHandleMap;

  @Inject public CarbondataMetadata(CarbondataConnectorId connectorId, CarbonTableReader reader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = requireNonNull(reader, "client is null");
  }


  @Override public List<String> listSchemaNames(ConnectorSession session) {
    return listSchemaNamesInternal();
  }

  public List<String> listSchemaNamesInternal() {
    return carbonTableReader.getSchemaNames();
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {

    List<String> schemaNames;
    if (schemaNameOrNull != null) {
      schemaNames = ImmutableList.of(schemaNameOrNull);
    } else {
      schemaNames = carbonTableReader.getSchemaNames();
    }

    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
    for (String schemaName : schemaNames) {
      for (String tableName : carbonTableReader.getTableNames(schemaName)) {
        if (!tableName.equalsIgnoreCase(".DS_Store")) {
          builder.add(new SchemaTableName(schemaName, tableName));
        }
      }
    }
    return builder.build();
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
      SchemaTablePrefix prefix) {
    requireNonNull(prefix, "SchemaTablePrefix is null");

    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
    for (SchemaTableName tableName : listTables(session, prefix)) {
      ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
      if (tableMetadata != null) {
        columns.put(tableName, tableMetadata.getColumns());
      }
    }
    return columns.build();
  }

  //if prefix is null. return all tables
  //if prefix is not null, just return this table
  private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
    if (prefix.getSchemaName() == null) {
      return listTables(session, prefix.getSchemaName());
    }
    return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
  }

  private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
    if (!listSchemaNamesInternal().contains(schemaTableName.getSchemaName())) {
      throw new SchemaNotFoundException(schemaTableName.getSchemaName());
    }

    CarbonTable carbonTable = carbonTableReader.getTable(schemaTableName);

    List<ColumnMetadata> columnsMetaList = new LinkedList<>();
    List<CarbonColumn> carbonColumns =
        carbonTable.getCreateOrderColumn(schemaTableName.getTableName());
    for (CarbonColumn col : carbonColumns) {
      //show columns command will return these data
      ColumnSchema columnSchema = col.getColumnSchema();
      Type columnType = carbonDataType2SpiMapper(columnSchema);
      String extraValues =
          columnSchema.getEncodingList().stream().map(encoding -> encoding.toString() + " ")
              .reduce("", String::concat);
      ColumnMetadata columnMeta =
          new ColumnMetadata(columnSchema.getColumnName(), columnType, "", extraValues, false);
      columnsMetaList.add(columnMeta);
    }

    //carbondata connector's table metadata
    return new ConnectorTableMetadata(schemaTableName, columnsMetaList);
  }

  @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
      ConnectorTableHandle tableHandle) {

    CarbondataTableHandle handle =
        checkType(tableHandle, CarbondataTableHandle.class, "tableHandle");
    checkArgument(handle.getConnectorId().equals(connectorId),
        "tableHandle is not for this connector");

    String schemaName = handle.getSchemaTableName().getSchemaName();

    if (!listSchemaNamesInternal().contains(schemaName)) {
      throw new SchemaNotFoundException(schemaName);
    }

    //CarbonTable(official struct) is stored in CarbonMetadata(official struct)
    CarbonTable cb = carbonTableReader.getTable(handle.getSchemaTableName());

    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
    String tableName = handle.getSchemaTableName().getTableName();
    for (CarbonDimension column : cb.getDimensionByTableName(tableName)) {
      ColumnSchema cs = column.getColumnSchema();

      Type spiType = carbonDataType2SpiMapper(cs);
      columnHandles.put(cs.getColumnName(),
          new CarbondataColumnHandle(connectorId, cs.getColumnName(), spiType,
              column.getSchemaOrdinal(), column.getKeyOrdinal(), false, cs.getColumnUniqueId(),
              cs.isUseInvertedIndex(), cs.getPrecision(), cs.getScale()));
    }

    for (CarbonMeasure measure : cb.getMeasureByTableName(tableName)) {
      ColumnSchema cs = measure.getColumnSchema();
      Type spiType = carbonDataType2SpiMapper(cs);
      columnHandles.put(cs.getColumnName(),
          new CarbondataColumnHandle(connectorId, cs.getColumnName(), spiType,
              cs.getSchemaOrdinal(), measure.getOrdinal(), true, cs.getColumnUniqueId(),
              cs.isUseInvertedIndex(), cs.getPrecision(), cs.getScale()));
    }

    columnHandleMap = columnHandles.build();

    return columnHandleMap;
  }

  @Override public ColumnMetadata getColumnMetadata(ConnectorSession session,
      ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {

    checkType(tableHandle, CarbondataTableHandle.class, "tableHandle");
    return checkType(columnHandle, CarbondataColumnHandle.class, "columnHandle")
        .getColumnMetadata();
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    return new CarbondataTableHandle(connectorId, tableName);
  }

  @Override public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
      ConnectorTableHandle table, Constraint<ColumnHandle> constraint,
      Optional<Set<ColumnHandle>> desiredColumns) {
    CarbondataTableHandle handle = checkType(table, CarbondataTableHandle.class, "table");
    ConnectorTableLayout layout = new ConnectorTableLayout(
        new CarbondataTableLayoutHandle(handle, constraint.getSummary()));
    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  @Override public ConnectorTableLayout getTableLayout(ConnectorSession session,
      ConnectorTableLayoutHandle handle) {
    return new ConnectorTableLayout(handle);
  }

  @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
      ConnectorTableHandle table) {
    return getTableMetadataInternal(table);
  }

  public ConnectorTableMetadata getTableMetadataInternal(ConnectorTableHandle table) {
    CarbondataTableHandle carbondataTableHandle =
        checkType(table, CarbondataTableHandle.class, "table");
    checkArgument(carbondataTableHandle.getConnectorId().equals(connectorId),
        "tableHandle is not for this connector");
    return getTableMetadata(carbondataTableHandle.getSchemaTableName());
  }

  public static Type carbonDataType2SpiMapper(ColumnSchema columnSchema) {
    DataType colType = columnSchema.getDataType();
    if (colType == DataTypes.BOOLEAN) {
      return BooleanType.BOOLEAN;
    } else if (colType == DataTypes.SHORT) {
      return SmallintType.SMALLINT;
    } else if (colType == DataTypes.INT) {
      return IntegerType.INTEGER;
    } else if (colType == DataTypes.LONG) {
      return BigintType.BIGINT;
    } else if (colType == DataTypes.FLOAT || colType == DataTypes.DOUBLE) {
      return DoubleType.DOUBLE;
    } else if (DataTypes.isDecimal(colType)) {
      if (columnSchema.getPrecision() > 0) {
        return DecimalType.createDecimalType(columnSchema.getPrecision(), columnSchema.getScale());
      } else {
        return DecimalType.createDecimalType();
      }
    } else if (colType == DataTypes.STRING) {
      return VarcharType.VARCHAR;
    } else if (colType == DataTypes.DATE) {
      return DateType.DATE;
    } else if (colType == DataTypes.TIMESTAMP) {
      return TimestampType.TIMESTAMP;
    } else {
      return VarcharType.VARCHAR;
    }
  }

}