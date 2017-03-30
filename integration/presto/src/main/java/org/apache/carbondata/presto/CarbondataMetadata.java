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

import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.presto.impl.CarbonTableReader;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import javax.inject.Inject;
import java.util.*;

import static org.apache.carbondata.presto.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CarbondataMetadata implements ConnectorMetadata {
  private final String connectorId;
  private CarbonTableReader carbonTableReader;
  private ClassLoader classLoader;

  private Map<String, ColumnHandle> columnHandleMap;

  @Inject public CarbondataMetadata(CarbondataConnectorId connectorId, CarbonTableReader reader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = requireNonNull(reader, "client is null");
  }

  public void putClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override public List<String> listSchemaNames(ConnectorSession session) {
    return listSchemaNamesInternal();
  }

  public List<String> listSchemaNamesInternal() {
    List<String> ret;
    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
      ret = carbonTableReader.getSchemaNames();
    }
    return ret;
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {

        /*List<SchemaTableName> all = carbonTableReader.getTableList();
        if(schemaNameOrNull != null)
        {
            return all.stream().filter(a -> schemaNameOrNull.equals(a.getSchemaName())).collect(Collectors.toList());
        }
        return all;*/

    List<String> schemaNames;
    if (schemaNameOrNull != null) {
      schemaNames = ImmutableList.of(schemaNameOrNull);
    } else {
      schemaNames = carbonTableReader.getSchemaNames();
    }

    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
    for (String schemaName : schemaNames) {
      for (String tableName : carbonTableReader.getTableNames(schemaName)) {
        builder.add(new SchemaTableName(schemaName, tableName));
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

  private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
    if (!listSchemaNamesInternal().contains(tableName.getSchemaName())) {
      return null;
    }

    CarbonTable cb = carbonTableReader.getTable(tableName);
    if (cb == null) {
      return null;
    }

    List<ColumnMetadata> spiCols = new LinkedList<>();
    List<CarbonColumn> carbonColumns = cb.getCreateOrderColumn(tableName.getTableName());
    for (CarbonColumn col : carbonColumns) {
      //show columns command will return these data
      Type spiType = CarbondataType2SpiMapper(col.getColumnSchema().getDataType());
      ColumnMetadata spiCol = new ColumnMetadata(col.getColumnSchema().getColumnName(), spiType);
      spiCols.add(spiCol);
    }

    //封装carbonTable
    return new ConnectorTableMetadata(tableName, spiCols);
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
    if (cb == null) {
      throw new TableNotFoundException(handle.getSchemaTableName());
    }

    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
    String tableName = handle.getSchemaTableName().getTableName();
    for (CarbonDimension column : cb.getDimensionByTableName(tableName)) {
      ColumnSchema cs = column.getColumnSchema();

      int complex = column.getComplexTypeOrdinal();
      column.getNumberOfChild();
      column.getListOfChildDimensions();

      Type spiType = CarbondataType2SpiMapper(cs.getDataType());
      columnHandles.put(cs.getColumnName(),
          new CarbondataColumnHandle(connectorId, cs.getColumnName(), spiType, column.getSchemaOrdinal(),
              column.getKeyOrdinal(), column.getColumnGroupOrdinal(), false, cs.getColumnGroupId(),
              cs.getColumnUniqueId(), cs.isUseInvertedIndex()));
    }

    for (CarbonMeasure measure : cb.getMeasureByTableName(tableName)) {
      ColumnSchema cs = measure.getColumnSchema();

      Type spiType = CarbondataType2SpiMapper(cs.getDataType());
      columnHandles.put(cs.getColumnName(),
          new CarbondataColumnHandle(connectorId, cs.getColumnName(), spiType, cs.getSchemaOrdinal(),
              measure.getOrdinal(), cs.getColumnGroupId(), true, cs.getColumnGroupId(),
              cs.getColumnUniqueId(), cs.isUseInvertedIndex()));
    }

    //should i cache it?
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
    //check tablename is valid
    //schema is exist
    //tables is exist

    //CarbondataTable  get from jar
    return new CarbondataTableHandle(connectorId, tableName);
  }

  @Override public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
      ConnectorTableHandle table, Constraint<ColumnHandle> constraint,
      Optional<Set<ColumnHandle>> desiredColumns) {
    CarbondataTableHandle handle = checkType(table, CarbondataTableHandle.class, "table");
    ConnectorTableLayout layout = new ConnectorTableLayout(
        new CarbondataTableLayoutHandle(handle, constraint.getSummary()/*, constraint.getPredicateMap(),constraint.getFilterTuples()*/));
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

  public static Type CarbondataType2SpiMapper(DataType colType) {
    switch (colType) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case SHORT:
        return SmallintType.SMALLINT;
      case INT:
        return IntegerType.INTEGER;
      case LONG:
        return BigintType.BIGINT;
      case FLOAT:
      case DOUBLE:
        return DoubleType.DOUBLE;

      case DECIMAL:
        return DecimalType.createDecimalType();
      case STRING:
        return VarcharType.VARCHAR;
      case DATE:
        return DateType.DATE;
      case TIMESTAMP:
        return TimestampType.TIMESTAMP;

            /*case DataType.MAP:
            case DataType.ARRAY:
            case DataType.STRUCT:
            case DataType.NULL:*/

      default:
        return VarcharType.VARCHAR;
    }
  }

}