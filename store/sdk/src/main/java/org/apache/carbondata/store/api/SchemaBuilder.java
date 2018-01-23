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

package org.apache.carbondata.store.api;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

public class SchemaBuilder {

  private int ordinal = 0;

  private List<ColumnSchema> sortColumns = new LinkedList<>();

  private List<ColumnSchema> otherColumns = new LinkedList<>();

  private TableSchema tableSchema;

  private SchemaBuilder(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public static SchemaBuilder newInstance() {
    TableSchema schema = new TableSchema();
    schema.setTableId(UUID.randomUUID().toString());
    schema.setPartitionInfo(null);
    schema.setBucketingInfo(null);
    SchemaEvolution schemaEvol = new SchemaEvolution();
    schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
    schema.setSchemaEvalution(schemaEvol);
    return new SchemaBuilder(schema);
  }

  public SchemaBuilder addColumn(StructField field, boolean isSortColumn) {
    ColumnSchema newColumn = new ColumnSchema();
    newColumn.setColumnName(field.getFieldName());
    newColumn.setDataType(field.getDataType());
    newColumn.setDimensionColumn(isSortColumn);
    newColumn.setSchemaOrdinal(ordinal++);
    newColumn.setColumnar(true);
    newColumn.setColumnUniqueId(UUID.randomUUID().toString());
    newColumn.setColumnReferenceId(newColumn.getColumnUniqueId());
    newColumn.setEncodingList(createEncoding(field.getDataType(), isSortColumn));

    if (isSortColumn) {
      sortColumns.add(newColumn);
    } else {
      otherColumns.add(newColumn);
    }
    return this;
  }

  public TableSchema create() {
    List<ColumnSchema> allColumns = new LinkedList<>(sortColumns);
    allColumns.addAll(otherColumns);
    tableSchema.setListOfColumns(allColumns);
    return tableSchema;
  }

  private List<Encoding> createEncoding(DataType dataType, boolean isSortColumn) {
    List<Encoding> encodings = new LinkedList<>();
    if (dataType == DataTypes.TIMESTAMP || dataType == DataTypes.DATE) {
      encodings.add(Encoding.DIRECT_DICTIONARY);
    }
    if (isSortColumn) {
      encodings.add(Encoding.INVERTED_INDEX);
    }
    return encodings;
  }


}
