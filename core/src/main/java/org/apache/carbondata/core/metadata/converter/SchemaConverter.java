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

import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Converter interface which will be implemented for external to carbon schema
 */
public interface SchemaConverter {
  /**
   * @param wrapperSchemaEvolutionEntry
   * @return
   */
  org.apache.carbondata.format.SchemaEvolutionEntry fromWrapperToExternalSchemaEvolutionEntry(
      SchemaEvolutionEntry wrapperSchemaEvolutionEntry);

  /**
   * @param wrapperSchemaEvolution
   * @return
   */
  org.apache.carbondata.format.SchemaEvolution fromWrapperToExternalSchemaEvolution(
      SchemaEvolution wrapperSchemaEvolution);

  /**
   * @param wrapperColumnSchema
   * @return
   */
  org.apache.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
      ColumnSchema wrapperColumnSchema);

  /**
   * @param wrapperTableSchema
   * @return
   */
  org.apache.carbondata.format.TableSchema fromWrapperToExternalTableSchema(
      TableSchema wrapperTableSchema);

  /**
   * @param wrapperTableInfo
   * @param dbName
   * @param tableName
   * @return
   */
  org.apache.carbondata.format.TableInfo fromWrapperToExternalTableInfo(TableInfo wrapperTableInfo,
      String dbName, String tableName);

  /**
   * @param externalSchemaEvolutionEntry
   * @return
   */
  SchemaEvolutionEntry fromExternalToWrapperSchemaEvolutionEntry(
      org.apache.carbondata.format.SchemaEvolutionEntry externalSchemaEvolutionEntry);

  /**
   * @param externalSchemaEvolution
   * @return
   */
  SchemaEvolution fromExternalToWrapperSchemaEvolution(
      org.apache.carbondata.format.SchemaEvolution externalSchemaEvolution);

  /**
   * @param externalColumnSchema
   * @return
   */
  ColumnSchema fromExternalToWrapperColumnSchema(
      org.apache.carbondata.format.ColumnSchema externalColumnSchema);

  /**
   * @param externalTableSchema
   * @param tableNam
   * @return
   */
  TableSchema fromExternalToWrapperTableSchema(
      org.apache.carbondata.format.TableSchema externalTableSchema, String tableNam);

  /**
   * method to convert thrift table info object to wrapper table info
   *
   * @param externalTableInfo thrift table info object
   * @param dbName database name
   * @param tableName table name
   * @param tablePath table path
   * @return TableInfo
   */
  TableInfo fromExternalToWrapperTableInfo(
      org.apache.carbondata.format.TableInfo externalTableInfo,
      String dbName,
      String tableName,
      String tablePath);

}
