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

import org.carbondata.core.carbon.metadata.schema.SchemaEvolution;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolutionEntry;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.carbondata.core.carbon.metadata.schema.table.TableSchema;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

/**
 * Converter interface which will be implemented for external to carbon schema
 */
public interface SchemaConverter {
  /**
   * @param wrapperSchemaEvolutionEntry
   * @return
   */
  org.carbondata.format.SchemaEvolutionEntry fromWrapperToExternalSchemaEvolutionEntry(
      SchemaEvolutionEntry wrapperSchemaEvolutionEntry);

  /**
   * @param wrapperSchemaEvolution
   * @return
   */
  org.carbondata.format.SchemaEvolution fromWrapperToExternalSchemaEvolution(
      SchemaEvolution wrapperSchemaEvolution);

  /**
   * @param wrapperColumnSchema
   * @return
   */
  org.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
      ColumnSchema wrapperColumnSchema);

  /**
   * @param wrapperTableSchema
   * @return
   */
  org.carbondata.format.TableSchema fromWrapperToExternalTableSchema(
      TableSchema wrapperTableSchema);

  /**
   * @param wrapperTableInfo
   * @param dbName
   * @param tableName
   * @return
   */
  org.carbondata.format.TableInfo fromWrapperToExternalTableInfo(TableInfo wrapperTableInfo,
      String dbName, String tableName);

  /**
   * @param externalSchemaEvolutionEntry
   * @return
   */
  SchemaEvolutionEntry fromExternalToWrapperSchemaEvolutionEntry(
      org.carbondata.format.SchemaEvolutionEntry externalSchemaEvolutionEntry);

  /**
   * @param externalSchemaEvolution
   * @return
   */
  SchemaEvolution fromExternalToWrapperSchemaEvolution(
      org.carbondata.format.SchemaEvolution externalSchemaEvolution);

  /**
   * @param externalColumnSchema
   * @return
   */
  ColumnSchema fromExternalToWrapperColumnSchema(
      org.carbondata.format.ColumnSchema externalColumnSchema);

  /**
   * @param externalTableSchema
   * @param tableNam
   * @return
   */
  TableSchema fromExternalToWrapperTableSchema(
      org.carbondata.format.TableSchema externalTableSchema, String tableNam);

  /**
   * @param externalTableInfo
   * @param dbName
   * @param tableName
   * @return
   */
  TableInfo fromExternalToWrapperTableInfo(org.carbondata.format.TableInfo externalTableInfo,
      String dbName, String tableName, String storePath);
}
