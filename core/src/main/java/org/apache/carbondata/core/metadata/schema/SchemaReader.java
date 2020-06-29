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

package org.apache.carbondata.core.metadata.schema;

import java.io.IOException;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;

/**
 * TODO: It should be removed after store manager implementation.
 */
public class SchemaReader {

  public static CarbonTable readCarbonTableFromStore(AbsoluteTableIdentifier identifier)
      throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025
    String schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3404
    if (FileFactory.isFileExist(schemaFilePath)) {
      String tableName = identifier.getCarbonTableIdentifier().getTableName();

      org.apache.carbondata.format.TableInfo tableInfo =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025
          CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(identifier.getTablePath()));
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1844
      TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
          tableInfo,
          identifier.getCarbonTableIdentifier().getDatabaseName(),
          tableName,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1573
          identifier.getTablePath());
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
      return CarbonMetadata.getInstance().getCarbonTable(
          identifier.getCarbonTableIdentifier().getTableUniqueName());
    } else {
      throw new IOException("File does not exist: " + schemaFilePath);
    }
  }

  /**
   * Read specified schema file as CarbonTable
   * @param schemaFilePath schema file path
   * @param conf hadoop configuration
   * @return CarbonTable object
   * @throws IOException if IO error occurs
   */
  public static CarbonTable readCarbonTableFromSchema(String schemaFilePath, Configuration conf)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3553
      throws IOException {
    TableInfo tableInfo = readTableInfoFromSchema(schemaFilePath, conf);
    return CarbonTable.buildFromTableInfo(tableInfo);
  }

  /**
   * Read specified schema file as TableInfo
   * @param schemaFilePath schema file path
   * @param conf hadoop configuration
   * @return TableInfo object
   * @throws IOException if IO error occurs
   */
  private static TableInfo readTableInfoFromSchema(String schemaFilePath, Configuration conf)
      throws IOException {
    if (FileFactory.isFileExist(schemaFilePath)) {
      String tableName = "dummy";

      org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(schemaFilePath);
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
          tableInfo,
          "dummy",
          tableName,
          "dummy");
      return wrapperTableInfo;
    } else {
      throw new IOException("File does not exist: " + schemaFilePath);
    }
  }

  /**
   * the method returns the Wrapper TableInfo
   *
   * @param identifier
   * @return
   */
  public static TableInfo getTableInfo(AbsoluteTableIdentifier identifier)
      throws IOException {
    org.apache.carbondata.format.TableInfo thriftTableInfo =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025
        CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(identifier.getTablePath()));
    ThriftWrapperSchemaConverterImpl thriftWrapperSchemaConverter =
        new ThriftWrapperSchemaConverterImpl();
    CarbonTableIdentifier carbonTableIdentifier =
        identifier.getCarbonTableIdentifier();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1903
    return thriftWrapperSchemaConverter.fromExternalToWrapperTableInfo(
        thriftTableInfo,
        carbonTableIdentifier.getDatabaseName(),
        carbonTableIdentifier.getTableName(),
        identifier.getTablePath());
  }

  public static TableInfo inferSchema(AbsoluteTableIdentifier identifier,
      boolean isCarbonFileProvider, Configuration configuration) throws IOException {

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2909
    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil
        .inferSchema(identifier.getTablePath(), identifier.getTableName(), isCarbonFileProvider,
            configuration);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        tableInfo, identifier.getDatabaseName(), identifier.getTableName(),
        identifier.getTablePath());
    wrapperTableInfo.setTransactionalTable(false);
    return wrapperTableInfo;
  }

  public static TableInfo inferSchema(AbsoluteTableIdentifier identifier,
      boolean isCarbonFileProvider) throws IOException {

    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil
        .inferSchema(identifier.getTablePath(), identifier.getTableName(), isCarbonFileProvider,
            FileFactory.getConfiguration());
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        tableInfo, identifier.getDatabaseName(), identifier.getTableName(),
        identifier.getTablePath());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2360
    wrapperTableInfo.setTransactionalTable(false);
    return wrapperTableInfo;
  }
}
