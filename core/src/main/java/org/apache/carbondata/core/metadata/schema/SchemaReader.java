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

/**
 * TODO: It should be removed after store manager implementation.
 */
public class SchemaReader {

  public static CarbonTable readCarbonTableFromStore(AbsoluteTableIdentifier identifier)
      throws IOException {
    String schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath());
    if (FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.LOCAL) ||
        FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.HDFS) ||
        FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.S3) ||
        FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.VIEWFS)) {
      String tableName = identifier.getCarbonTableIdentifier().getTableName();

      org.apache.carbondata.format.TableInfo tableInfo =
          CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(identifier.getTablePath()));
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
          tableInfo,
          identifier.getCarbonTableIdentifier().getDatabaseName(),
          tableName,
          identifier.getTablePath());
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
      return CarbonMetadata.getInstance().getCarbonTable(
          identifier.getCarbonTableIdentifier().getTableUniqueName());
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
        CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(identifier.getTablePath()));
    ThriftWrapperSchemaConverterImpl thriftWrapperSchemaConverter =
        new ThriftWrapperSchemaConverterImpl();
    CarbonTableIdentifier carbonTableIdentifier =
        identifier.getCarbonTableIdentifier();
    return thriftWrapperSchemaConverter.fromExternalToWrapperTableInfo(
        thriftTableInfo,
        carbonTableIdentifier.getDatabaseName(),
        carbonTableIdentifier.getTableName(),
        identifier.getTablePath());
  }

  public static TableInfo inferSchema(AbsoluteTableIdentifier identifier,
      boolean isCarbonFileProvider) throws IOException {

    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil
        .inferSchema(identifier.getTablePath(), identifier.getTableName(), isCarbonFileProvider);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        tableInfo, identifier.getDatabaseName(), identifier.getTableName(),
        identifier.getTablePath());
    wrapperTableInfo.setTransactionalTable(false);
    return wrapperTableInfo;
  }
}
