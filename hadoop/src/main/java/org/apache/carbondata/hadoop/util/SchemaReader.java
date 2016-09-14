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
package org.apache.carbondata.hadoop.util;

import java.io.IOException;

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.reader.ThriftReader;

import org.apache.thrift.TBase;

/**
 * TODO: It should be removed after store manager implemetation.
 */
public class SchemaReader {

  public CarbonTable readCarbonTableFromStore(CarbonTablePath carbonTablePath,
      CarbonTableIdentifier tableIdentifier, String storePath) throws IOException {
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    if (FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.HDFS) ||
        FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.VIEWFS) ||
        FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.LOCAL)) {
      String tableName = tableIdentifier.getTableName();

      ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
        public TBase create() {
          return new org.apache.carbondata.format.TableInfo();
        }
      };
      ThriftReader thriftReader =
          new ThriftReader(carbonTablePath.getSchemaFilePath(), createTBase);
      thriftReader.open();
      org.apache.carbondata.format.TableInfo tableInfo =
          (org.apache.carbondata.format.TableInfo) thriftReader.read();
      thriftReader.close();

      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo wrapperTableInfo = schemaConverter
          .fromExternalToWrapperTableInfo(tableInfo, tableIdentifier.getDatabaseName(), tableName,
              storePath);
      wrapperTableInfo.setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath));
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
      return CarbonMetadata.getInstance().getCarbonTable(tableIdentifier.getTableUniqueName());
    }
    return null;
  }
}
