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

package org.apache.carbondata.core.carbon.metadata.schema;

import java.io.IOException;

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.thrift.TBase;

public class TableSchemaUpdater {
  public static void updateTableSchemaBlockSize(String storePath,
      CarbonTableIdentifier tableIdentifier, int blocksize) throws IOException {
    //Read TableInfo from store
    String tableName = tableIdentifier.getTableName();
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(storePath, tableIdentifier);
    String schemaFilePath = carbonTablePath.getSchemaFilePath();

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
    TableInfo newTableInfo = schemaConverter
        .fromExternalToWrapperTableInfo(tableInfo,
            tableIdentifier.getDatabaseName(), tableName,
            storePath);
    // Update the new TableInfo
    newTableInfo.setLastUpdatedTime(System.currentTimeMillis());
    // Update the blocksize for one table
    newTableInfo.getFactTable().setBlockszie(blocksize);

    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    // TODO make sure where the blocksize would be used and RE-loadTableMetadata to set this value.
    CarbonMetadata.getInstance().loadTableMetadata(newTableInfo);

    org.apache.carbondata.format.TableInfo thriftTableInfo = schemaConverter
        .fromWrapperToExternalTableInfo(newTableInfo, newTableInfo.getDatabaseName(),
            newTableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry(newTableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);

    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
      thriftWriter.open();
      thriftWriter.write(thriftTableInfo);
      thriftWriter.close();
    }

  }

}
