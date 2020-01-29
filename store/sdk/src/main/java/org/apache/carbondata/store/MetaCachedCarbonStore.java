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

package org.apache.carbondata.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * A CarbonStore base class that caches CarbonTable object
 */
@InterfaceAudience.Internal
abstract class MetaCachedCarbonStore implements CarbonStore {

  // mapping of table path to CarbonTable object
  private Map<String, CarbonTable> cache = new HashMap<>();

  CarbonTable getTable(String path) throws IOException {
    if (cache.containsKey(path)) {
      return cache.get(path);
    }
    String schemaPath = CarbonTablePath.getSchemaFilePath(path);
    TableInfo tableInfo;
    if (!FileFactory.isFileExist(schemaPath)) {
      tableInfo = SchemaReader.inferSchema(AbsoluteTableIdentifier.from(path), false);
    } else {
      org.apache.carbondata.format.TableInfo tableInfoFormat;
      tableInfoFormat = CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(path));
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      tableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfoFormat, "", "", "");
      tableInfo.setTablePath(path);
    }
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    cache.put(path, table);
    return table;
  }

  @Override
  public void close() {
    cache.clear();
  }
}
