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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.sdk.file.CarbonReader;

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
    TableInfo schema = CarbonReader.readSchemaFile(CarbonTablePath.getSchemaFilePath(path));
    schema.setTablePath(path);
    CarbonTable table = CarbonTable.buildFromTableInfo(schema);
    cache.put(path, table);
    return table;
  }

  @Override
  public void close() throws IOException {
    cache.clear();
  }
}
