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

package org.apache.carbondata.datamap;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapCatalog;
import org.apache.carbondata.core.datamap.DataMapProvider;
import org.apache.carbondata.core.datamap.DataMapRegistry;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.dev.IndexDataMap;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;

@InterfaceAudience.Internal
public class IndexDataMapProvider implements DataMapProvider {

  public IndexDataMapProvider() {
  }

  @Override
  public void initMeta(CarbonTable mainTable, DataMapSchema dataMapSchema, String ctasSqlStatement)
      throws MalformedDataMapCommandException, IOException {
    if (mainTable == null) {
      throw new MalformedDataMapCommandException(
          "Parent table is required to create index datamap");
    }
    ArrayList<RelationIdentifier> relationIdentifiers = new ArrayList<>();
    RelationIdentifier relationIdentifier =
        new RelationIdentifier(mainTable.getDatabaseName(), mainTable.getTableName(),
            mainTable.getTableInfo().getFactTable().getTableId());
    relationIdentifiers.add(relationIdentifier);
    dataMapSchema.setRelationIdentifier(relationIdentifier);
    dataMapSchema.setParentTables(relationIdentifiers);
    IndexDataMap indexDataMap = createIndexDataMapFactory(dataMapSchema);
    DataMapStoreManager.getInstance().registerDataMap(mainTable, dataMapSchema, indexDataMap);
    DataMapStoreManager.getInstance().saveDataMapSchema(dataMapSchema);
  }

  @Override
  public void initData(CarbonTable mainTable) {
    // Nothing is needed to do by default
  }

  @Override
  public void freeMeta(CarbonTable mainTable, DataMapSchema dataMapSchema) throws IOException {
    if (mainTable == null) {
      throw new UnsupportedOperationException("Table need to be specified in index datamaps");
    }
    DataMapStoreManager.getInstance().dropDataMapSchema(dataMapSchema.getDataMapName());
  }

  @Override
  public void freeData(CarbonTable mainTable, DataMapSchema dataMapSchema) {
    if (mainTable == null) {
      throw new UnsupportedOperationException("Table need to be specified in index datamaps");
    }
    DataMapStoreManager.getInstance().clearDataMap(
        mainTable.getAbsoluteTableIdentifier(), dataMapSchema.getDataMapName());
  }

  @Override
  public void rebuild(CarbonTable mainTable, DataMapSchema dataMapSchema) {
    // create a RDD to rebuild the index

  }

  @Override
  public void incrementalBuild(CarbonTable mainTable, DataMapSchema dataMapSchema,
      String[] segmentIds) {
    throw new UnsupportedOperationException();
  }

  private IndexDataMap createIndexDataMapFactory(DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    IndexDataMap indexDataMap;
    try {
      // try to create DataMapClassProvider instance by taking providerName as class name
      Class<? extends IndexDataMap> providerClass =
          (Class<? extends IndexDataMap>) Class.forName(dataMapSchema.getProviderName());
      indexDataMap = providerClass.newInstance();
    } catch (ClassNotFoundException e) {
      // try to create DataMapClassProvider instance by taking providerName as short name
      indexDataMap = getDataMapByShortName(dataMapSchema.getProviderName());
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to create DataMapClassProvider '" + dataMapSchema.getProviderName() + "'", e);
    }
    return indexDataMap;
  }

  public static IndexDataMap getDataMapByShortName(String providerName)
      throws MalformedDataMapCommandException {
    try {
      DataMapRegistry.registerDataMap(
          DataMapClassProvider.getDataMapProviderOnName(providerName).getClassName(),
          DataMapClassProvider.getDataMapProviderOnName(providerName).getShortName());
    } catch (UnsupportedOperationException ex) {
      throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found", ex);
    }
    IndexDataMap indexDataMap;
    String className = DataMapRegistry.getDataMapClassName(providerName.toLowerCase());
    if (className != null) {
      try {
        Class<? extends IndexDataMap> datamapClass =
            (Class<? extends IndexDataMap>) Class.forName(className);
        indexDataMap = datamapClass.newInstance();
      } catch (ClassNotFoundException ex) {
        throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found", ex);
      } catch (Throwable ex) {
        throw new MetadataProcessException("failed to create DataMap '" + providerName + "'", ex);
      }
    } else {
      throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found");
    }
    return indexDataMap;
  }

  @Override
  public DataMapCatalog createDataMapCatalog() {
    // TODO create abstract class and move the default implementation there.
    return null;
  }
}
