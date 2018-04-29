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
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapCatalog;
import org.apache.carbondata.core.datamap.DataMapProvider;
import org.apache.carbondata.core.datamap.DataMapRegistry;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.IndexDataMap;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;

import org.apache.spark.sql.SparkSession;

/**
 * Index type DataMap, all index datamap should implement this interface.
 */
@InterfaceAudience.Internal
public class IndexDataMapProvider extends DataMapProvider {

  private SparkSession sparkSession;
  private IndexDataMap<? extends DataMap> indexDataMap;
  private List<String> indexedColumns;

  IndexDataMapProvider(CarbonTable table, DataMapSchema schema, SparkSession sparkSession)
      throws MalformedDataMapCommandException {
    super(table, schema);
    this.sparkSession = sparkSession;
    this.indexDataMap = createIndexDataMapFactory();
    indexDataMap.validateIndexedColumns(schema, table);
    this.indexedColumns = indexDataMap.getIndexedColumns(schema);
  }

  public List<String> getIndexedColumns() {
    return indexedColumns;
  }

  @Override
  public void initMeta(String ctasSqlStatement)
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
    DataMapStoreManager.getInstance().registerDataMap(mainTable, dataMapSchema, indexDataMap);
    DataMapStoreManager.getInstance().saveDataMapSchema(dataMapSchema);
  }

  @Override
  public void initData() {
    // Nothing is needed to do by default
  }

  @Override
  public void freeMeta() throws IOException {
    if (mainTable == null) {
      throw new UnsupportedOperationException("Table need to be specified in index datamaps");
    }
    DataMapStoreManager.getInstance().dropDataMapSchema(dataMapSchema.getDataMapName());
  }

  @Override
  public void freeData() {
    if (mainTable == null) {
      throw new UnsupportedOperationException("Table need to be specified in index datamaps");
    }
    DataMapStoreManager.getInstance().clearDataMap(
        mainTable.getAbsoluteTableIdentifier(), dataMapSchema.getDataMapName());
  }

  @Override
  public void rebuild() {
    // IndexDataMapRefresher.rebuildDataMap(sparkSession, mainTable, dataMapSchema);
  }

  @Override
  public void incrementalBuild(String[] segmentIds) {
    throw new UnsupportedOperationException();
  }

  private IndexDataMap<? extends DataMap> createIndexDataMapFactory() throws MalformedDataMapCommandException {
    IndexDataMap<? extends DataMap> indexDataMap;
    try {
      // try to create DataMapClassProvider instance by taking providerName as class name
      Class<? extends IndexDataMap<? extends DataMap>> providerClass =
          (Class<? extends IndexDataMap<? extends DataMap>>)
              Class.forName(dataMapSchema.getProviderName());
      indexDataMap = providerClass.newInstance();
    } catch (ClassNotFoundException e) {
      // try to create DataMapClassProvider instance by taking providerName as short name
      indexDataMap = DataMapRegistry.getDataMapByShortName(dataMapSchema.getProviderName());
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to create DataMapClassProvider '" + dataMapSchema.getProviderName() + "'", e);
    }
    return indexDataMap;
  }

  @Override
  public DataMapCatalog createDataMapCatalog() {
    // TODO create abstract class and move the default implementation there.
    return null;
  }
}
