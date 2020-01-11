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
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapProvider;
import org.apache.carbondata.core.datamap.DataMapRegistry;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

import org.apache.spark.sql.SparkSession;

/**
 * Index type DataMap, all index datamap should implement this interface.
 */
@InterfaceAudience.Internal
public class IndexDataMapProvider extends DataMapProvider {

  private SparkSession sparkSession;
  private DataMapFactory<? extends DataMap> dataMapFactory;
  private List<CarbonColumn> indexedColumns;

  IndexDataMapProvider(CarbonTable table, DataMapSchema schema, SparkSession sparkSession)
      throws MalformedDataMapCommandException {
    super(table, schema);
    this.sparkSession = sparkSession;
    this.dataMapFactory = createDataMapFactory();
    dataMapFactory.validate();
    this.indexedColumns = table.getIndexedColumns(schema);
  }

  public List<CarbonColumn> getIndexedColumns() {
    return indexedColumns;
  }

  @Override
  public void initMeta(String ctasSqlStatement)
      throws MalformedDataMapCommandException, IOException {
    CarbonTable mainTable = getMainTable();
    DataMapSchema dataMapSchema = getDataMapSchema();
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
    DataMapStoreManager.getInstance().registerDataMap(mainTable, dataMapSchema, dataMapFactory);
    DataMapStoreManager.getInstance().saveDataMapSchema(dataMapSchema);
  }

  @Override
  public void cleanMeta() throws IOException {
    if (getMainTable() == null) {
      throw new UnsupportedOperationException("Table need to be specified in index datamaps");
    }
    DataMapStoreManager.getInstance().dropDataMapSchema(getDataMapSchema().getDataMapName());
  }

  @Override
  public void cleanData() {
    CarbonTable mainTable = getMainTable();
    if (mainTable == null) {
      throw new UnsupportedOperationException("Table need to be specified in index datamaps");
    }
    DataMapStoreManager.getInstance().deleteDataMap(
        mainTable, getDataMapSchema().getDataMapName());
  }

  @Override
  public boolean rebuild() {
    IndexDataMapRebuildRDD.rebuildDataMap(sparkSession, getMainTable(), getDataMapSchema());
    return true;
  }

  private DataMapFactory<? extends DataMap> createDataMapFactory()
      throws MalformedDataMapCommandException {
    CarbonTable mainTable = getMainTable();
    DataMapSchema dataMapSchema = getDataMapSchema();
    DataMapFactory<? extends DataMap> dataMapFactory;
    try {
      // try to create DataMapClassProvider instance by taking providerName as class name
      dataMapFactory = (DataMapFactory<? extends DataMap>)
          Class.forName(dataMapSchema.getProviderName()).getConstructors()[0]
              .newInstance(mainTable, dataMapSchema);
    } catch (ClassNotFoundException e) {
      // try to create DataMapClassProvider instance by taking providerName as short name
      dataMapFactory =
          DataMapRegistry.getDataMapFactoryByShortName(mainTable, dataMapSchema);
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to create DataMapClassProvider '" + dataMapSchema.getProviderName() + "'", e);
    }
    return dataMapFactory;
  }

  @Override
  public DataMapFactory getDataMapFactory() {
    return dataMapFactory;
  }

  @Override
  public boolean supportRebuild() {
    return dataMapFactory.supportRebuild();
  }

  @Override
  public boolean rebuildInternal(String newLoadName, Map<String, List<String>> segmentMap,
      CarbonTable carbonTable) {
    return false;
  }
}
