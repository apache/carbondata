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

package org.apache.carbondata.core.datamap;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;

@InterfaceAudience.Internal
public class IndexDataMapProvider implements DataMapProvider {

  private DataMapSchemaStorageProvider storageProvider;

  public IndexDataMapProvider(DataMapSchemaStorageProvider storageProvider) {
    this.storageProvider = storageProvider;
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
    DataMapFactory dataMapFactory = createIndexDataMapFactory(dataMapSchema);
    DataMapStoreManager.getInstance().registerDataMap(mainTable, dataMapSchema, dataMapFactory);
    storageProvider.saveSchema(dataMapSchema);
  }

  @Override
  public void initData(CarbonTable mainTable) {
    // Nothing is needed to do by default
  }

  @Override
  public void freeMeta(CarbonTable mainTable, DataMapSchema dataMapSchema) throws IOException {
    storageProvider.dropSchema(dataMapSchema.getDataMapName(),
        dataMapSchema.getParentTables().get(0).getTableName());
  }

  @Override
  public void freeData(CarbonTable mainTable, DataMapSchema dataMapSchema) {
    DataMapStoreManager.getInstance().clearDataMap(
        mainTable.getAbsoluteTableIdentifier(), dataMapSchema.getDataMapName());
  }

  @Override
  public void rebuild(CarbonTable mainTable, DataMapSchema dataMapSchema) {
    // Nothing is needed to do by default
  }

  @Override public void incrementalBuild(CarbonTable mainTable, DataMapSchema dataMapSchema,
      String[] segmentIds) {
    throw new UnsupportedOperationException();
  }

  private DataMapFactory createIndexDataMapFactory(DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    DataMapFactory dataMapFactory;
    try {
      // try to create DataMapClassProvider instance by taking providerName as class name
      Class<? extends DataMapFactory> providerClass =
          (Class<? extends DataMapFactory>) Class.forName(dataMapSchema.getProviderName());
      dataMapFactory = providerClass.newInstance();
    } catch (ClassNotFoundException e) {
      // try to create DataMapClassProvider instance by taking providerName as short name
      dataMapFactory = getDataMapFactoryByShortName(dataMapSchema.getProviderName());
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to create DataMapClassProvider '" + dataMapSchema.getProviderName() + "'", e);
    }
    return dataMapFactory;
  }

  public static DataMapFactory getDataMapFactoryByShortName(String providerName)
      throws MalformedDataMapCommandException {
    if (providerName.equalsIgnoreCase(DataMapClassProvider.LUCENECG.getShortName())) {
      throw new MalformedDataMapCommandException(
          "failed to create datamap, Lucene CG datamap is not yet supported");
    }
    DataMapRegistry.registerDataMap(DataMapClassProvider.LUCENEFG.getClassName(),
        DataMapClassProvider.LUCENEFG.getShortName());
    DataMapFactory dataMapFactory;
    String className = DataMapRegistry.getDataMapClassName(providerName.toLowerCase());
    if (className != null) {
      try {
        Class<? extends DataMapFactory> datamapClass =
            (Class<? extends DataMapFactory>) Class.forName(className);
        dataMapFactory = datamapClass.newInstance();
      } catch (ClassNotFoundException ex) {
        throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found", ex);
      } catch (Throwable ex) {
        throw new MetadataProcessException("failed to create DataMap '" + providerName + "'", ex);
      }
    } else {
      throw new MalformedDataMapCommandException("DataMap '" + providerName + "' not found");
    }
    return dataMapFactory;
  }

  @Override public DataMapCatalog createDataMapCatalog() {
    // TODO create abstract class and move the default implementation there.
    return null;
  }
}
