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

package org.apache.carbondata.index;

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.index.IndexRegistry;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

import org.apache.spark.sql.SparkSession;

/**
 * Interface to manipulate the index, All index should implement this interface.
 */
@InterfaceAudience.Internal
public class IndexProvider {

  private CarbonTable mainTable;
  private IndexSchema indexSchema;

  private SparkSession sparkSession;
  private IndexFactory<? extends Index> indexFactory;
  private List<CarbonColumn> indexedColumns;

  public IndexProvider(CarbonTable table, IndexSchema schema, SparkSession sparkSession)
      throws MalformedIndexCommandException {
    this.mainTable = table;
    this.indexSchema = schema;
    this.sparkSession = sparkSession;
    this.indexFactory = createIndexFactory();
    this.indexedColumns = table.getIndexedColumns(schema.getIndexColumns());
  }

  protected final CarbonTable getMainTable() {
    return mainTable;
  }

  public final IndexSchema getIndexSchema() {
    return indexSchema;
  }

  public void setMainTable(CarbonTable carbonTable) {
    this.mainTable = carbonTable;
  }

  public List<CarbonColumn> getIndexedColumns() {
    return indexedColumns;
  }

  public boolean rebuild() {
    IndexRebuildRDD.rebuildIndex(sparkSession, getMainTable(), getIndexSchema());
    return true;
  }

  private IndexFactory<? extends Index> createIndexFactory()
      throws MalformedIndexCommandException {
    CarbonTable mainTable = getMainTable();
    IndexSchema indexSchema = getIndexSchema();
    IndexFactory<? extends Index> indexFactory;
    try {
      // try to create IndexClassProvider instance by taking providerName as class name
      indexFactory = (IndexFactory<? extends Index>)
          Class.forName(indexSchema.getProviderName()).getConstructors()[0]
              .newInstance(mainTable, indexSchema);
    } catch (ClassNotFoundException e) {
      // try to create IndexClassProvider instance by taking providerName as short name
      indexFactory =
          IndexRegistry.getIndexFactoryByShortName(mainTable, indexSchema);
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to create IndexClassProvider '" + indexSchema.getProviderName() + "'", e);
    }
    return indexFactory;
  }

  public IndexFactory getIndexFactory() {
    return indexFactory;
  }

  public boolean supportRebuild() {
    return indexFactory.supportRebuild();
  }

}
