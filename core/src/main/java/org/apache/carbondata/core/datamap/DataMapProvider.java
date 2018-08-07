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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

/**
 * DataMap is a accelerator for certain type of query. Developer can add new DataMap
 * implementation to improve query performance.
 *
 * Currently two types of DataMap are supported
 * <ol>
 *   <li> MVDataMap: materialized view type of DataMap to accelerate olap style query,
 * like SPJG query (select, predicate, join, groupby) </li>
 *   <li> DataMap: index type of DataMap to accelerate filter query </li>
 * </ol>
 *
 * <p>
 * In following command <br>
 * {@code CREATE DATAMAP dm ON TABLE main USING 'provider'}, <br>
 * the <b>provider</b> string can be a short name or class name of the DataMap implementation.
 *
 * <br>Currently CarbonData supports following provider:
 * <ol>
 *   <li> preaggregate: pre-aggregate table of single table </li>
 *   <li> timeseries: pre-aggregate table based on time dimension of the table </li>
 *   <li> lucene: index backed by Apache Lucene </li>
 *   <li> bloomfilter: index backed by Bloom Filter </li>
 * </ol>
 *
 * @since 1.4.0
 */
@InterfaceAudience.Internal
public abstract class DataMapProvider {

  private CarbonTable mainTable;
  private DataMapSchema dataMapSchema;

  public DataMapProvider(CarbonTable mainTable, DataMapSchema dataMapSchema) {
    this.mainTable = mainTable;
    this.dataMapSchema = dataMapSchema;
  }

  protected final CarbonTable getMainTable() {
    return mainTable;
  }

  public final DataMapSchema getDataMapSchema() {
    return dataMapSchema;
  }

  /**
   * Initialize a datamap's metadata.
   * This is called when user creates datamap, for example "CREATE DATAMAP dm ON TABLE mainTable"
   * Implementation should initialize metadata for datamap, like creating table
   */
  public abstract void initMeta(String ctasSqlStatement) throws MalformedDataMapCommandException,
      IOException;

  /**
   * Initialize a datamap's data.
   * This is called when user creates datamap, for example "CREATE DATAMAP dm ON TABLE mainTable"
   * Implementation should initialize data for datamap, like creating data folders
   */
  public void initData() { }

  /**
   * Opposite operation of {@link #initMeta(String)}.
   * This is called when user drops datamap, for example "DROP DATAMAP dm ON TABLE mainTable"
   * Implementation should clean all meta for the datamap
   */
  public abstract void cleanMeta() throws IOException;

  /**
   * Opposite operation of {@link #initData()}.
   * This is called when user drops datamap, for example "DROP DATAMAP dm ON TABLE mainTable"
   * Implementation should clean all data for the datamap
   */
  public abstract void cleanData();

  /**
   * Rebuild the datamap by loading all existing data from mainTable
   * This is called when refreshing the datamap when
   * 1. after datamap creation and if `autoRefreshDataMap` is set to true
   * 2. user manually trigger REBUILD DATAMAP command
   */
  public abstract void rebuild() throws IOException, NoSuchDataMapException;

  /**
   * Build the datamap incrementally by loading specified segment data
   */
  public void incrementalBuild(String[] segmentIds) {
    throw new UnsupportedOperationException();
  }

  /**
   * Provide the datamap catalog instance or null if this datamap not required to rewrite
   * the query.
   */
  public DataMapCatalog createDataMapCatalog() {
    return null;
  }

  public abstract DataMapFactory getDataMapFactory();

  public abstract boolean supportRebuild();
}
