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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.processing.exception.DataLoadingException;

import org.apache.spark.sql.SparkSession;

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
 *   <li> preaggregate: one type of MVDataMap that do pre-aggregate of single table </li>
 *   <li> timeseries: one type of MVDataMap that do pre-aggregate based on time dimension
 *     of the table </li>
 *   <li> class name of {@link org.apache.carbondata.core.datamap.dev.DataMapFactory}
 * implementation: Developer can implement new type of DataMap by extending
 * {@link org.apache.carbondata.core.datamap.dev.DataMapFactory} </li>
 * </ol>
 *
 * @since 1.4.0
 */
@InterfaceAudience.Internal
public interface DataMapProvider {

  /**
   * Initialize a datamap's metadata.
   * This is called when user creates datamap, for example "CREATE DATAMAP dm ON TABLE mainTable"
   * Implementation should initialize metadata for datamap, like creating table
   */
  void initMeta(CarbonTable mainTable, DataMapSchema dataMapSchema, String ctasSqlStatement,
      SparkSession sparkSession) throws MalformedDataMapCommandException, IOException;

  /**
   * Initialize a datamap's data.
   * This is called when user creates datamap, for example "CREATE DATAMAP dm ON TABLE mainTable"
   * Implementation should initialize data for datamap, like creating data folders
   */
  void initData(CarbonTable mainTable, SparkSession sparkSession);

  /**
   * Opposite operation of {@link #initMeta(CarbonTable, DataMapSchema, String, SparkSession)}.
   * This is called when user drops datamap, for example "DROP DATAMAP dm ON TABLE mainTable"
   * Implementation should clean all meta for the datamap
   */
  void freeMeta(CarbonTable mainTable, DataMapSchema dataMapSchema, SparkSession sparkSession);

  /**
   * Opposite operation of {@link #initData(CarbonTable, SparkSession)}.
   * This is called when user drops datamap, for example "DROP DATAMAP dm ON TABLE mainTable"
   * Implementation should clean all data for the datamap
   */
  void freeData(CarbonTable mainTable, DataMapSchema dataMapSchema, SparkSession sparkSession);

  /**
   * Rebuild the datamap by loading all existing data from mainTable
   * This is called when refreshing the datamap when
   * 1. after datamap creation and if `autoRefreshDataMap` is set to true
   * 2. user manually trigger refresh datamap command
   */
  void rebuild(CarbonTable mainTable, SparkSession sparkSession) throws DataLoadingException;

  /**
   * Build the datamap incrementally by loading specified segment data
   * This is called when user manually trigger refresh datamap
   */
  void incrementalBuild(CarbonTable mainTable, String[] segmentIds, SparkSession sparkSession)
    throws DataLoadingException;

}
