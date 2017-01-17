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
package org.apache.carbondata.core.scan.executor.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;

/**
 * Holds all the properties required for query execution
 */
public class QueryExecutorProperties {

  /**
   * holds the information required for updating the order block
   * dictionary key
   */
  public KeyStructureInfo keyStructureInfo;

  /**
   * this will hold the information about the dictionary dimension
   * which to
   */
  public Map<String, Dictionary> columnToDictionayMapping;

  /**
   * Measure datatypes
   */
  public DataType[] measureDataTypes;
  /**
   * all the complex dimension which is on filter
   */
  public Set<CarbonDimension> complexFilterDimension;

  public Set<CarbonMeasure> filterMeasures;
  /**
   * to record the query execution details phase wise
   */
  public QueryStatisticsRecorder queryStatisticsRecorder;
  /**
   * executor service to execute the query
   */
  public ExecutorService executorService;
  /**
   * list of blocks in which query will be executed
   */
  protected List<AbstractIndex> dataBlocks;
}
