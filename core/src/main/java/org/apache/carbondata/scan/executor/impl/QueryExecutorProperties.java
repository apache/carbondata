/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.executor.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsRecorder;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.scan.filter.GenericQueryType;

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
   * as we have multiple type of column aggregation like
   * dimension,expression,measure so this will be used to for getting the
   * measure aggregation start index
   */
  public int measureStartIndex;
  /**
   * query like count(1),count(*) ,etc will used this parameter
   */
  public boolean isFunctionQuery;
  /**
   * aggExpressionStartIndex
   */
  public int aggExpressionStartIndex;
  /**
   * index of the dimension which is present in the order by
   * in a query
   */
  public byte[] sortDimIndexes;

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
   * complex parent index to query mapping
   */
  public Map<Integer, GenericQueryType> complexDimensionInfoMap;
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
