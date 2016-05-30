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
package org.carbondata.query.carbon.executor.impl;

import java.util.List;
import java.util.Map;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;

/**
 * Holds all the properties required for query execution
 */
public class QueryExecutorProperties {

  /**
   * table unique name
   */
  protected String tableUniqueName;

  /**
   * list of blocks in which query will be executed
   */
  protected List<AbstractIndex> dataBlocks;

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
   * aggregator class selected for all aggregation function selected in query
   */
  public MeasureAggregator[] measureAggregators;

  /**
   * this will hold the information about the dictionary dimension
   * which to
   */
  public Map<String, Dictionary> columnToDictionayMapping;
}
