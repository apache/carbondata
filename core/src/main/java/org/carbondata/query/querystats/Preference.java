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

package org.carbondata.query.querystats;

public final class Preference {
  //distinct relationship will not be calculated for dimension with cardinality <= this
  public static final int IGNORE_CARDINALITY = 1;
  /**
   * parameter to decide whether to consider given combination as part of aggregate table
   * if benfit ratio is less than configured than ignore it else consider it
   */
  public static final int BENEFIT_RATIO = 10;
  //carbon configuration properties
  //sample load to consider for sampling
  public static final String AGG_LOAD_COUNT = "carbon.agg.loadCount";
  //no of partition to consider for sampling
  public static final String AGG_PARTITION_COUNT = "carbon.agg.partitionCount";
  // no of fact per load to consider for sampling
  public static final String AGG_FACT_COUNT = "carbon.agg.factCount";
  // no of record per fact to consider for sampling
  public static final String AGG_REC_COUNT = "carbon.agg.recordCount";
  //maximum aggregate combination suggestion
  public static final int AGG_COMBINATION_SIZE = 100;
  //query stats file saved on store
  public static final String QUERYSTATS_FILE_NAME = "queryStats";
  //directory where all files will be saved
  public static final String AGGREGATE_STORE_DIR = "aggsuggestion";
  //distinct relationship for data stats will be saved in this file
  public static final String DATASTATS_DISTINCT_FILE_NAME = "distinctData";
  //0->dimensions, 1->measures,2->cubename
  public static final String AGGREGATE_TABLE_SCRIPT = "CREATE AGGREGATETABLE {0}{1} from cube {2}";
  //aggregate combination for data stats will be saved in this file
  public static final String DATA_STATS_FILE_NAME = "dataStats";
  //performance goal in sec
  public static final int PERFORMANCE_GOAL = 3;
  public static final int QUERY_EXPIRY_DAYS = 30;
  /**
   * is it required to cache data stats suggestion
   */
  public static final String DATA_STATS_SUGG_CACHE = "carbon.agg.datastats.cache";
  public static final String PERFORMANCE_GOAL_KEY = "carbon.agg.query.performance.goal";
  public static final String QUERY_STATS_EXPIRY_DAYS_KEY = "carbon.agg.querystats.expiryday";
  public static final String BENEFIT_RATIO_KEY = "carbon.agg.benefit.ratio";

  private Preference() {

  }
}
