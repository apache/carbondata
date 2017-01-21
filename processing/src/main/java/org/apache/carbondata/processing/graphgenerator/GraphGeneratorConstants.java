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

package org.apache.carbondata.processing.graphgenerator;

public final class GraphGeneratorConstants {
  /**
   * TABLE_INPUT
   */
  public static final String TABLE_INPUT = "Table Input";
  /**
   * CARBON_SURROGATE_KEY_GENERATOR
   */
  public static final String CARBON_SURROGATE_KEY_GENERATOR = "Carbon Surrogate Key Generator";
  /**
   * MDKEY_GENERATOR
   */
  public static final String MDKEY_GENERATOR = "MDKey Generator";
  /**
   * SORT_KEY
   */
  public static final String SORT_KEY = "Sort keys";
  /**
   * SORT_KEY
   */
  public static final String SORT_KEY_AND_GROUPBY = "Sort keys And Group By Step";
  /**
   * CARBON_SLICE_MERGER
   */
  public static final String CARBON_SLICE_MERGER = "Carbon Slice Merger";
  /**
   * SELECT_REQUIRED_VALUE
   */
  public static final String SELECT_REQUIRED_VALUE = "Select Required Value";
  /**
   * CSV Input
   */
  public static final String CSV_INPUT = "CSV Input";
  /**
   * CARBON_MDKEY_GENERATOR_ID
   */
  public static final String MDKEY_GENERATOR_ID = "MDKeyGen";
  /**
   * CARBON_SLICE_MERGER_ID
   */
  public static final String CARBON_SLICE_MERGER_ID = "CarbonSliceMerger";
  /**
   * CARBON_SLICE_MERGER_ID
   */
  public static final String SORTKEY_ID = "SortKey";
  /**
   * CARBON_CSV_BASED_SURROAGATEGEN_ID
   */
  public static final String CARBON_CSV_BASED_SURROAGATEGEN_ID = "CarbonCSVBasedSurrogateGen";

  private GraphGeneratorConstants() {

  }

}
