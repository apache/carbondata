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

/**
 *
 */
package org.carbondata.spark.query.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * It is filter class for Carbon dimension.
 */
public class CarbonDimensionFilter implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 5726964665470324380L;

  /**
   * Include filters.
   * Ex: select employee_name,department_name,sum(salary)
   * from employee where employee_name in ("abc","xyz");
   * then "abc"and "xyz" would be the include filters.
   */
  private List<String> includeFilters =
      new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * Exclude filters.
   * Ex: select employee_name,department_name,sum(salary)
   * from employee where employee_name not in ("abc","xyz");
   * then "abc"and "xyz" would be the exclude filters.
   */
  private List<String> excludeFilters =
      new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * Include like filters.
   * Ex: select employee_name,department_name,sum(salary)
   * from employee where employee_name like ("a","b");
   * then "a" and "b" will be the include like filters.
   */
  private List<String> includeLikeFilters =
      new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * Exclude like filters.
   * Ex: select employee_name,department_name,sum(salary)
   * from employee where employee_name not like ("a","b");
   * then "a" and "b" will be the exclude like filters.
   */
  private List<String> excludeLikeFilters =
      new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * Include range filters.These filters are usually used to query time range
   * when time is in millisecond format.
   * Like get the data between 1370428539 and 1370429000.
   */
  private List<long[]> includeRangeFilters =
      new ArrayList<long[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * Exclude range filters.These filters are usually used to query time range
   * when time is in millisecond format.
   * Like get the data between 1370428539 and 1370429000.
   */
  private List<long[]> excludeRangeFilters =
      new ArrayList<long[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * @return the includeFilter
   */
  public List<String> getIncludeFilters() {
    return includeFilters;
  }

  /**
   * @param includeFilter the includeFilter to set
   */
  public void addIncludeFilter(String includeFilter) {
    this.includeFilters.add(includeFilter);
  }

  /**
   * @return the excludeFilter
   */
  public List<String> getExcludeFilters() {
    return excludeFilters;
  }

  /**
   * @param excludeFilter the excludeFilter to set
   */
  public void addExcludeFilter(String excludeFilter) {
    this.excludeFilters.add(excludeFilter);
  }

  /**
   * @return the includeLikeFilter
   */
  public List<String> getIncludeLikeFilters() {
    return includeLikeFilters;
  }

  /**
   * @param includeLikeFilter the includeLikeFilter to set
   */
  public void addIncludeLikeFilter(String includeLikeFilter) {
    this.includeLikeFilters.add(includeLikeFilter);
  }

  /**
   * @return the excludeLikeFilter
   */
  public List<String> getExcludeLikeFilters() {
    return excludeLikeFilters;
  }

  /**
   * @param excludeLikeFilter the excludeLikeFilter to set
   */
  public void addExcludeLikeFilter(String excludeLikeFilter) {
    this.excludeLikeFilters.add(excludeLikeFilter);
  }

  /**
   * @return the includeRangeFilters
   */
  public List<long[]> getIncludeRangeFilters() {
    return includeRangeFilters;
  }

  public void addIncludeRangeFilter(long[] includeRangeFilter) {
    this.includeRangeFilters.add(includeRangeFilter);
  }

  /**
   * @return the excludeRangeFilters
   */
  public List<long[]> getExcludeRangeFilters() {
    return excludeRangeFilters;
  }

  public void addExcludeRangeFilter(long[] excludeRangeFilter) {
    this.excludeRangeFilters.add(excludeRangeFilter);
  }

}
