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

package org.apache.carbondata.core.scan.filter;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

public class ColumnFilterInfo implements Serializable {

  private static final long serialVersionUID = 8181578747306832771L;

  private boolean isIncludeFilter;

  private List<Integer> filterList;

  /**
   * Implicit column filter values to be used for block and blocklet pruning
   */
  private List<String> implicitColumnFilterList;
  private transient Set<String> implicitDriverColumnFilterList;
  private List<Integer> excludeFilterList;
  /**
   * maintain the no dictionary filter values list.
   */
  private List<byte[]> noDictionaryFilterValuesList;

  public boolean isOptimized() {
    return isOptimized;
  }

  public void setOptimized(boolean optimized) {
    isOptimized = optimized;
  }

  private boolean isOptimized;

  private List<Object> measuresFilterValuesList;

  public List<byte[]> getNoDictionaryFilterValuesList() {
    return noDictionaryFilterValuesList;
  }

  public boolean isIncludeFilter() {
    return isIncludeFilter;
  }

  public void setIncludeFilter(boolean isIncludeFilter) {
    this.isIncludeFilter = isIncludeFilter;
  }

  public List<Integer> getFilterList() {
    return filterList;
  }

  public void setFilterList(List<Integer> filterList) {
    this.filterList = filterList;
  }

  public void setFilterListForNoDictionaryCols(List<byte[]> noDictionaryFilterValuesList) {
    this.noDictionaryFilterValuesList = noDictionaryFilterValuesList;
  }

  public List<Integer> getExcludeFilterList() {
    return excludeFilterList;
  }
  public void setExcludeFilterList(List<Integer> excludeFilterList) {
    this.excludeFilterList = excludeFilterList;
  }
  public List<String> getImplicitColumnFilterList() {
    return implicitColumnFilterList;
  }

  public void setImplicitColumnFilterList(List<String> implicitColumnFilterList) {
    this.implicitColumnFilterList = implicitColumnFilterList;
  }

  public List<Object> getMeasuresFilterValuesList() {
    return measuresFilterValuesList;
  }

  public void setMeasuresFilterValuesList(List<Object> measuresFilterValuesList) {
    this.measuresFilterValuesList = measuresFilterValuesList;
  }

  public Set<String> getImplicitDriverColumnFilterList() {
    // this list is required to be populated only n case of driver, so in executor this check will
    // avoid unnecessary loading of the driver filter list
    if (null == implicitDriverColumnFilterList) {
      populateBlockIdListForDriverBlockPruning();
    }
    return implicitDriverColumnFilterList;
  }

  private void populateBlockIdListForDriverBlockPruning() {
    implicitDriverColumnFilterList = new HashSet<>(implicitColumnFilterList.size());
    String blockId = null;
    for (String blockletId : implicitColumnFilterList) {
      blockId =
          blockletId.substring(0, blockletId.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR));
      implicitDriverColumnFilterList.add(blockId);
    }
  }
}
