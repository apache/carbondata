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

package org.apache.carbondata.core.profiler;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.dev.expr.DataMapWrapperSimpleInfo;

/**
 * Used for EXPLAIN command
 * Notice that after 1.4.1, carbondata do blocklet pruning by segments, so that we need to record
 * and append it to the previous pruning result.
 */
@InterfaceAudience.Internal
public class TablePruningInfo {

  private int totalBlocklets;
  private String filterStatement;

  private DataMapWrapperSimpleInfo defaultDataMap;
  private int numBlockletsAfterDefaultPruning;

  private DataMapWrapperSimpleInfo cgDataMap;
  private int numBlockletsAfterCGPruning;

  private DataMapWrapperSimpleInfo fgDataMap;
  private int numBlockletsAfterFGPruning;

  void addTotalBlocklets(int numBlocklets) {
    this.totalBlocklets += numBlocklets;
  }

  void setFilterStatement(String filterStatement) {
    this.filterStatement = filterStatement;
  }

  public void setDefaultDataMap(DataMapWrapperSimpleInfo defaultDataMap) {
    this.defaultDataMap = defaultDataMap;
  }

  public void setCgDataMap(DataMapWrapperSimpleInfo cgDataMap) {
    this.cgDataMap = cgDataMap;
  }

  public void setFgDataMap(DataMapWrapperSimpleInfo fgDataMap) {
    this.fgDataMap = fgDataMap;
  }

  void addNumBlockletsAfterDefaultPruning(int numBlocklets) {
    this.numBlockletsAfterDefaultPruning += numBlocklets;
  }

  void addNumBlockletsAfterCGPruning(int numBlocklets) {
    this.numBlockletsAfterCGPruning += numBlocklets;
  }

  void addNumBlockletsAfterFGPruning(int numBlocklets) {
    this.numBlockletsAfterFGPruning += numBlocklets;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder
        .append(" - total blocklets: ").append(totalBlocklets).append("\n")
        .append(" - filter: ").append(filterStatement).append("\n");
    if (defaultDataMap != null) {
      int skipBlocklets = totalBlocklets - numBlockletsAfterDefaultPruning;
      builder
          .append(" - pruned by Main DataMap").append("\n")
          .append("    - skipped blocklets: ").append(skipBlocklets).append("\n");
    }
    if (cgDataMap != null) {
      int skipBlocklets = numBlockletsAfterDefaultPruning - numBlockletsAfterCGPruning;
      builder
          .append(" - pruned by CG DataMap").append("\n")
          .append("    - name: ").append(cgDataMap.getDataMapWrapperName()).append("\n")
          .append("    - provider: ").append(cgDataMap.getDataMapWrapperProvider()).append("\n")
          .append("    - skipped blocklets: ").append(skipBlocklets).append("\n");
    }
    if (fgDataMap != null) {
      int skipBlocklets;
      if (numBlockletsAfterCGPruning != 0) {
        skipBlocklets = numBlockletsAfterCGPruning - numBlockletsAfterFGPruning;
      } else {
        skipBlocklets = numBlockletsAfterDefaultPruning - numBlockletsAfterFGPruning;
      }
      builder
          .append(" - pruned by FG DataMap").append("\n")
          .append("    - name: ").append(fgDataMap.getDataMapWrapperName()).append("\n")
          .append("    - provider: ").append(fgDataMap.getDataMapWrapperProvider()).append("\n")
          .append("    - skipped blocklets: ").append(skipBlocklets).append("\n");
    }
    return builder.toString();
  }
}
