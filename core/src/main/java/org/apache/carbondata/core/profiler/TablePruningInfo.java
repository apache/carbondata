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
 */
@InterfaceAudience.Internal
public class TablePruningInfo {

  private int totalBlocks;
  private int totalBlocklets;
  private String filterStatement;
  private boolean showPruningInfo;

  private int numBlocksAfterDefaultPruning;
  private int numBlockletsAfterDefaultPruning = 0;

  private DataMapWrapperSimpleInfo cgDataMap;
  private int numBlocksAfterCGPruning;
  private int numBlockletsAfterCGPruning;

  private DataMapWrapperSimpleInfo fgDataMap;
  private int numBlocksAfterFGPruning;
  private int numBlockletsAfterFGPruning;

  void addTotalBlocks(int numBlocks) {
    this.totalBlocks += numBlocks;
  }

  void addTotalBlocklets(int numBlocklets) {
    this.totalBlocklets += numBlocklets;
  }

  void setFilterStatement(String filterStatement) {
    this.filterStatement = filterStatement;
  }

  void setShowPruningInfo(boolean showPruningInfo) {
    this.showPruningInfo = showPruningInfo;
  }

  void setNumBlocksAfterDefaultPruning(int numBlocks) {
    this.numBlocksAfterDefaultPruning = numBlocks;
  }

  /**
   * To get blocklet number no matter what cache level(block/blocklet) it is,
   * we accumulate blocklet number in default datamap instead of setting it
   * in CarbonInputFormat
   */
  void addNumBlockletsAfterDefaultPruning(int numBlocklets) {
    this.numBlockletsAfterDefaultPruning += numBlocklets;
  }

  void setNumBlockletsAfterCGPruning(DataMapWrapperSimpleInfo dataMapWrapperSimpleInfo,
      int numBlocklets, int numBlocks) {
    this.cgDataMap = dataMapWrapperSimpleInfo;
    this.numBlocksAfterCGPruning = numBlocks;
    this.numBlockletsAfterCGPruning = numBlocklets;
  }

  void setNumBlockletsAfterFGPruning(DataMapWrapperSimpleInfo dataMapWrapperSimpleInfo,
      int numBlocklets, int numBlocks) {
    this.fgDataMap = dataMapWrapperSimpleInfo;
    this.numBlocksAfterFGPruning = numBlocks;
    this.numBlockletsAfterFGPruning = numBlocklets;
  }

  @Override
  public String toString() {
    if (showPruningInfo) {
      StringBuilder builder = new StringBuilder();
      builder
          .append(" - total: ").append(totalBlocks).append(" blocks, ")
          .append(totalBlocklets).append(" blocklets").append("\n")
          .append(" - filter: ").append(filterStatement).append("\n");
      int skipBlocks = totalBlocks - numBlocksAfterDefaultPruning;
      int skipBlocklets = totalBlocklets - numBlockletsAfterDefaultPruning;
      builder
          .append(" - pruned by Main DataMap").append("\n")
          .append("    - skipped: ").append(skipBlocks).append(" blocks, ")
          .append(skipBlocklets).append(" blocklets").append("\n");
      if (cgDataMap != null) {
        skipBlocks = numBlocksAfterDefaultPruning - numBlocksAfterCGPruning;
        skipBlocklets = numBlockletsAfterDefaultPruning - numBlockletsAfterCGPruning;
        builder
            .append(" - pruned by CG DataMap").append("\n")
            .append("    - name: ").append(cgDataMap.getDataMapWrapperName()).append("\n")
            .append("    - provider: ").append(cgDataMap.getDataMapWrapperProvider()).append("\n")
            .append("    - skipped: ").append(skipBlocks).append(" blocks, ")
            .append(skipBlocklets).append(" blocklets").append("\n");;
      }
      if (fgDataMap != null) {
        if (numBlockletsAfterCGPruning != 0) {
          skipBlocks = numBlocksAfterCGPruning - numBlocksAfterFGPruning;
          skipBlocklets = numBlockletsAfterCGPruning - numBlockletsAfterFGPruning;
        } else {
          skipBlocks = numBlocksAfterDefaultPruning - numBlocksAfterFGPruning;
          skipBlocklets = numBlockletsAfterDefaultPruning - numBlockletsAfterFGPruning;
        }
        builder
            .append(" - pruned by FG DataMap").append("\n")
            .append("    - name: ").append(fgDataMap.getDataMapWrapperName()).append("\n")
            .append("    - provider: ").append(fgDataMap.getDataMapWrapperProvider()).append("\n")
            .append("    - skipped: ").append(skipBlocks).append(" blocks, ")
            .append(skipBlocklets).append(" blocklets").append("\n");;
      }
      return builder.toString();
    } else {
      return "";
    }
  }
}
