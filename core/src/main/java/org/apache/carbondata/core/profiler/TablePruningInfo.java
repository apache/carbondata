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
import org.apache.carbondata.core.index.dev.expr.IndexWrapperSimpleInfo;

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

  private IndexWrapperSimpleInfo cgIndex;
  private int numBlocksAfterCGPruning;
  private int numBlockletsAfterCGPruning;

  private IndexWrapperSimpleInfo fgIndex;
  private int numBlocksAfterFGPruning;
  private int numBlockletsAfterFGPruning;

  synchronized void addTotalBlocks(int numBlocks) {
    this.totalBlocks += numBlocks;
  }

  synchronized void addTotalBlocklets(int numBlocklets) {
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
   * we accumulate blocklet number in default index instead of setting it
   * in CarbonInputFormat
   */
  synchronized void addNumBlockletsAfterDefaultPruning(int numBlocklets) {
    this.numBlockletsAfterDefaultPruning += numBlocklets;
  }

  void setNumBlockletsAfterCGPruning(IndexWrapperSimpleInfo indexWrapperSimpleInfo,
      int numBlocklets, int numBlocks) {
    this.cgIndex = indexWrapperSimpleInfo;
    this.numBlocksAfterCGPruning = numBlocks;
    this.numBlockletsAfterCGPruning = numBlocklets;
  }

  void setNumBlockletsAfterFGPruning(IndexWrapperSimpleInfo indexWrapperSimpleInfo,
      int numBlocklets, int numBlocks) {
    this.fgIndex = indexWrapperSimpleInfo;
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
          .append(" - pruned by Main Index").append("\n")
          .append("    - skipped: ").append(skipBlocks).append(" blocks, ")
          .append(skipBlocklets).append(" blocklets").append("\n");
      if (cgIndex != null) {
        skipBlocks = numBlocksAfterDefaultPruning - numBlocksAfterCGPruning;
        skipBlocklets = numBlockletsAfterDefaultPruning - numBlockletsAfterCGPruning;
        builder
            .append(" - pruned by CG Index").append("\n")
            .append("    - name: ").append(cgIndex.getIndexWrapperName()).append("\n")
            .append("    - provider: ").append(cgIndex.getIndexWrapperProvider()).append("\n")
            .append("    - skipped: ").append(skipBlocks).append(" blocks, ")
            .append(skipBlocklets).append(" blocklets").append("\n");
      }
      if (fgIndex != null) {
        if (numBlockletsAfterCGPruning != 0) {
          skipBlocks = numBlocksAfterCGPruning - numBlocksAfterFGPruning;
          skipBlocklets = numBlockletsAfterCGPruning - numBlockletsAfterFGPruning;
        } else {
          skipBlocks = numBlocksAfterDefaultPruning - numBlocksAfterFGPruning;
          skipBlocklets = numBlockletsAfterDefaultPruning - numBlockletsAfterFGPruning;
        }
        builder
            .append(" - pruned by FG Index").append("\n")
            .append("    - name: ").append(fgIndex.getIndexWrapperName()).append("\n")
            .append("    - provider: ").append(fgIndex.getIndexWrapperProvider()).append("\n")
            .append("    - skipped: ").append(skipBlocks).append(" blocks, ")
            .append(skipBlocklets).append(" blocklets").append("\n");
      }
      return builder.toString();
    } else {
      return "";
    }
  }
}
