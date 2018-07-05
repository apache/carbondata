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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.dev.expr.DataMapWrapperSimpleInfo;

/**
 * An information collector used for EXPLAIN command, to print out
 * SQL rewrite and pruning information.
 * This class is a singleton, not supporting concurrent EXPLAIN command
 */
@InterfaceAudience.Internal
public class ExplainCollector {

  private static ExplainCollector INSTANCE = null;

  private List<String> olapDataMapProviders = new ArrayList<>();
  private List<String> olapDataMapNames = new ArrayList<>();

  // mapping of thread name to map of table name to pruning info
  private Map<String, Map<String, TablePruningInfo>> scans = new ConcurrentHashMap<>();

  private ExplainCollector() {
  }

  public static boolean enabled() {
    return INSTANCE != null;
  }

  public static void setup() {
    INSTANCE = new ExplainCollector();
  }

  public static void remove() {
    if (enabled()) {
      INSTANCE = null;
    }
  }

  public static ExplainCollector get() {
    return INSTANCE;
  }

  public static void recordMatchedOlapDataMap(String dataMapProvider, String dataMapName) {
    if (enabled()) {
      Objects.requireNonNull(dataMapProvider);
      Objects.requireNonNull(dataMapName);
      ExplainCollector profiler = get();
      profiler.olapDataMapProviders.add(dataMapProvider);
      profiler.olapDataMapNames.add(dataMapName);
    }
  }

  public static void addPruningInfo(String tableName) {
    if (enabled()) {
      ExplainCollector profiler = get();
      String threadName = Thread.currentThread().getName();
      if (!profiler.scans.containsKey(threadName)) {
        Map<String, TablePruningInfo> map = new HashMap<>();
        map.put(tableName, new TablePruningInfo());
        profiler.scans.put(threadName, map);
      }
    }
  }

  public static void setFilterStatement(String filterStatement) {
    if (enabled()) {
      TablePruningInfo scan = getCurrentTablePruningInfo();
      scan.setFilterStatement(filterStatement);
    }
  }

  public static void recordDefaultDataMapPruning(DataMapWrapperSimpleInfo dataMapWrapperSimpleInfo,
      int numBlocklets) {
    if (enabled()) {
      TablePruningInfo scan = getCurrentTablePruningInfo();
      scan.setNumBlockletsAfterDefaultPruning(dataMapWrapperSimpleInfo, numBlocklets);
    }
  }

  public static void recordCGDataMapPruning(DataMapWrapperSimpleInfo dataMapWrapperSimpleInfo,
      int numBlocklets) {
    if (enabled()) {
      TablePruningInfo scan = getCurrentTablePruningInfo();
      scan.setNumBlockletsAfterCGPruning(dataMapWrapperSimpleInfo, numBlocklets);
    }
  }

  public static void recordFGDataMapPruning(DataMapWrapperSimpleInfo dataMapWrapperSimpleInfo,
      int numBlocklets) {
    if (enabled()) {
      TablePruningInfo scan = getCurrentTablePruningInfo();
      scan.setNumBlockletsAfterFGPruning(dataMapWrapperSimpleInfo, numBlocklets);
    }
  }

  public static void addTotalBlocklets(int numBlocklets) {
    if (enabled()) {
      TablePruningInfo scan = getCurrentTablePruningInfo();
      scan.addTotalBlocklets(numBlocklets);
    }
  }

  /**
   * Return the current TablePruningInfo (It is the last one in the map, since it is in
   * single thread)
   */
  private static TablePruningInfo getCurrentTablePruningInfo() {
    String threadName = Thread.currentThread().getName();
    if (!get().scans.containsKey(threadName)) {
      throw new IllegalStateException();
    }

    Iterator<TablePruningInfo> iterator = get().scans.get(threadName).values().iterator();
    TablePruningInfo output = null;
    while (iterator.hasNext()) {
      output = iterator.next();
    }
    return output;
  }

  public static String getFormatedOutput() {
    return get().toString();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < olapDataMapProviders.size(); i++) {
      if (i == 0) {
        builder.append("Query rewrite based on DataMap:").append("\n");
      }
      builder.append(" - ").append(olapDataMapNames.get(i)).append(" (")
          .append(olapDataMapProviders.get(i)).append(")").append("\n");
    }
    for (Map.Entry<String, Map<String, TablePruningInfo>> allThreads : scans.entrySet()) {
      for (Map.Entry<String, TablePruningInfo> entry : allThreads.getValue().entrySet()) {
        builder.append("Table Scan on ").append(entry.getKey()).append("\n")
            .append(entry.getValue().toString());
      }
    }
    return builder.toString();
  }

}
