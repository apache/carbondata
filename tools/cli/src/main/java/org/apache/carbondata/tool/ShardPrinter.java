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

package org.apache.carbondata.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ShardPrinter {
  private Map<String, TableFormatter> shardPrinter = new HashMap<>();
  private String[] header;
  private List<String> outPuts;

  ShardPrinter(String[] header, List<String> outPuts) {
    this.header = header;
    this.outPuts = outPuts;
  }

  void addRow(String shardName, String[] row) {
    TableFormatter tableFormatter = shardPrinter.get(shardName);
    if (tableFormatter == null) {
      tableFormatter = new TableFormatter(header, outPuts);
      shardPrinter.put(shardName, tableFormatter);
    }
    tableFormatter.addRow(row);
  }

  void collectFormattedData() {
    int shardId = 1;
    for (Map.Entry<String, TableFormatter> entry : shardPrinter.entrySet()) {
      outPuts.add(String.format("Shard #%d (%s)", shardId++, entry.getKey()));
      entry.getValue().printFormatted();
      outPuts.add("");
    }
  }
}
