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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

class ShardPrinter {
  private Map<String, TablePrinter> shardPrinter = new HashMap<>();
  private String[] header;
  private ArrayList<String> outPuts;

  ShardPrinter(String[] header, ArrayList<String> outPuts) {
    this.header = header;
    this.outPuts = outPuts;
  }

  void addRow(String shardName, String[] row) {
    TablePrinter printer = shardPrinter.get(shardName);
    if (printer == null) {
      printer = new TablePrinter(header, outPuts);
      shardPrinter.put(shardName, printer);
    }
    printer.addRow(row);
  }

  void printFormatted(PrintStream out) {
    int shardId = 1;
    for (Map.Entry<String, TablePrinter> entry : shardPrinter.entrySet()) {
      outPuts.add(String.format("Shard #%d (%s)", shardId++, entry.getKey()));
      entry.getValue().printFormatted();
      outPuts.add("");
    }
  }
}
