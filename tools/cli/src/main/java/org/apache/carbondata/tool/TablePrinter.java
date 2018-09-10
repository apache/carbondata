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
import java.util.LinkedList;
import java.util.List;

class TablePrinter {
  private List<String[]> table = new LinkedList<>();

  /**
   * create a new Table Printer
   * @param header table header
   */
  TablePrinter(String[] header) {
    this.table.add(header);
  }

  void addRow(String[] row) {
    table.add(row);
  }

  void printFormatted(PrintStream out) {
    // calculate the max length of each output field in the table
    int padding = 2;
    int[] maxLength = new int[table.get(0).length];
    for (int i = 0; i < table.get(0).length; i++) {
      for (String[] row : table) {
        maxLength[i] = Math.max(maxLength[i], row[i].length());
      }
    }

    for (String[] row : table) {
      for (int i = 0; i < row.length; i++) {
        out.print(row[i]);
        for (int num = 0; num < maxLength[i] + padding - row[i].length(); num++) {
          out.print(" ");
        }
      }
      out.println();
    }
  }
}
