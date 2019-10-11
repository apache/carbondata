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

package org.apache.carbondata.spark.util;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * deduplicate base on sort_columns and duplicate_column
 */
public class DeduplicateHelper {

  private Object[] oldRow;
  private Comparator<Object[]> sortComparator;
  private int duplicateColumn;
  private Map<Object, Boolean> map;

  public DeduplicateHelper(
      Comparator<Object[]> sortComparator, int duplicateColumn, int initCapacity) {
    this.sortComparator = sortComparator;
    this.duplicateColumn = duplicateColumn;
    map = new HashMap<>(initCapacity);
  }

  public boolean isDuplicate(Object[] row) {
    if (oldRow == null) {
      map.clear();
      map.put(row[duplicateColumn], true);
      oldRow = row;
      return false;
    } else {
      if (sortComparator.compare(oldRow, row) == 0) {
        if (map.get(row[duplicateColumn]) == null) {
          map.put(row[duplicateColumn], true);
          return false;
        } else {
          return true;
        }
      } else {
        map.clear();
        map.put(row[duplicateColumn], true);
        oldRow = row;
        return false;
      }
    }
  }

  public void close() {
    if (map != null) {
      map.clear();
      map = null;
    }
  }
}
