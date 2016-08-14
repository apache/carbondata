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

package org.apache.carbondata.core.util;

import java.util.Comparator;

import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;

public class CarbonFileFolderComparator implements Comparator<CarbonFile> {

  /**
   * Below method will be used to compare two file
   *
   * @param o1 first file
   * @param o2 Second file
   * @return compare result
   */
  @Override public int compare(CarbonFile o1, CarbonFile o2) {
    String firstFileName = o1.getName();
    String secondFileName = o2.getName();
    int lastIndexOfO1 = firstFileName.lastIndexOf('_');
    int lastIndexOfO2 = secondFileName.lastIndexOf('_');
    int file1 = 0;
    int file2 = 0;

    try {
      file1 = Integer.parseInt(firstFileName.substring(lastIndexOfO1 + 1));
      file2 = Integer.parseInt(secondFileName.substring(lastIndexOfO2 + 1));
    } catch (NumberFormatException e) {
      return -1;
    }
    return (file1 < file2) ? -1 : (file1 == file2 ? 0 : 1);
  }
}
