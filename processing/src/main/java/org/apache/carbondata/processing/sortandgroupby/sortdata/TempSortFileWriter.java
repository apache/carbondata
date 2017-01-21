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

package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.File;

import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

public interface TempSortFileWriter {
  /**
   * Method will be used to initialize
   *
   * @param file
   * @param entryCount
   * @throws CarbonSortKeyAndGroupByException
   */
  void initiaize(File file, int entryCount) throws CarbonSortKeyAndGroupByException;

  /**
   * Method will be used to finish
   */
  void finish();

  /**
   * Below method will be used to write the sort temp file
   *
   * @param records
   * @throws CarbonSortKeyAndGroupByException
   */
  void writeSortTempFile(Object[][] records) throws CarbonSortKeyAndGroupByException;
}
