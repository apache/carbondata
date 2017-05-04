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

package org.apache.carbondata.core.datastore.page.statistics;

import org.apache.carbondata.core.datastore.page.ColumnPage;

/**
 * Calculate the statistics for a column page and blocklet
 */
public interface StatisticsCollector {

  /**
   * name will be stored in Header
   */
  String getName();

  void startPage(int pageID);

  void endPage(int pageID);

  void startBlocklet(int blockletID);

  void endBlocklet(int blockletID);

  void startBlock(int blocklID);

  void endBlock(int blockID);

  /**
   * Update the stats for the input batch
   */
  void update(ColumnPage batch);

  /**
   * Ouput will be written to DataChunk2 (page header)
   */
  byte[] getPageStatistisc();

  /**
   * Output will be written to DataChunk3 (blocklet header)
   */
  byte[] getBlockletStatistics();

  /**
   * Output will be written to Footer
   */
  byte[] getBlockStatistics();
}



