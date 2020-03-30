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

package org.apache.carbondata.core.index.dev;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * An entity which can store and retrieve index data.
 */
@InterfaceAudience.Internal
public interface Index<T extends Blocklet> {

  /**
   * It is called to load the index to memory or to initialize it.
   */
  void init(IndexModel indexModel) throws IOException;

  /**
   * Prune the table with resolved filter expression and partition information.
   * It returns the list of blocklets where these filters can exist.
   */
  List<T> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions, FilterExecuter filterExecuter, CarbonTable table)
      throws IOException;

  /**
   * Prune the table with filter expression and partition information. It returns the list of
   * blocklets where these filters can exist.
   */
  List<T> prune(Expression filter, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions, CarbonTable carbonTable, FilterExecuter filterExecuter);

  /**
   * Prune the data maps for finding the row count. It returns a Map of
   * blockletpath and the row count
   */
  long getRowCount(Segment segment, List<PartitionSpec> partitions);

  /**
   * Prune the data maps for finding the row count for each block. It returns a Map of
   * blockletpath and the row count
   */
  Map<String, Long> getRowCountForEachBlock(Segment segment, List<PartitionSpec> partitions,
      Map<String, Long> blockletToRowCountMap);

  // TODO Move this method to Abstract class
  /**
   * Validate whether the current segment needs to be fetching the required data
   */
  boolean isScanRequired(FilterResolverIntf filterExp);

  /**
   * Clear complete index table and release memory.
   */
  void clear();

  /**
   * clears all the resources
   */
  void finish();

  /**
   * Returns number of records information that are stored in Index.
   * Driver multi-thread block pruning happens based on the number of rows in Index.
   * So Indexs can have multiple rows if they store information of multiple files.
   * so, this number of entries is used to represent how many files information a Index contains
   */
  int getNumberOfEntries();
}
