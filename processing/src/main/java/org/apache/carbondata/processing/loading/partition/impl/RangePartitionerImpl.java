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

package org.apache.carbondata.processing.loading.partition.impl;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.loading.partition.Partitioner;

@InterfaceAudience.Internal
public class RangePartitionerImpl implements Partitioner<CarbonRow> {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RangePartitionerImpl.class.getName());
  private CarbonRow[] rangeBounds;
  private Comparator<CarbonRow> comparator;

  public RangePartitionerImpl(CarbonRow[] rangeBounds, Comparator<CarbonRow> comparator) {
    this.rangeBounds = rangeBounds;
    LOGGER.info("Use range partitioner to distribute data to "
        + (rangeBounds.length + 1) + " ranges.");
    this.comparator = comparator;
  }

  /**
   * learned from spark org.apache.spark.RangePartitioner
   *
   * @param key key
   * @return partitionId
   */
  @Override
  public int getPartition(CarbonRow key) {
    int partition = 0;
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length
          && comparator.compare(key, rangeBounds[partition]) > 0) {
        partition += 1;
      }
    } else {
      // binary search. binarySearch either returns the match location or -[insertion point]-1
      partition = Arrays.binarySearch(rangeBounds, 0, rangeBounds.length, key, comparator);
      if (partition < 0) {
        partition = -partition - 1;
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length;
      }
    }

    return partition;
  }
}
