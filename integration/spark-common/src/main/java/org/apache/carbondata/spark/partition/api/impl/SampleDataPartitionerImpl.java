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

package org.apache.carbondata.spark.partition.api.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.scan.model.CarbonQueryPlan;
import org.apache.carbondata.spark.partition.api.DataPartitioner;
import org.apache.carbondata.spark.partition.api.Partition;

import org.apache.spark.sql.execution.command.Partitioner;

/**
 * Sample partition.
 */
public class SampleDataPartitionerImpl implements DataPartitioner {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SampleDataPartitionerImpl.class.getName());
  private int numberOfPartitions = 1;

  private int partionColumnIndex = -1;

  private String partitionColumn;

  private Partitioner partitioner;
  private List<Partition> allPartitions;
  private String baseLocation;

  public SampleDataPartitionerImpl() {
  }

  public void initialize(String basePath, String[] columns, Partitioner partitioner) {
    this.partitioner = partitioner;
    numberOfPartitions = partitioner.partitionCount();

    partitionColumn = partitioner.partitionColumn()[0];
    LOGGER.info("SampleDataPartitionerImpl initializing with following properties.");
    LOGGER.info("partitionCount: " + numberOfPartitions);
    LOGGER.info("partitionColumn: " + partitionColumn);
    LOGGER.info("basePath: " + basePath);
    LOGGER.info("columns: " + Arrays.toString(columns));

    this.baseLocation = basePath;
    allPartitions = new ArrayList<Partition>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (int i = 0; i < columns.length; i++) {
      if (columns[i].equalsIgnoreCase(partitionColumn)) {
        partionColumnIndex = i;
        break;
      }
    }

    for (int partionCounter = 0; partionCounter < numberOfPartitions; partionCounter++) {
      PartitionImpl partitionImpl =
          new PartitionImpl("" + partionCounter, baseLocation + '/' + partionCounter);

      List<Object> includedHashes = new ArrayList<Object>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      includedHashes.add(partionCounter);

      allPartitions.add(partitionImpl);
    }
  }

  @Override public Partition getPartionForTuple(Object[] tuple, long rowCounter) {
    int hashCode;
    if (partionColumnIndex == -1) {
      hashCode = hashCode(rowCounter);
    } else {
      try {
        hashCode = hashCode(((String) tuple[partionColumnIndex]).hashCode());
      } catch (NumberFormatException e) {
        hashCode = hashCode(0);
      }
    }
    return allPartitions.get(hashCode);
  }

  /**
   *
   */
  public List<Partition> getAllPartitions() {
    return allPartitions;
  }

  /**
   * @see DataPartitioner#getPartitions(CarbonQueryPlan)
   */
  public List<Partition> getPartitions(CarbonQueryPlan queryPlan) {
    // TODO: this has to be redone during partitioning implmentatation
    return allPartitions;
  }

  /**
   * Identify the partitions applicable for the given filter
   */
  public List<Partition> getPartitions() {
    return allPartitions;

    // TODO: this has to be redone during partitioning implementation
    //    for (Partition aPartition : allPartitions) {
    //      CarbonDimensionLevelFilter partitionFilterDetails =
    //          aPartition.getPartitionDetails().get(partitionColumn);
    //
    //      //Check if the partition is serving any of the
    //      //hash code generated for include filter of query
    //      for (Object includeFilter : msisdnFilter.getIncludeFilter()) {
    //        int hashCode = hashCode(((String) includeFilter).hashCode());
    //        if (partitionFilterDetails.getIncludeFilter().contains(hashCode)) {
    //          allowedPartitions.add(aPartition);
    //          break;
    //        }
    //      }
    //    }

  }

  private int hashCode(long key) {
    return (int) (Math.abs(key) % numberOfPartitions);
  }

  @Override public String[] getPartitionedColumns() {
    return new String[] { partitionColumn };
  }

  @Override public Partitioner getPartitioner() {
    return partitioner;
  }

}
