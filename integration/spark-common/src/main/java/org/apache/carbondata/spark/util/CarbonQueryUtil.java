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

package org.apache.carbondata.spark.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.scan.model.CarbonQueryPlan;
import org.apache.carbondata.spark.partition.api.Partition;
import org.apache.carbondata.spark.partition.api.impl.DefaultLoadBalancer;
import org.apache.carbondata.spark.partition.api.impl.PartitionMultiFileImpl;
import org.apache.carbondata.spark.partition.api.impl.QueryPartitionHelper;
import org.apache.carbondata.spark.splits.TableSplit;

import org.apache.commons.lang3.StringUtils;

/**
 * This utilty parses the Carbon query plan to actual query model object.
 */
public final class CarbonQueryUtil {

  private CarbonQueryUtil() {

  }

  /**
   * It creates the one split for each region server.
   */
  public static synchronized TableSplit[] getTableSplits(String databaseName, String tableName,
      CarbonQueryPlan queryPlan) throws IOException {

    //Just create splits depends on locations of region servers
    List<Partition> allPartitions = null;
    if (queryPlan == null) {
      allPartitions =
          QueryPartitionHelper.getInstance().getAllPartitions(databaseName, tableName);
    } else {
      allPartitions =
          QueryPartitionHelper.getInstance().getPartitionsForQuery(queryPlan);
    }
    TableSplit[] splits = new TableSplit[allPartitions.size()];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new TableSplit();
      List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      Partition partition = allPartitions.get(i);
      String location = QueryPartitionHelper.getInstance()
          .getLocation(partition, databaseName, tableName);
      locations.add(location);
      splits[i].setPartition(partition);
      splits[i].setLocations(locations);
    }

    return splits;
  }

  /**
   * It creates the one split for each region server.
   */
  public static TableSplit[] getTableSplitsForDirectLoad(String sourcePath) throws Exception {

    //Just create splits depends on locations of region servers
    DefaultLoadBalancer loadBalancer = null;
    List<Partition> allPartitions = getAllFilesForDataLoad(sourcePath);
    loadBalancer = new DefaultLoadBalancer(new ArrayList<String>(), allPartitions);
    TableSplit[] tblSplits = new TableSplit[allPartitions.size()];
    for (int i = 0; i < tblSplits.length; i++) {
      tblSplits[i] = new TableSplit();
      List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      Partition partition = allPartitions.get(i);
      String location = loadBalancer.getNodeForPartitions(partition);
      locations.add(location);
      tblSplits[i].setPartition(partition);
      tblSplits[i].setLocations(locations);
    }
    return tblSplits;
  }

  /**
   * split sourcePath by comma
   */
  public static void splitFilePath(String sourcePath, List<String> partitionsFiles,
      String separator) {
    if (StringUtils.isNotEmpty(sourcePath)) {
      String[] files = sourcePath.split(separator);
      for (String file : files) {
        partitionsFiles.add(file);
      }
    }
  }

  private static List<Partition> getAllFilesForDataLoad(String sourcePath) throws Exception {
    List<String> files = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    splitFilePath(sourcePath, files, CarbonCommonConstants.COMMA);
    List<Partition> partitionList =
        new ArrayList<Partition>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    Map<Integer, List<String>> partitionFiles = new HashMap<Integer, List<String>>();

    partitionFiles.put(0, new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN));
    partitionList.add(new PartitionMultiFileImpl(0 + "", partitionFiles.get(0)));

    for (int i = 0; i < files.size(); i++) {
      partitionFiles.get(i % 1).add(files.get(i));
    }
    return partitionList;
  }

  public static List<String> getListOfSlices(LoadMetadataDetails[] details) {
    List<String> slices = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    if (null != details) {
      for (LoadMetadataDetails oneLoad : details) {
        if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(oneLoad.getLoadStatus())) {
          String loadName = CarbonCommonConstants.LOAD_FOLDER + oneLoad.getLoadName();
          slices.add(loadName);
        }
      }
    }
    return slices;
  }

}
