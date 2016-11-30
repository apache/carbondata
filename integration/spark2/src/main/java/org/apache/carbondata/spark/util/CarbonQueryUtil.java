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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.model.CarbonQueryPlan;
import org.apache.carbondata.spark.partition.api.Partition;
import org.apache.carbondata.spark.partition.api.impl.DefaultLoadBalancer;
import org.apache.carbondata.spark.partition.api.impl.PartitionMultiFileImpl;
import org.apache.carbondata.spark.partition.api.impl.QueryPartitionHelper;
import org.apache.carbondata.spark.splits.TableSplit;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.command.Partitioner;
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
      String location = QueryPartitionHelper.getInstance().getLocation(partition, databaseName, tableName);
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
    FileType fileType = FileFactory.getFileType(sourcePath);
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
   * It creates the one split for each region server.
   */
  public static TableSplit[] getPartitionSplits(String sourcePath, String[] nodeList,
      int partitionCount) throws Exception {

    //Just create splits depends on locations of region servers
    FileType fileType = FileFactory.getFileType(sourcePath);
    DefaultLoadBalancer loadBalancer = null;
    List<Partition> allPartitions = getAllPartitions(sourcePath, fileType, partitionCount);
    loadBalancer = new DefaultLoadBalancer(Arrays.asList(nodeList), allPartitions);
    TableSplit[] splits = new TableSplit[allPartitions.size()];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new TableSplit();
      List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      Partition partition = allPartitions.get(i);
      String location = loadBalancer.getNodeForPartitions(partition);
      locations.add(location);
      splits[i].setPartition(partition);
      splits[i].setLocations(locations);
    }
    return splits;
  }

  public static void getAllFiles(String sourcePath, List<String> partitionsFiles, FileType fileType)
      throws Exception {

    if (!FileFactory.isFileExist(sourcePath, fileType, false)) {
      throw new Exception("Source file doesn't exist at path: " + sourcePath);
    }

    CarbonFile file = FileFactory.getCarbonFile(sourcePath, fileType);
    if (file.isDirectory()) {
      CarbonFile[] fileNames = file.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile pathname) {
          return true;
        }
      });
      for (int i = 0; i < fileNames.length; i++) {
        getAllFiles(fileNames[i].getPath(), partitionsFiles, fileType);
      }
    } else {
      // add only csv files
      if (file.getName().endsWith("csv")) {
        partitionsFiles.add(file.getPath());
      }
    }
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

  private static List<Partition> getAllPartitions(String sourcePath, FileType fileType,
      int partitionCount) throws Exception {
    List<String> files = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    splitFilePath(sourcePath, files, CarbonCommonConstants.COMMA);
    int[] numberOfFilesPerPartition = getNumberOfFilesPerPartition(files.size(), partitionCount);
    int startIndex = 0;
    int endIndex = 0;
    List<Partition> partitionList =
        new ArrayList<Partition>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    if (numberOfFilesPerPartition != null) {
      for (int i = 0; i < numberOfFilesPerPartition.length; i++) {
        List<String> partitionFiles =
            new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        endIndex += numberOfFilesPerPartition[i];
        for (int j = startIndex; j < endIndex; j++) {
          partitionFiles.add(files.get(j));
        }
        startIndex += numberOfFilesPerPartition[i];
        partitionList.add(new PartitionMultiFileImpl(i + "", partitionFiles));
      }
    }
    return partitionList;
  }

  private static int[] getNumberOfFilesPerPartition(int numberOfFiles, int partitionCount) {
    int div = numberOfFiles / partitionCount;
    int mod = numberOfFiles % partitionCount;
    int[] numberOfNodeToScan = null;
    if (div > 0) {
      numberOfNodeToScan = new int[partitionCount];
      Arrays.fill(numberOfNodeToScan, div);
    } else if (mod > 0) {
      numberOfNodeToScan = new int[mod];
    }
    for (int i = 0; i < mod; i++) {
      numberOfNodeToScan[i] = numberOfNodeToScan[i] + 1;
    }
    return numberOfNodeToScan;
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

  /**
   * This method will clear the dictionary cache for a given map of columns and dictionary cache
   * mapping
   *
   * @param columnToDictionaryMap
   */
  public static void clearColumnDictionaryCache(Map<String, Dictionary> columnToDictionaryMap) {
    for (Map.Entry<String, Dictionary> entry : columnToDictionaryMap.entrySet()) {
      CarbonUtil.clearDictionaryCache(entry.getValue());
    }
  }

}
