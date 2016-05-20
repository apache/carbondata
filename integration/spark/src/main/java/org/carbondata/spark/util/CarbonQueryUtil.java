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

package org.carbondata.spark.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryColumn;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.spark.partition.api.Partition;
import org.carbondata.spark.partition.api.impl.DefaultLoadBalancer;
import org.carbondata.spark.partition.api.impl.PartitionMultiFileImpl;
import org.carbondata.spark.partition.api.impl.QueryPartitionHelper;
import org.carbondata.spark.query.CarbonQueryPlan;
import org.carbondata.spark.splits.TableSplit;

import org.apache.spark.sql.SparkUnknownExpression;
import org.apache.spark.sql.execution.command.Partitioner;
/**
 * This utilty parses the Carbon query plan to actual query model object.
 */
public final class CarbonQueryUtil {

  private CarbonQueryUtil() {

  }

  public static QueryModel createQueryModel(AbsoluteTableIdentifier absoluteTableIdentifier,
      CarbonQueryPlan queryPlan, CarbonTable carbonTable) throws IOException {
    QueryModel executorModel = new QueryModel();
    //TODO : Need to find out right table as per the dims and msrs requested.

    String factTableName = carbonTable.getFactTableName();
    executorModel.setAbsoluteTableIdentifier(absoluteTableIdentifier);

    fillExecutorModel(queryPlan, carbonTable, executorModel, factTableName);

    // TODO need to handle to select aggregate table
    //    String suitableTableName = factTableName;
    //    if (!queryPlan.isDetailQuery() && queryPlan.getExpressions().isEmpty() && Boolean
    //        .parseBoolean(CarbonProperties.getInstance()
    //          .getProperty("spark.carbon.use.agg", "true"))) {
    //      suitableTableName = CarbonQueryParseUtil
    //          .getSuitableTable(carbonTable, dims, executorModel.getQueryMeasures());
    //    }
    //    if (!suitableTableName.equals(factTableName)) {
    //      fillExecutorModel(queryPlan, carbonTable, executorModel, suitableTableName);
    //      executorModel.setAggTable(true);
    //      fillDimensionAggregator(queryPlan, carbonTable, executorModel);
    //    } else {
    //
    //    }
    fillDimensionAggregator(queryPlan, executorModel);
    executorModel.setLimit(queryPlan.getLimit());
    executorModel.setDetailQuery(queryPlan.isDetailQuery());
    executorModel.setQueryId(queryPlan.getQueryId());
    executorModel.setQueryTempLocation(queryPlan.getOutLocationPath());
    return executorModel;
  }

  private static void fillExecutorModel(CarbonQueryPlan queryPlan, CarbonTable carbonTable,
      QueryModel queryModel, String factTableName) {
    AbsoluteTableIdentifier currentTemp = queryModel.getAbsoluteTableIdentifier();
    queryModel.setAbsoluteTableIdentifier(new AbsoluteTableIdentifier(currentTemp.getStorePath(),
        new CarbonTableIdentifier(queryPlan.getSchemaName(), factTableName)));
    queryModel.setQueryDimension(queryPlan.getDimensions());
    fillSortInfoInModel(queryModel, queryPlan.getSortedDimemsions());
    queryModel.setQueryMeasures(
        queryPlan.getMeasures());
    if (null != queryPlan.getFilterExpression()) {
      traverseAndSetDimensionOrMsrTypeForColumnExpressions(queryPlan.getFilterExpression(),
          carbonTable.getDimensionByTableName(factTableName),
          carbonTable.getMeasureByTableName(factTableName));
    }
    queryModel.setCountStarQuery(queryPlan.isCountStarQuery());
    //TODO need to remove this code, and executor will load the table
    // from file metadata
    queryModel.setTable(carbonTable);
  }

  private static void fillSortInfoInModel(QueryModel executorModel,
      List<QueryDimension> sortedDims) {
    if (null != sortedDims) {
      byte[] sortOrderByteArray = new byte[sortedDims.size()];
      int i = 0;
      for (QueryColumn mdim : sortedDims) {
        sortOrderByteArray[i++] = (byte) mdim.getSortOrder().ordinal();
      }
      executorModel.setSortOrder(sortOrderByteArray);
      executorModel.setSortDimension(sortedDims);
    } else {
      executorModel.setSortOrder(new byte[0]);
      executorModel.setSortDimension(new ArrayList<QueryDimension>(0));
    }

  }

  private static void fillDimensionAggregator(CarbonQueryPlan logicalPlan,
      QueryModel executorModel) {
    Map<String, DimensionAggregatorInfo> dimAggregatorInfos = logicalPlan.getDimAggregatorInfos();
    List<DimensionAggregatorInfo> dimensionAggregatorInfos =
        new ArrayList<DimensionAggregatorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos.entrySet()) {
      dimensionAggregatorInfos.add(entry.getValue());
    }
    executorModel.setDimAggregationInfo(dimensionAggregatorInfos);
  }

  private static void traverseAndSetDimensionOrMsrTypeForColumnExpressions(
      Expression filterExpression, List<CarbonDimension> dimensions, List<CarbonMeasure> measures) {
    if (null != filterExpression) {
      if (null != filterExpression.getChildren() && filterExpression.getChildren().size() == 0) {
        if (filterExpression instanceof ConditionalExpression) {
          List<ColumnExpression> listOfCol =
              ((ConditionalExpression) filterExpression).getColumnList();
          for (ColumnExpression expression : listOfCol) {
            setDimAndMsrColumnNode(dimensions, measures, (ColumnExpression) expression);
          }

        }
      }
      for (Expression expression : filterExpression.getChildren()) {

        if (expression instanceof ColumnExpression) {
          setDimAndMsrColumnNode(dimensions, measures, (ColumnExpression) expression);
        } else if (expression instanceof SparkUnknownExpression) {
          SparkUnknownExpression exp = ((SparkUnknownExpression) expression);
          List<ColumnExpression> listOfColExpression = exp.getAllColumnList();
          for (ColumnExpression col : listOfColExpression) {
            setDimAndMsrColumnNode(dimensions, measures, col);
          }
        } else {
          traverseAndSetDimensionOrMsrTypeForColumnExpressions(expression, dimensions, measures);
        }
      }
    }

  }

  private static void setDimAndMsrColumnNode(List<CarbonDimension> dimensions,
      List<CarbonMeasure> measures, ColumnExpression col) {
    CarbonDimension dim;
    CarbonMeasure msr;
    String columnName;
    columnName = col.getColumnName();
    dim = findDimension(dimensions, columnName);
    col.setCarbonColumn(dim);
    col.setDimension(dim);
    col.setDimension(true);
    if (null == dim) {
      msr = getCarbonMetadataMeasure(columnName, measures);
      col.setCarbonColumn(msr);
      col.setDimension(false);
    }
  }

  /**
   * Find the dimension from metadata by using unique name. As of now we are
   * taking level name as unique name. But user needs to give one unique name
   * for each level,that level he needs to mention in query.
   *
   * @param dimensions
   * @param carbonDim
   * @return
   */
  public static CarbonDimension findDimension(List<CarbonDimension> dimensions, String carbonDim) {
    CarbonDimension findDim = null;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.getColName().equalsIgnoreCase(carbonDim)) {
        findDim = dimension;
        break;
      }
    }
    return findDim;
  }

  public static CarbonMeasure getCarbonMetadataMeasure(String name, List<CarbonMeasure> measures) {
    for (CarbonMeasure measure : measures) {
      if (measure.getColName().equalsIgnoreCase(name)) {
        return measure;
      }
    }
    return null;
  }

  /**
   * It creates the one split for each region server.
   */
  public static synchronized TableSplit[] getTableSplits(String schemaName, String cubeName,
      CarbonQueryPlan queryPlan, Partitioner partitioner) throws IOException {

    //Just create splits depends on locations of region servers
    List<Partition> allPartitions = null;
    if (queryPlan == null) {
      allPartitions =
          QueryPartitionHelper.getInstance().getAllPartitions(schemaName, cubeName, partitioner);
    } else {
      allPartitions =
          QueryPartitionHelper.getInstance().getPartitionsForQuery(queryPlan, partitioner);
    }
    TableSplit[] splits = new TableSplit[allPartitions.size()];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new TableSplit();
      List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      Partition partition = allPartitions.get(i);
      String location = QueryPartitionHelper.getInstance()
          .getLocation(partition, schemaName, cubeName, partitioner);
      locations.add(location);
      splits[i].setPartition(partition);
      splits[i].setLocations(locations);
    }

    return splits;
  }

  /**
   * It creates the one split for each region server.
   */
  public static TableSplit[] getTableSplitsForDirectLoad(String sourcePath, String[] nodeList,
      int partitionCount) throws Exception {

    //Just create splits depends on locations of region servers
    FileType fileType = FileFactory.getFileType(sourcePath);
    DefaultLoadBalancer loadBalancer = null;
    List<Partition> allPartitions = getAllFilesForDataLoad(sourcePath, fileType, partitionCount);
    loadBalancer = new DefaultLoadBalancer(Arrays.asList(nodeList), allPartitions);
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

  private static List<Partition> getAllFilesForDataLoad(String sourcePath, FileType fileType,
      int partitionCount) throws Exception {
    List<String> files = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    getAllFiles(sourcePath, files, fileType);
    List<Partition> partitionList =
        new ArrayList<Partition>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    Map<Integer, List<String>> partitionFiles = new HashMap<Integer, List<String>>();

    for (int i = 0; i < partitionCount; i++) {
      partitionFiles.put(i, new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN));
      partitionList.add(new PartitionMultiFileImpl(i + "", partitionFiles.get(i)));
    }
    for (int i = 0; i < files.size(); i++) {
      partitionFiles.get(i % partitionCount).add(files.get(i));
    }
    return partitionList;
  }

  private static List<Partition> getAllPartitions(String sourcePath, FileType fileType,
      int partitionCount) throws Exception {
    List<String> files = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    getAllFiles(sourcePath, files, fileType);
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

}
