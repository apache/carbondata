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

package org.carbondata.integration.spark.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperations;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.integration.spark.partition.api.Partition;
import org.carbondata.integration.spark.partition.api.impl.DefaultLoadBalancer;
import org.carbondata.integration.spark.partition.api.impl.PartitionMultiFileImpl;
import org.carbondata.integration.spark.partition.api.impl.QueryPartitionHelper;
import org.carbondata.integration.spark.query.CarbonQueryPlan;
import org.carbondata.integration.spark.query.metadata.CarbonLikeFilter;
import org.carbondata.integration.spark.query.metadata.CarbonPlanDimension;
import org.carbondata.integration.spark.query.metadata.CarbonPlanMeasure;
import org.carbondata.integration.spark.splits.TableSplit;
import org.carbondata.processing.util.CarbonSchemaParser;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.directinterface.impl.CarbonQueryParseUtil;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filters.likefilters.FilterLikeExpressionIntf;
import org.carbondata.query.filters.metadata.ContentMatchFilterInfo;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;
import org.carbondata.query.scope.QueryScopeObject;

import com.google.gson.Gson;
import org.apache.spark.sql.SparkUnknownExpression;
import org.apache.spark.sql.cubemodel.Partitioner;

/**
 * This utilty parses the Carbon query plan to actual query model object.
 */
public final class CarbonQueryUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonQueryUtil.class.getName());

  private CarbonQueryUtil() {

  }

  /**
   * API will provide the slices
   *
   * @param executerModel
   * @param basePath
   * @param partitionID
   * @return
   */
  public static List<String> getSliceLoads(CarbonQueryExecutorModel executerModel, String basePath,
      String partitionID) {

    List<String> listOfLoadPaths = new ArrayList<String>(20);
    if (null != executerModel) {
      List<String> listOfLoad = executerModel.getListValidSliceNumbers();

      if (null != listOfLoad) {
        for (String name : listOfLoad) {
          String loadPath = CarbonCommonConstants.LOAD_FOLDER + name;
          listOfLoadPaths.add(loadPath);
        }
      }
    }

    return listOfLoadPaths;

  }

  /**
   * This API will update the query executer model with valid sclice folder numbers based on its
   * load status present in the load metadata.
   */
  @Deprecated public static CarbonQueryExecutorModel updateCarbonExecuterModelWithLoadMetadata(
      CarbonQueryExecutorModel executerModel) {

    List<String> listOfValidSlices = new ArrayList<String>(10);
    String dataPath = executerModel.getCube().getMetaDataFilepath() + File.separator
        + CarbonCommonConstants.LOADMETADATA_FILENAME;
    DataInputStream dataInputStream = null;
    Gson gsonObjectToRead = new Gson();
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(dataPath, FileFactory.getFileType(dataPath));
    LoadMetadataDetails[] loadFolderDetailsArray;
    try {
      if (FileFactory.isFileExist(dataPath, FileFactory.getFileType(dataPath))) {

        dataInputStream = fileOperation.openForRead();

        BufferedReader buffReader =
            new BufferedReader(new InputStreamReader(dataInputStream, "UTF-8"));

        loadFolderDetailsArray = gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
        List<String> listOfValidUpdatedSlices = new ArrayList<String>(10);
        List<String> listOfAllLoadFolders = new ArrayList<String>(loadFolderDetailsArray.length);
        //just directly iterate Array
        List<LoadMetadataDetails> loadFolderDetails = Arrays.asList(loadFolderDetailsArray);

        for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails) {
          if (CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
              || CarbonCommonConstants.MARKED_FOR_UPDATE
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
              || CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
            // check for merged loads.
            if (null != loadMetadataDetails.getMergedLoadName()) {

              if (!listOfValidSlices.contains(loadMetadataDetails.getMergedLoadName())) {
                listOfValidSlices.add(loadMetadataDetails.getMergedLoadName());
              }
              // if merged load is updated then put it in updated list
              if (CarbonCommonConstants.MARKED_FOR_UPDATE
                  .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
                listOfValidUpdatedSlices.add(loadMetadataDetails.getMergedLoadName());
              }
              continue;
            }

            if (CarbonCommonConstants.MARKED_FOR_UPDATE
                .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {

              listOfValidUpdatedSlices.add(loadMetadataDetails.getLoadName());
            }
            listOfValidSlices.add(loadMetadataDetails.getLoadName());

          }
          if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
            listOfAllLoadFolders
                .add(CarbonCommonConstants.LOAD_FOLDER + loadMetadataDetails.getLoadName());
          }
        }
        executerModel.setListValidSliceNumbers(listOfValidSlices);
        executerModel.setListValidUpdatedSlice(listOfValidUpdatedSlices);
        executerModel.setListOfAllLoadFolder(listOfAllLoadFolders);
      } else {
        loadFolderDetailsArray = new LoadMetadataDetails[0];
      }
      executerModel.setLoadMetadataDetails(loadFolderDetailsArray);
    } catch (IOException e) {
      LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "IO Exception @: " + e.getMessage());
    } finally {
      try {

        if (null != dataInputStream) {
          dataInputStream.close();
        }
      } catch (Exception e) {
        LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "IO Exception @: " + e.getMessage());
      }

    }
    return executerModel;
  }

  public static QueryModel createQueryModel(AbsoluteTableIdentifier absoluteTableIdentifier,
      CarbonQueryPlan queryPlan, CarbonTable carbonTable) throws IOException {
    QueryModel executorModel = new QueryModel();
    //TODO : Need to find out right table as per the dims and msrs requested.

    String factTableName = carbonTable.getFactTableName();
    executorModel.setAbsoluteTableIdentifier(absoluteTableIdentifier);

    fillExecutorModel(queryPlan, carbonTable, executorModel, factTableName);
    List<CarbonDimension> dims =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    dims.addAll(executorModel.getQueryDimension());

    String suitableTableName = factTableName;
    if (!queryPlan.isDetailQuery() && queryPlan.getExpressions().isEmpty() && Boolean
        .parseBoolean(CarbonProperties.getInstance().getProperty("spark.carbon.use.agg", "true"))) {
      suitableTableName = CarbonQueryParseUtil
          .getSuitableTable(carbonTable, dims, executorModel.getQueryMeasures());
    }
    if (!suitableTableName.equals(factTableName)) {
      fillExecutorModel(queryPlan, carbonTable, executorModel, suitableTableName);
      executorModel.setAggTable(true);
      fillDimensionAggregator(queryPlan, carbonTable, executorModel);
    } else {
      fillDimensionAggregator(queryPlan, carbonTable, executorModel,
          carbonTable.getDimensionByTableName(factTableName));
    }
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

    // fill dimensions
    List<CarbonDimension> carbonDimensions = new ArrayList<CarbonDimension>();
    for (CarbonPlanDimension planDimension : queryPlan.getDimensions()) {
      carbonDimensions.add(
          carbonTable.getDimensionByName(factTableName, planDimension.getDimensionUniqueName()));
    }
    queryModel.setQueryDimension(carbonDimensions);

    fillSortInfoInModel(queryModel, queryPlan.getSortedDimemsions(), carbonTable);

    // fill measures
    List<CarbonMeasure> carbonMeasures = carbonTable.getMeasureByTableName(factTableName);
    queryModel.setQueryMeasures(
        getMeasures(queryPlan.getMeasures(), carbonTable, queryModel.isDetailQuery(), queryModel));

    // fill filter Column Expression
    if (null != queryPlan.getFilterExpression()) {
      traverseAndSetDimensionOrMsrTypeForColumnExpressions(queryPlan.getFilterExpression(),
          carbonDimensions, carbonMeasures);
    }

    queryModel.setCountStarQuery(queryPlan.isCountStartQuery());
  }

  private static void fillSortInfoInModel(QueryModel executorModel,
      List<CarbonPlanDimension> sortedDims, CarbonTable carbonTable) {
    String factTableName =
        executorModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName();
    if (null != sortedDims) {
      byte[] sortOrderByteArray = new byte[sortedDims.size()];
      int i = 0;
      for (CarbonPlanDimension mdim : sortedDims) {
        sortOrderByteArray[i++] = (byte) mdim.getSortOrderType().ordinal();
      }
      executorModel.setSortOrder(sortOrderByteArray);
      executorModel.setSortDimension(getDimensions(sortedDims, carbonTable, factTableName));
    } else {
      executorModel.setSortOrder(new byte[0]);
      executorModel.setSortDimension(new ArrayList<CarbonDimension>(0));
    }

  }

  private static void fillDimensionAggregator(CarbonQueryPlan queryPlan, CarbonTable carbonTable,
      QueryModel queryModel) {
    Map<String, DimensionAggregatorInfo> dimAggregatorInfos = queryPlan.getDimAggregatorInfos();
    String dimColumnName = null;
    List<CarbonMeasure> measure = queryModel.getQueryMeasures();
    List<DimensionAggregatorInfo> dimensionAggregatorInfos =
        new ArrayList<DimensionAggregatorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    DimensionAggregatorInfo value = null;
    CarbonMeasure aggMsr = null;
    String tableName =
        queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName();
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(tableName);
    for (Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos.entrySet()) {
      dimColumnName = entry.getKey();
      value = entry.getValue();
      List<Integer> orderList = value.getOrderList();
      List<String> aggList = value.getAggList();
      for (int i = 0; i < aggList.size(); i++) {
        aggMsr = getMeasure(measures, dimColumnName, aggList.get(i));
        if (aggMsr != null) {
          aggMsr.setQueryOrder(orderList.get(i));
          if (CarbonCommonConstants.DISTINCT_COUNT.equals(aggList.get(i))) {
            aggMsr.setDistinctQuery(true);
          }
          measure.add(aggMsr);
        }
      }
    }
    queryModel.setDimAggregationInfo(dimensionAggregatorInfos);
  }

  private static CarbonMeasure getMeasure(List<CarbonMeasure> measures, String msrName,
      String aggName) {
    for (CarbonMeasure measure : measures) {
      if (measure.getColName().equals(msrName) && measure.getAggregateFunction().equals(aggName)) {
        return measure;
      }
    }
    return null;
  }

  private static void fillDimensionAggregator(CarbonQueryPlan logicalPlan, CarbonTable carbonTable,
      QueryModel executorModel, List<CarbonDimension> dimensions) {
    Map<String, DimensionAggregatorInfo> dimAggregatorInfos = logicalPlan.getDimAggregatorInfos();
    String dimColumnName = null;
    List<DimensionAggregatorInfo> dimensionAggregatorInfos =
        new ArrayList<DimensionAggregatorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    DimensionAggregatorInfo value = null;
    CarbonDimension dimension = null;
    for (Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos.entrySet()) {
      dimColumnName = entry.getKey();
      value = entry.getValue();
      dimension = CarbonQueryParseUtil.findDimension(dimensions, dimColumnName);
      value.setDim(dimension);
      dimensionAggregatorInfos.add(value);
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
    dim = CarbonQueryParseUtil.findDimension(dimensions, columnName);
    col.setCarbonColumn(dim);
    col.setDimension(dim);
    col.setDimension(true);
    if (null == dim) {
      msr = getCarbonMetadataMeasure(columnName, measures);
      col.setCarbonColumn(msr);
      col.setDimension(false);
    }
  }

  private static Map<CarbonDimension, CarbonFilterInfo> getLikeConstaints(
      Map<CarbonPlanDimension, List<CarbonLikeFilter>> dimensionLikeFilters,
      List<CarbonDimension> dimensions) {
    Map<CarbonDimension, CarbonFilterInfo> cons = new HashMap<CarbonDimension, CarbonFilterInfo>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (Entry<CarbonPlanDimension, List<CarbonLikeFilter>> entry : dimensionLikeFilters
        .entrySet()) {
      CarbonDimension findDim =
          CarbonQueryParseUtil.findDimension(dimensions, entry.getKey().getDimensionUniqueName());
      CarbonFilterInfo createFilterInfo = createLikeFilterInfo(entry.getValue());
      cons.put(findDim, createFilterInfo);
    }
    return cons;
  }

  public static Cube loadSchema(String schemaPath, String schemaName, String cubeName) {
    Schema schema = CarbonSchemaParser.loadXML(schemaPath);

    CarbonMetadata.getInstance().loadSchema(schema);
    Cube cube = null;
    if (schemaName != null && cubeName != null) {
      cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    }
    return cube;
  }

  public static Cube loadSchema(Schema schema, String schemaName, String cubeName) {

    CarbonMetadata.getInstance().loadSchema(schema);
    Cube cube = null;
    if (schemaName != null && cubeName != null) {
      cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    }
    return cube;
  }

  public static int getDimensionIndex(Dimension dimension, Dimension[] dimensions) {
    int idx = -1;

    if (dimension != null) {
      for (int i = 0; i < dimensions.length; i++) {
        if (dimensions[i].equals(dimension)) {
          idx = i;
        }
      }
    }
    return idx;
  }

  /**
   * Get the best suited dimensions from metadata.
   */
  private static List<CarbonDimension> getDimensions(List<CarbonPlanDimension> carbonDims,
      CarbonTable carbonTable, String factTableName) {
    List<CarbonDimension> dims = new ArrayList<CarbonDimension>(carbonDims.size());

    for (CarbonPlanDimension carbonDim : carbonDims) {
      CarbonDimension findDim =
          carbonTable.getDimensionByName(factTableName, carbonDim.getDimensionUniqueName());
      if (findDim != null) {
        findDim.setDistinctQuery(carbonDim.isDistinctCountQuery());
        findDim.setQueryOrder(carbonDim.getQueryOrder());
        dims.add(findDim);
      }
    }
    return dims;
  }

  /**
   * Find the dimension from metadata by using unique name. As of now we are taking level name as
   * unique name.But user needs to give one unique name for each level,that level he needs to
   * mention in query.
   */
  public static CarbonPlanDimension getCarbonDimension(List<CarbonDimension> dimensions,
      String carbonDim) {
    for (CarbonDimension dimension : dimensions) {
      //Its just a temp work around to use level name as unique name. we need to provide a way
      // to configure unique name
      //to user in schema.
      if (dimension.getColName().equalsIgnoreCase(carbonDim)) {
        return new CarbonPlanDimension(carbonDim);
      }
    }
    return null;
  }

  /**
   * This method returns dimension ordinal for given dimensions name
   */
  public static int[] getDimensionOrdinal(List<Dimension> dimensions, String[] dimensionNames) {
    int[] dimOrdinals = new int[dimensionNames.length];
    int index = 0;
    for (String dimensionName : dimensionNames) {
      for (Dimension dimension : dimensions) {
        if (dimension.getName().equals(dimensionName)) {
          dimOrdinals[index++] = dimension.getOrdinal();
          break;
        }
      }
    }

    Arrays.sort(dimOrdinals);
    return dimOrdinals;
  }

  /**
   * Create the carbon measures from the requested query model.
   */
  private static List<CarbonMeasure> getMeasures(List<CarbonPlanMeasure> carbonPlanMeasures,
      CarbonTable carbonTable, boolean isDetailQuery, QueryModel executorModel) {

    List<CarbonMeasure> reqMsrs =
        new ArrayList<CarbonMeasure>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (CarbonPlanMeasure carbonPlanMsr : carbonPlanMeasures) {
      CarbonMeasure carbonMeasure = carbonTable.getMeasureByName(
          executorModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName(),
          carbonPlanMsr.getMeasure());
      if (null != carbonMeasure) {
        carbonMeasure.setDistinctQuery(carbonPlanMsr.isQueryDistinctCount());
        carbonMeasure.setQueryOrder(carbonPlanMsr.getQueryOrder());
      }
      reqMsrs.add(carbonMeasure);
      // TODO: while pushing down sort
      //            if (carbonMsr.getSortOrderType() != SortOrderType.NONE) {
      //                MeasureSortModel sortModel = new MeasureSortModel(carbonMeasure,
      //                        carbonMsr.getSortOrderType() == SortOrderType.DSC ? 1 : 0);
      //                executorModel.setSortModel(sortModel);
      //            }
    }
    return reqMsrs;
  }

  private static Measure findMeasure(List<Measure> measures, boolean isDetailQuery,
      CarbonPlanMeasure carbonMsr) {
    String aggName = null;
    String name = carbonMsr.getMeasure();
    if (!isDetailQuery) {
      //we assume the format is like sum(colName). need to handle in proper way.
      int indexOf = name.indexOf("(");
      if (indexOf > 0) {
        aggName = name.substring(0, indexOf).toLowerCase(Locale.getDefault());
        name = name.substring(indexOf + 1, name.length() - 1);
      }
    }
    if (name.equals("*")) {
      Measure measure = measures.get(0);
      measure = measure.getCopy();
      measure.setAggName(carbonMsr.getAggregatorType() != null ?
          carbonMsr.getAggregatorType().getValue().toLowerCase(Locale.getDefault()) :
          aggName);
      return measure;
    }
    for (Measure measure : measures) {
      if (measure.getName().equalsIgnoreCase(name)) {
        measure = measure.getCopy();
        measure.setAggName(carbonMsr.getAggregatorType() != null ?
            carbonMsr.getAggregatorType().getValue().toLowerCase() :
            measure.getAggName());
        return measure;
      }
    }
    return null;
  }

  public static CarbonPlanMeasure getCarbonMeasure(String name, List<CarbonMeasure> measures) {

    //dcd fix
    //String aggName = null;
    String msrName = name;
    //we assume the format is like sum(colName). need to handle in proper way.
    int indexOf = name.indexOf("(");
    if (indexOf > 0) {
      //dcd fix
      //aggName = name.substring(0, indexOf).toLowerCase();
      msrName = name.substring(indexOf + 1, name.length() - 1);
    }
    if (msrName.equals("*")) {
      return new CarbonPlanMeasure(name);
    }
    for (CarbonMeasure measure : measures) {
      if (measure.getColName().equalsIgnoreCase(msrName)) {
        return new CarbonPlanMeasure(name);
      }
    }

    return null;
  }

  public static CarbonMeasure getCarbonMetadataMeasure(String name, List<CarbonMeasure> measures) {
    for (CarbonMeasure measure : measures) {
      if (measure.getColName().equalsIgnoreCase(name)) {
        return measure;
      }
    }
    return null;
  }

  private static CarbonFilterInfo createLikeFilterInfo(List<CarbonLikeFilter> listOfFilter) {
    CarbonFilterInfo filterInfo = null;
    filterInfo = getCarbonFilterInfoBasedOnLikeExpressionTypeList(listOfFilter);
    return filterInfo;
  }

  private static CarbonFilterInfo getCarbonFilterInfoBasedOnLikeExpressionTypeList(
      List<CarbonLikeFilter> listOfFilter) {
    List<FilterLikeExpressionIntf> listOfFilterLikeExpressionIntf =
        new ArrayList<FilterLikeExpressionIntf>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    ContentMatchFilterInfo filterInfo = new ContentMatchFilterInfo();
    for (CarbonLikeFilter carbonLikeFilter : listOfFilter) {
      listOfFilterLikeExpressionIntf.add(carbonLikeFilter.getLikeFilterExpression());
    }
    filterInfo.setLikeFilterExpression(listOfFilterLikeExpressionIntf);
    return filterInfo;
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
      partitionsFiles.add(file.getPath());
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

  public static QueryScopeObject createDataSource(int currentRestructNumber, Schema schema,
      Cube cube, String partitionID, List<String> sliceLoadPaths, String factTableName,
      String basePath, long cubeCreationTime, LoadMetadataDetails[] loadMetadataDetails) {
    QueryScopeObject queryScopeObject = InMemoryTableStore.getInstance()
        .loadCube(schema, cube, partitionID, sliceLoadPaths, factTableName, basePath,
            currentRestructNumber, cubeCreationTime, loadMetadataDetails);
    return queryScopeObject;
  }

  public static boolean isQuickFilter(QueryModel queryModel) {
    return ("true".equals(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_ENABLE_QUICK_FILTER)) && null == queryModel
        .getFilterExpressionResolverTree() && queryModel.getQueryDimension().size() == 1
        && queryModel.getQueryMeasures().size() == 0
        && queryModel.getDimAggregationInfo().size() == 0 && queryModel.getExpressions().size() == 0
        && !queryModel.isDetailQuery());
  }

  /**
   * This method will traverse and populate the column list
   */
  private static void traverseAndPopulateColumnList(Expression filterExpression,
      List<Dimension> dimensions, List<Measure> measures, Set<String> columns) {
    if (null != filterExpression) {
      if (null != filterExpression.getChildren() && filterExpression.getChildren().size() == 0) {
        if (filterExpression instanceof ConditionalExpression) {
          List<ColumnExpression> listOfCol =
              ((ConditionalExpression) filterExpression).getColumnList();
          for (ColumnExpression expression : listOfCol) {
            columns.add(expression.getColumnName());
          }
        }
      }
      for (Expression expression : filterExpression.getChildren()) {
        if (expression instanceof ColumnExpression) {
          columns.add(((ColumnExpression) expression).getColumnName());
        } else if (expression instanceof SparkUnknownExpression) {
          SparkUnknownExpression exp = ((SparkUnknownExpression) expression);
          List<ColumnExpression> listOfColExpression = exp.getAllColumnList();
          for (ColumnExpression col : listOfColExpression) {
            columns.add(col.getColumnName());
          }
        } else {
          traverseAndPopulateColumnList(expression, dimensions, measures, columns);
        }
      }
    }
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

  public static void clearLevelLRUCacheForDroppedColumns(List<String> listOfLoadFolders,
      List<String> columns, String schemaName, String cubeName, int partitionCount) {
    CarbonQueryParseUtil
        .removeDroppedColumnsFromLevelLRUCache(listOfLoadFolders, columns, schemaName, cubeName,
            partitionCount);
  }
}
