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

/**
 *
 */
package org.carbondata.integration.spark.util;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import com.google.gson.Gson;
import org.apache.spark.sql.SparkUnknownExpression;
import org.apache.spark.sql.cubemodel.Partitioner;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.CubeDimension;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.CarbonDef.Table;
import org.carbondata.core.carbon.SqlStatement.Type;
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
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.integration.spark.partition.api.Partition;
import org.carbondata.integration.spark.partition.api.impl.DefaultLoadBalancer;
import org.carbondata.integration.spark.partition.api.impl.PartitionMultiFileImpl;
import org.carbondata.integration.spark.partition.api.impl.QueryPartitionHelper;
import org.carbondata.integration.spark.query.CarbonQueryPlan;
import org.carbondata.integration.spark.query.metadata.*;
import org.carbondata.integration.spark.splits.TableSplit;
import org.carbondata.processing.util.CarbonSchemaParser;
import org.carbondata.query.aggregator.CustomCarbonAggregateExpression;
import org.carbondata.query.aggregator.CustomMeasureAggregator;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.directinterface.impl.CarbonQueryParseUtil;
import org.carbondata.query.directinterface.impl.MeasureSortModel;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.executer.QueryExecutor;
import org.carbondata.query.executer.impl.QueryExecutorImpl;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filters.likefilters.FilterLikeExpressionIntf;
import org.carbondata.query.filters.metadata.ContentMatchFilterInfo;
import org.carbondata.query.holders.CarbonResultHolder;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;
import org.carbondata.query.scope.QueryScopeObject;

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
    public static CarbonQueryExecutorModel updateCarbonExecuterModelWithLoadMetadata(
            CarbonQueryExecutorModel executerModel) {

        List<String> listOfValidSlices = new ArrayList<String>(10);
        String dataPath = executerModel.getCube().getMetaDataFilepath() + File.separator
                + CarbonCommonConstants.LOADMETADATA_FILENAME
                + CarbonCommonConstants.CARBON_METADATA_EXTENSION;
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

                 loadFolderDetailsArray =
                        gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
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

                            if (!listOfValidSlices
                                    .contains(loadMetadataDetails.getMergedLoadName())) {
                                listOfValidSlices.add(loadMetadataDetails.getMergedLoadName());
                            }
                            // if merged load is updated then put it in updated list
                            if (CarbonCommonConstants.MARKED_FOR_UPDATE
                                    .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
                                listOfValidUpdatedSlices
                                        .add(loadMetadataDetails.getMergedLoadName());
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
                                .add(CarbonCommonConstants.LOAD_FOLDER + loadMetadataDetails
                                        .getLoadName());
                    }
                }
                executerModel.setListValidSliceNumbers(listOfValidSlices);
                executerModel.setListValidUpdatedSlice(listOfValidUpdatedSlices);
                executerModel.setListOfAllLoadFolder(listOfAllLoadFolders);
            }
            else {
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
                LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                        "IO Exception @: " + e.getMessage());
            }

        }
        return executerModel;
    }

    public static CarbonQueryExecutorModel createModel(CarbonQueryPlan logicalPlan, Schema schema,
            Cube cube, String storeLocation, int partitionCount) throws IOException {
        CarbonQueryExecutorModel executorModel = new CarbonQueryExecutorModel();
        executorModel.setSparkExecution(true);
        //TODO : Need to find out right table as per the dims and msrs requested.

        String factTableName = cube.getFactTableName();
        executorModel.setCube(cube);
        executorModel.sethIterator(
                new CarbonResultHolder(new ArrayList<Type>(CarbonCommonConstants.CONSTANT_SIZE_TEN)));

        fillExecutorModel(logicalPlan, cube, schema, executorModel, factTableName);
        List<Dimension> dims = new ArrayList<Dimension>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        //since a new executorModel instance has been created the same has to be updated with
        //high cardinality property.

        dims.addAll(Arrays.asList(executorModel.getDims()));

        String suitableTableName = factTableName;
        if (!logicalPlan.isDetailQuery() && logicalPlan.getExpressions().isEmpty() && Boolean
                .parseBoolean(
                        CarbonProperties.getInstance().getProperty("spark.carbon.use.agg", "true"))) {
            if (null == schema) {
                suitableTableName =
                        CarbonQueryParseUtil.getSuitableTable(cube, dims, executorModel.getMsrs());
            } else {
                suitableTableName = CarbonQueryParseUtil
                        .getSuitableTable(logicalPlan.getDimAggregatorInfos(), schema, cube, dims,
                                executorModel, storeLocation, partitionCount);
            }
        }
        if (!suitableTableName.equals(factTableName)) {
            fillExecutorModel(logicalPlan, cube, schema, executorModel, suitableTableName);
            executorModel.setAggTable(true);
            fillDimensionAggregator(logicalPlan, schema, cube, executorModel);
        } else {
            fillDimensionAggregator(logicalPlan, schema, cube, executorModel,
                    cube.getDimensions(factTableName));
        }
        executorModel.setActualDimsRows(executorModel.getDims());
        executorModel.setActualDimsCols(new Dimension[0]);
        executorModel
                .setCalcMeasures(new ArrayList<Measure>(CarbonCommonConstants.CONSTANT_SIZE_TEN));
        executorModel.setAnalyzerDims(executorModel.getDims());
        executorModel.setConstraintsAfterTopN(new HashMap<CarbonMetadata.Dimension, CarbonFilterInfo>(
                CarbonCommonConstants.DEFAULT_COLLECTION_SIZE));
        executorModel.setLimit(logicalPlan.getLimit());
        executorModel.setDetailQuery(logicalPlan.isDetailQuery());
        executorModel.setQueryId(logicalPlan.getQueryId());
        executorModel.setOutLocation(logicalPlan.getOutLocationPath());
        return executorModel;
    }

    private static void fillExecutorModel(CarbonQueryPlan logicalPlan, Cube cube, Schema schema,
            CarbonQueryExecutorModel executorModel, String factTableName) {
        executorModel.setFactTable(factTableName);
        List<Dimension> dimensions = cube.getDimensions(factTableName);
        executorModel.setDims(getDimensions(logicalPlan.getDimensions(), dimensions));
        updateDimensionWithNoDictionaryVal(schema, executorModel);
        fillSortInfoInModel(executorModel, logicalPlan.getSortedDimemsions(), dimensions);
        List<Measure> measures = cube.getMeasures(factTableName);
        executorModel.setMsrs(
                getMeasures(logicalPlan.getMeasures(), measures, executorModel.isDetailQuery(),
                        executorModel));
        if (null != logicalPlan.getFilterExpression()) {
            traverseAndSetDimensionOrMsrTypeForColumnExpressions(logicalPlan.getFilterExpression(),
                    dimensions, measures);
            executorModel.setFilterExpression(logicalPlan.getFilterExpression());
            executorModel.setConstraints(
                    getLikeConstaints(logicalPlan.getDimensionLikeFilters(), dimensions));
        }
        List<CarbonQueryExpression> expressions = logicalPlan.getExpressions();
        for (CarbonQueryExpression anExpression : expressions) {
            if (anExpression.getUsageType() == CarbonQueryExpression.UsageType.EXPRESSION) {
                CustomCarbonAggregateExpression carbonExpr = new CustomCarbonAggregateExpression();
                carbonExpr.setAggregator((CustomMeasureAggregator) anExpression.getAggregator());
                carbonExpr.setExpression(anExpression.getExpression());
                carbonExpr.setName(anExpression.getExpression());
                carbonExpr.setQueryOrder(anExpression.getQueryOrder());
                List<Dimension> columns =
                        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                for (CarbonColumn column : anExpression.getColumns()) {
                    if (column instanceof CarbonDimension) {
                        columns.add(CarbonQueryParseUtil.findDimension(dimensions,
                                ((CarbonDimension) column).getDimensionUniqueName()));
                    } else {
                        columns.add(findMeasure(measures, executorModel.isDetailQuery(),
                                ((CarbonMeasure) column)));
                    }
                }
                carbonExpr.setReferredColumns(columns);
                executorModel.addExpression(carbonExpr);
            }
        }
        executorModel.setCountStarQuery(logicalPlan.isCountStartQuery());
    }

    public static void setPartitionColumn(CarbonQueryExecutorModel executorModel,
            String[] partitionColumns) {
        List<String> partitionList = Arrays.asList(partitionColumns);
        executorModel.setPartitionColumns(partitionList);
    }

    private static void fillSortInfoInModel(CarbonQueryExecutorModel executorModel,
            List<CarbonDimension> sortedDims, List<Dimension> dimensions) {
        if (null != sortedDims) {
            byte[] sortOrderByteArray = new byte[sortedDims.size()];
            int i = 0;
            for (CarbonDimension mdim : sortedDims) {
                sortOrderByteArray[i++] = (byte) mdim.getSortOrderType().ordinal();
            }
            executorModel.setSortOrder(sortOrderByteArray);
            executorModel.setSortedDimensions(getDimensions(sortedDims, dimensions));
        } else {
            executorModel.setSortOrder(new byte[0]);
            executorModel.setSortedDimensions(new Dimension[0]);
        }

    }

    private static void fillDimensionAggregator(CarbonQueryPlan logicalPlan, Schema schema,
            Cube cube, CarbonQueryExecutorModel executorModel) {
        Map<String, DimensionAggregatorInfo> dimAggregatorInfos =
                logicalPlan.getDimAggregatorInfos();
        String dimColumnName = null;
        List<Measure> measure = executorModel.getMsrs();
        List<DimensionAggregatorInfo> dimensionAggregatorInfos =
                new ArrayList<DimensionAggregatorInfo>(
                        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        DimensionAggregatorInfo value = null;
        Measure aggMsr = null;
        List<Measure> measures = cube.getMeasures(executorModel.getFactTable());
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
        executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
    }

    private static Measure getMeasure(List<Measure> measures, String msrName, String aggName) {
        for (Measure measure : measures) {
            if (measure.getName().equals(msrName) && measure.getAggName().equals(aggName)) {
                return measure;
            }
        }
        return null;
    }

    private static void fillDimensionAggregator(CarbonQueryPlan logicalPlan, Schema schema,
            Cube cube, CarbonQueryExecutorModel executorModel, List<Dimension> dimensions) {
        Map<String, DimensionAggregatorInfo> dimAggregatorInfos =
                logicalPlan.getDimAggregatorInfos();
        String dimColumnName = null;
        List<DimensionAggregatorInfo> dimensionAggregatorInfos =
                new ArrayList<DimensionAggregatorInfo>(
                        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        DimensionAggregatorInfo value = null;
        Dimension dimension = null;
        for (Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos.entrySet()) {
            dimColumnName = entry.getKey();
            value = entry.getValue();
            dimension = CarbonQueryParseUtil.findDimension(dimensions, dimColumnName);
            value.setDim(dimension);
            dimensionAggregatorInfos.add(value);
        }
        executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
    }

    private static void traverseAndSetDimensionOrMsrTypeForColumnExpressions(
            Expression filterExpression, List<Dimension> dimensions, List<Measure> measures) {
        if (null != filterExpression) {
            if (null != filterExpression.getChildren()
                    && filterExpression.getChildren().size() == 0) {
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
                    traverseAndSetDimensionOrMsrTypeForColumnExpressions(expression, dimensions,
                            measures);
                }
            }
        }

    }

    private static void setDimAndMsrColumnNode(List<Dimension> dimensions, List<Measure> measures,
            ColumnExpression col) {
        Dimension dim;
        Measure msr;
        String columnName;
        columnName = col.getColumnName();
        dim = CarbonQueryParseUtil.findDimension(dimensions, columnName);
        col.setDim(dim);
        col.setDimension(true);
        if (null == dim) {
            msr = getCarbonMetadataMeasure(columnName, measures);
            col.setDim(msr);
            col.setDimension(false);
        }
    }

    private static Map<Dimension, CarbonFilterInfo> getLikeConstaints(
            Map<CarbonDimension, List<CarbonLikeFilter>> dimensionLikeFilters,
            List<Dimension> dimensions) {
        Map<Dimension, CarbonFilterInfo> cons =
                new HashMap<CarbonMetadata.Dimension, CarbonFilterInfo>(
                        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (Entry<CarbonDimension, List<CarbonLikeFilter>> entry : dimensionLikeFilters.entrySet()) {
            Dimension findDim = CarbonQueryParseUtil
                    .findDimension(dimensions, entry.getKey().getDimensionUniqueName());
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
    private static Dimension[] getDimensions(List<CarbonDimension> carbonDims,
            List<Dimension> dimensions) {
        Dimension[] dims = new Dimension[carbonDims.size()];

        int i = 0;
        for (CarbonDimension carbonDim : carbonDims) {
            Dimension findDim = CarbonQueryParseUtil
                    .findDimension(dimensions, carbonDim.getDimensionUniqueName());
            if (findDim != null) {
                findDim.setQueryForDistinctCount(carbonDim.isDistinctCountQuery());
                findDim.setQueryOrder(carbonDim.getQueryOrder());
                dims[i++] = findDim;
            }
        }
        return dims;

    }

    /**
     * Find the dimension from metadata by using unique name. As of now we are taking level name as
     * unique name.But user needs to give one unique name for each level,that level he needs to
     * mention in query.
     */
    public static CarbonDimension getCarbonDimension(List<Dimension> dimensions, String carbonDim) {
        for (Dimension dimension : dimensions) {
            //Its just a temp work around to use level name as unique name. we need to provide a way
            // to configure unique name
            //to user in schema.
            if (dimension.getName().equalsIgnoreCase(carbonDim)) {
                return new CarbonDimension(carbonDim);
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
    private static List<Measure> getMeasures(List<CarbonMeasure> carbonMsrs, List<Measure> measures,
            boolean isDetailQuery, CarbonQueryExecutorModel executorModel) {
        List<Measure> reqMsrs = new ArrayList<Measure>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

        for (CarbonMeasure carbonMsr : carbonMsrs) {
            Measure findMeasure = findMeasure(measures, isDetailQuery, carbonMsr);
            if (null != findMeasure) {
                findMeasure.setDistinctQuery(carbonMsr.isQueryDistinctCount());
                findMeasure.setQueryOrder(carbonMsr.getQueryOrder());
            }
            reqMsrs.add(findMeasure);
            if (carbonMsr.getSortOrderType() != SortOrderType.NONE) {
                MeasureSortModel sortModel = new MeasureSortModel(findMeasure,
                        carbonMsr.getSortOrderType() == SortOrderType.DSC ? 1 : 0);
                executorModel.setSortModel(sortModel);
            }

        }

        return reqMsrs;
    }

    private static Measure findMeasure(List<Measure> measures, boolean isDetailQuery,
            CarbonMeasure carbonMsr) {
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

    public static CarbonMeasure getCarbonMeasure(String name, List<Measure> measures) {

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
            return new CarbonMeasure(name);
        }
        for (Measure measure : measures) {
            if (measure.getName().equalsIgnoreCase(msrName)) {
                return new CarbonMeasure(name);
            }
        }

        return null;
    }

    public static Measure getCarbonMetadataMeasure(String name, List<Measure> measures) {
        for (Measure measure : measures) {
            if (measure.getName().equalsIgnoreCase(name)) {
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
            allPartitions = QueryPartitionHelper.getInstance()
                    .getAllPartitions(schemaName, cubeName, partitioner);
        } else {
            allPartitions = QueryPartitionHelper.getInstance()
                    .getPartitionsForQuery(queryPlan, partitioner);
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
        List<Partition> allPartitions =
                getAllFilesForDataLoad(sourcePath, fileType, partitionCount);
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

    public static void getAllFiles(String sourcePath, List<String> partitionsFiles,
            FileType fileType) throws Exception {

        if (!FileFactory.isFileExist(sourcePath, fileType, false)) {
            throw new Exception("Source file doesn't exist at path: " + sourcePath);
        }

        CarbonFile file = FileFactory.getCarbonFile(sourcePath, fileType);
        if (file.isDirectory()) {
            CarbonFile[] fileNames = file.listFiles(new CarbonFileFilter() {
                @Override
                public boolean accept(CarbonFile pathname) {
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
        int[] numberOfFilesPerPartition =
                getNumberOfFilesPerPartition(files.size(), partitionCount);
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

    public static void createDataSource(int currentRestructNumber, Schema schema, Cube cube,
            String partitionID, List<String> sliceLoadPaths, String factTableName,
            long cubeCreationTime, LoadMetadataDetails[] loadMetadataDetails) {
        String basePath = CarbonUtil.getCarbonStorePath(schema.name, schema.cubes[0].name);
        InMemoryTableStore.getInstance().
                loadCubeMetadataIfRequired(schema, schema.cubes[0], partitionID, cubeCreationTime);
        InMemoryTableStore.getInstance()
                .loadCube(schema, cube, partitionID, sliceLoadPaths, factTableName, basePath,
                        currentRestructNumber, cubeCreationTime, loadMetadataDetails);
    }

    public static QueryScopeObject createDataSource(int currentRestructNumber, Schema schema,
            Cube cube, String partitionID, List<String> sliceLoadPaths, String factTableName,
            String basePath, long cubeCreationTime, LoadMetadataDetails[] loadMetadataDetails) {
        QueryScopeObject queryScopeObject = InMemoryTableStore.getInstance()
                .loadCube(schema, cube, partitionID, sliceLoadPaths, factTableName, basePath,
                        currentRestructNumber, cubeCreationTime, loadMetadataDetails);
        return queryScopeObject;
    }

    public static Schema updateSchemaWithPartition(Schema schema, String partitionID) {

        String originalSchemaName = schema.name;
        String originalCubeName = schema.cubes[0].name;
        schema.name = originalSchemaName + '_' + partitionID;
        schema.cubes[0].name = originalCubeName + '_' + partitionID;
        return schema;
    }

    public static Schema updateSchemaWithPartition(String path, String partitionID) {
        Schema schema = CarbonSchemaParser.loadXML(path);

        String originalSchemaName = schema.name;
        String originalCubeName = schema.cubes[0].name;
        schema.name = originalSchemaName + '_' + partitionID;
        schema.cubes[0].name = originalCubeName + '_' + partitionID;
        return schema;
    }

    public static boolean isQuickFilter(CarbonQueryExecutorModel carbonQueryModel) {
        return ("true".equals(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.CARBON_ENABLE_QUICK_FILTER))
                && null == carbonQueryModel.getFilterExpression()
                && carbonQueryModel.getDims().length == 1 && carbonQueryModel.getMsrs().size() == 0
                && carbonQueryModel.getDimensionAggInfo().size() == 0
                && carbonQueryModel.getExpressions().size() == 0 && !carbonQueryModel
                .isDetailQuery());
    }

    public static String[] getAllColumns(Schema schema) {
        String cubeUniqueName = schema.name + '_' + schema.cubes[0].name;
        CarbonMetadata.getInstance().removeCube(cubeUniqueName);
        CarbonMetadata.getInstance().loadSchema(schema);
        Cube cube = CarbonMetadata.getInstance().getCube(cubeUniqueName);
        Set<String> metaTableColumns =
                cube.getMetaTableColumns(((Table) schema.cubes[0].fact).name);
        return metaTableColumns.toArray(new String[metaTableColumns.size()]);
    }

    public static QueryExecutor getQueryExecuter(Cube cube, String factTable, QueryScopeObject queryScopeObject) {
        QueryExecutor executer =
                new QueryExecutorImpl(cube.getDimensions(factTable), cube.getSchemaName(),
                        cube.getOnlyCubeName(), queryScopeObject);
        return executer;
    }

    public static void updateDimensionWithNoDictionaryVal(CarbonDef.Schema schema,
            CarbonQueryExecutorModel queryModel) {

        CubeDimension[] cubeDimensions = schema.cubes[0].dimensions;
        Dimension[] metadataDimensions = queryModel.getDims();

        for (Dimension metadataDimension : metadataDimensions) {
            for (CubeDimension cubeDimension : cubeDimensions) {
                if (metadataDimension.getName().equals(cubeDimension.name)
                        && cubeDimension.noDictionary) {
                    metadataDimension.setNoDictionaryDims(cubeDimension.noDictionary);
                    break;
                }

            }
        }

    }

    public static List<String> validateAndLoadRequiredSlicesInMemory(
            CarbonQueryExecutorModel queryModel, List<String> listLoadFolders, String cubeUniqueName)
            throws RuntimeException {
        Set<String> columns = getColumnList(queryModel, cubeUniqueName);
        return loadRequiredLevels(listLoadFolders, cubeUniqueName, columns);
    }

    public static List<String> loadRequiredLevels(List<String> listLoadFolders,
            String cubeUniqueName, Set<String> columns) throws RuntimeException {
        List<String> levelCacheKeys = InMemoryTableStore.getInstance()
                .loadRequiredLevels(cubeUniqueName, columns, listLoadFolders);
        return levelCacheKeys;
    }

    /**
     * This method will return the name of the all the columns reference in the
     * query
     */
    public static Set<String> getColumnList(CarbonQueryExecutorModel queryModel,
            String cubeUniqueName) {
        Set<String> queryColumns =
                new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        Expression filterExpression = queryModel.getFilterExpression();
        Cube cube = queryModel.getCube();
        List<Dimension> dimensions = cube.getDimensions(cube.getFactTableName());
        LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "@@@@Dimension size :: " + dimensions.size());
        if (dimensions.isEmpty()) {
            cube = CarbonMetadata.getInstance().getCube(cubeUniqueName);
            dimensions = cube.getDimensions(cube.getFactTableName());
        }
        List<Measure> measures = cube.getMeasures(cube.getFactTableName());
        traverseAndPopulateColumnList(filterExpression, dimensions, measures, queryColumns);
        Dimension[] dims = queryModel.getDims();
        for (int i = 0; i < dims.length; i++) {
            queryColumns.add(dims[i].getColName());
        }
        dims = queryModel.getSortedDimensions();
        for (int i = 0; i < dims.length; i++) {
            queryColumns.add(dims[i].getColName());
        }
        List<DimensionAggregatorInfo> dimensionAggInfo = queryModel.getDimensionAggInfo();
        for (DimensionAggregatorInfo dimAggInfo : dimensionAggInfo) {
            Dimension dim =
                    CarbonQueryParseUtil.findDimension(dimensions, dimAggInfo.getColumnName());
            if (null != dim) {
                queryColumns.add(dim.getColName());
            }
        }
        addCustomExpressionColumnsToColumnList(queryModel.getExpressions(), queryColumns,
                dimensions);
        return queryColumns;
    }

    private static void addCustomExpressionColumnsToColumnList(
            List<CustomCarbonAggregateExpression> expressions, Set<String> queryColumns,
            List<Dimension> dimensions) {
        for (CustomCarbonAggregateExpression expression : expressions) {
            List<Dimension> referredColumns = expression.getReferredColumns();
            for (Dimension refColumn : referredColumns) {
                Dimension dim =
                        CarbonQueryParseUtil.findDimension(dimensions, refColumn.getColName());
                if (null != dim) {
                    queryColumns.add(dim.getColName());
                }
            }
        }
    }

    /**
     * This method will traverse and populate the column list
     */
    private static void traverseAndPopulateColumnList(Expression filterExpression,
            List<Dimension> dimensions, List<Measure> measures, Set<String> columns) {
        if (null != filterExpression) {
            if (null != filterExpression.getChildren()
                    && filterExpression.getChildren().size() == 0) {
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
                if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
                        .equals(oneLoad.getLoadStatus())) {
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
                .removeDroppedColumnsFromLevelLRUCache(listOfLoadFolders, columns, schemaName,
                        cubeName, partitionCount);
    }
}
