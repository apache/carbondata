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

package org.carbondata.processing.suggest.datastats.analysis;

import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.datastats.LoadSampler;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.processing.suggest.datastats.util.DataStatsUtil;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.executer.QueryExecutor;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.LiteralExpression;
import org.carbondata.query.expression.conditional.InExpression;
import org.carbondata.query.expression.conditional.ListExpression;
import org.carbondata.query.holders.CarbonResultHolder;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;
import org.carbondata.query.result.RowResult;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * this class will read some sample data from store and analyze how its spread
 * on complete store
 *
 * @author A00902717
 */
public class SampleAnalyzer {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(SampleAnalyzer.class.getName());
    private LoadSampler loadSampler;
    private Cube cube;

    private String cubeUniqueName;

    private Level[] result;

    public SampleAnalyzer(LoadSampler loadSampler, Level[] result) {

        this.loadSampler = loadSampler;
        this.cube = loadSampler.getMetaCube();
        this.result = result;
        this.cubeUniqueName = loadSampler.getCubeUniqueName();

    }

    public void execute(List<Level> dimOrdCard, String partitionId) throws AggSuggestException {
        int totalDimensions = loadSampler.getDimensions().size();

        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Processing cardinality:" + dimOrdCard);
        for (int i = 0; i < dimOrdCard.size(); i++) {

            Level masterDim = dimOrdCard.get(i);
            //this array will have distinct relation of master dimension w.r.t to other dimension

            // setting default value to 1. We have considered totalDimensions because there is ignore_cardinality which is
            //not part of dimOrdCard and hence indexoutbound exception.
            //TO-DO explain this scenarion in more detail
            Map<Integer, Integer> distinctOfMasterDim =
                    new HashMap<Integer, Integer>(totalDimensions);

            for (int j = 0; j < i; j++) {
                // Step 1: we need to find distinct data of dimCO in earlier
                // calculated dimensions
                Level dim = dimOrdCard.get(j);
                // get distinctdata mapping for jth dimension ordinal
                Level dimDistinctData = null;
                for (Level level : result) {
                    if (level.getOrdinal() == dim.getOrdinal()) {
                        dimDistinctData = level;
                        break;
                    }
                }
                // this will give distinct data of dim w.r.t dimCO
                int distinct = 0;
                //findbug fix
                if (null != dimDistinctData && null != dimDistinctData
                        .getOtherDimesnionDistinctData()) {
                    Integer distinctInt = dimDistinctData.getOtherDimesnionDistinctData()
                            .get(masterDim.getOrdinal());
                    if (null != distinctInt) {
                        distinct = distinctInt;
                    }
                }
                /**
                 * formulae is (earlier computed cardinality)*distinct/current
                 * cardinality
                 */
                // this will calculate distinct data of dimCO w.r.t dim
                int res = (dim.getCardinality() * distinct) / masterDim.getCardinality();
                distinctOfMasterDim.put(dim.getOrdinal(), res);

            }
            LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "From 0 to " + i + " is calculated data");
            // find out distinct data for remaining dimensions
            ArrayList<Level> leftOut = new ArrayList<Level>(dimOrdCard.size());
            //It is i+1 because we are calculating distinct data for  ith ordinal
            for (int k = i; k < dimOrdCard.size(); k++) {
                leftOut.add(dimOrdCard.get(k));

            }
            if (leftOut.size() > 1) {
                //read sample data from fact and query all record for given sample on BPlus tree
                queryData(masterDim, leftOut, distinctOfMasterDim);

            }
            //setting result
            masterDim.setOtherDimesnionDistinctData(distinctOfMasterDim);
            //logResult(masterDim, distinctOfMasterDim);

        }
    }

    private void queryData(Level master, ArrayList<Level> slaves,
            Map<Integer, Integer> distinctOfMasterDim) throws AggSuggestException {
        List<Dimension> allDimensions = loadSampler.getDimensions();
        Dimension dimension = null;
        for (Dimension dim : allDimensions) {
            if (dim.getOrdinal() == master.getOrdinal()) {
                dimension = dim;
                break;
            }
        }
        //findbug fix
        if (null == dimension) {
            throw new AggSuggestException(
                    "dimension:" + master.getName() + ", is not available in visible dimension.");
        }
        // Sample data
        List<String> realDatas = loadSampler.getSampleData(dimension, cubeUniqueName);

        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Load size for dimension[" + dimension.getColName() + "]:" + realDatas.size());
        long queryExecutionStart = System.currentTimeMillis();
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Started with sample data:" + realDatas);
        //query on bplus tree
        querySampleDataOnBTree(dimension, realDatas, slaves, distinctOfMasterDim);

        long queryExecutionEnd = System.currentTimeMillis();
        long queryExecutionTimeTaken = queryExecutionEnd - queryExecutionStart;
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Finished with sample data,time taken[ms]:" + queryExecutionTimeTaken);

    }

    /**
     * @param master              : master dimension
     * @param tableName
     * @param data                : member value of given dimension
     * @param distinctOfMasterDim
     * @param NoDictionaryinalities
     * @return dimension's ordinal and its distinct value for given member value
     * @throws AggSuggestException
     */
    public void querySampleDataOnBTree(Dimension master, List<String> data, ArrayList<Level> slaves,
            Map<Integer, Integer> distinctOfMasterDim) throws AggSuggestException {

        ResultAnalyzer resultAnalyzer = new ResultAnalyzer(master);

        try {
            Set<String> columnsForQuery = new HashSet<String>(10);
            // Create QueryExecution model
            CarbonQueryExecutorModel queryExecutionModel =
                    createQueryExecutorModel(master, data, slaves, columnsForQuery);
            List<String> levelCacheKeys = null;
            if (InMemoryTableStore.getInstance().isLevelCacheEnabled()) {
                levelCacheKeys = DataStatsUtil
                        .validateAndLoadRequiredSlicesInMemory(loadSampler.getAllLoads(),
                                cubeUniqueName, columnsForQuery);
            }
            cube = CarbonMetadata.getInstance().getCube(cubeUniqueName);
            queryExecutionModel.setCube(cube);
            QueryExecutor queryExecutor = DataStatsUtil
                    .getQueryExecuter(queryExecutionModel.getCube(),
                            queryExecutionModel.getFactTable(), loadSampler.getQueryScopeObject());

            // Execute the query
            CarbonIterator<RowResult> rowIterator = queryExecutor.execute(queryExecutionModel);

            //delegate result analysiss
            //analyzed result will be set in distinctOfMasterDim
            resultAnalyzer.analyze(rowIterator, slaves, distinctOfMasterDim);
            if (null != levelCacheKeys) {
                for (String levelCacheKey : levelCacheKeys) {
                    InMemoryTableStore.getInstance().updateLevelAccessCountInLRUCache(levelCacheKey);
                }
            }

        } catch (Exception e) {
            throw new AggSuggestException(e.getMessage(), e);
        }

    }

    /**
     * Creating query model to execute on BPlus tree
     *
     * @param master
     * @param datas
     * @param slaves
     * @param columnsForQuery
     * @return
     */
    public CarbonQueryExecutorModel createQueryExecutorModel(Dimension master, List<String> datas,
            ArrayList<Level> slaves, Set<String> columnsForQuery) {
        CarbonQueryExecutorModel executorModel = new CarbonQueryExecutorModel();
        executorModel.setSparkExecution(true);
        String factTableName = cube.getFactTableName();
        executorModel.setCube(cube);
        executorModel.sethIterator(new CarbonResultHolder(new ArrayList<Type>(1)));
        executorModel.setFactTable(factTableName);

        List<Dimension> allDimensions = loadSampler.getDimensions();
        Dimension[] dimensions = new Dimension[slaves.size()];
        int index = 0;
        for (Level slave : slaves) {
            for (Dimension dimension : allDimensions) {
                if (dimension.getOrdinal() == slave.getOrdinal()) {
                    dimensions[index] = dimension;
                    columnsForQuery.add(dimension.getColName());
                    break;
                }
            }
            dimensions[index].setQueryOrder(index++);

        }
        executorModel.setSortedDimensions(new Dimension[0]);
        executorModel.setSortOrder(new byte[0]);
        executorModel.setDims(dimensions);
        executorModel.setMsrs(new ArrayList<Measure>(1));

        /**
         * Creating filter expression
         */
        // Create Expression which needs to be executed, for e.g select city
        // where mobileno=2
        // here mobileno=2 is an expression
        Expression left = new ColumnExpression(master.getDimName(),
                DataStatsUtil.getDataType(master.getDataType()));
        columnsForQuery.add(master.getDimName());
        ((ColumnExpression) left).setDim(master);
        ((ColumnExpression) left).setDimension(true);

        // creating all data as LiteralExpression and adding to list
        List<Expression> exprs = new ArrayList<Expression>(datas.size());
        for (String data : datas) {
            Expression right =
                    new LiteralExpression(data, DataStatsUtil.getDataType(master.getDataType()));
            exprs.add(right);
        }

        // Creating listexpression so that it can be used in InExpression
        Expression listExpr = new ListExpression(exprs);
        InExpression inExpr = new InExpression(left, listExpr);

        // EqualToExpression equalExpr = new EqualToExpression(left, right);
        executorModel.setFilterExpression(inExpr);

        List<DimensionAggregatorInfo> dimensionAggregatorInfos =
                new ArrayList<DimensionAggregatorInfo>(
                        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        executorModel.setConstraints(new HashMap<CarbonMetadata.Dimension, CarbonFilterInfo>(1));
        executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
        executorModel.setActualDimsRows(executorModel.getDims());
        executorModel.setActualDimsCols(new Dimension[0]);
        executorModel.setCalcMeasures(new ArrayList<Measure>(1));
        executorModel.setAnalyzerDims(executorModel.getDims());
        executorModel
                .setConstraintsAfterTopN(new HashMap<CarbonMetadata.Dimension, CarbonFilterInfo>(1));
        executorModel.setLimit(-1);
        executorModel.setDetailQuery(false);
        executorModel.setQueryId(System.nanoTime() + "");
        executorModel.setOutLocation(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS));
        return executorModel;
    }

}
