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

package org.carbondata.query.directinterface.impl;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.filesystem.MolapFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.metadata.CalculatedMeasure;
import org.carbondata.core.metadata.MolapMetadata;
import org.carbondata.core.metadata.MolapMetadata.Cube;
import org.carbondata.core.metadata.MolapMetadata.Dimension;
import org.carbondata.core.metadata.MolapMetadata.Measure;
import org.carbondata.core.olap.MolapDef;
import org.carbondata.core.olap.MolapDef.AggLevel;
import org.carbondata.core.olap.MolapDef.AggMeasure;
import org.carbondata.core.olap.MolapDef.Schema;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.datastorage.InMemoryCubeStore;
import org.carbondata.query.datastorage.cache.LevelInfo;
import org.carbondata.query.datastorage.cache.MolapLRULevelCache;
import org.carbondata.query.executer.MolapQueryExecutorModel;
import org.carbondata.query.executer.impl.topn.TopNModel;
import org.carbondata.query.expression.BinaryExpression;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filters.measurefilter.MeasureFilterModel;
import org.carbondata.query.filters.metadata.ContentMatchFilterInfo;
import org.carbondata.query.molapfilterinterface.ExpressionType;
import org.carbondata.query.queryinterface.filter.MolapFilterInfo;
import org.carbondata.query.queryinterface.query.MolapQuery;
import org.carbondata.query.queryinterface.query.MolapQuery.AxisType;
import org.carbondata.query.queryinterface.query.MolapQuery.SortType;
import org.carbondata.query.queryinterface.query.impl.MolapQueryImpl;
import org.carbondata.query.queryinterface.query.metadata.*;
import org.carbondata.query.queryinterface.query.metadata.MolapLevel.MolapLevelType;
import org.carbondata.query.util.MolapEngineLogEvent;

//import org.carbondata.core.olap.MolapDef.AggTable;

/**
 * It is util class to parse the molap query object
 */
public final class MolapQueryParseUtil {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapQueryParseUtil.class.getName());

    private MolapQueryParseUtil() {

    }

    /**
     * It parses the MOLAP query object and returns model object.
     *
     * @param molapQuery
     * @param schemaName
     * @param cubeName
     * @return
     * @throws IOException
     */
    public static MolapQueryModel parseMolapQuery(MolapQuery molapQuery, String schemaName,
            String cubeName) throws IOException {

        MolapQueryImpl queryImpl = (MolapQueryImpl) molapQuery;
        List<TopCount> topCounts = queryImpl.getTopCounts();
        MolapMetadata metadata = MolapMetadata.getInstance();
        String cubeUniqueName = schemaName + '_' + cubeName;
        Cube cube = metadata.getCube(cubeUniqueName);
        if (cube == null) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Scheme or Cube name does not exist");
            throw new RuntimeException("Scheme or Cube name does not exist");
        }

        String factTableName = cube.getFactTableName();

        MolapQueryModel model = new MolapQueryModel();
        if (null != queryImpl.getExtraProperties() && null != queryImpl.getExtraProperties()
                .get("ANALYZER_QUERY")) {
            model.setAnalyzer(true);
        }
        if (null != queryImpl.getExtraProperties() && null != queryImpl.getExtraProperties()
                .get("isSliceFilterPresent")) {
            model.setIsSliceFilterPresent(true);
        }

        //Call first time to get the dims and msrs as per the fact table
        fillMeta(queryImpl, cube, factTableName, model);
        model.setFactTableName(factTableName);

        //Find out the suitable table (aggregation table or fact table)
        /*Object pageRequire = queryImpl.getExtraProperties().get(MolapQuery.PAGINATION_REQUIRED);
        boolean isPaginationrequired=false;
        if(pageRequire != null)
        {
            isPaginationrequired = Boolean.parseBoolean(pageRequire.toString());
        }*/
        //Find out the suitable table (aggregation table or fact table)
        //        if(model.isAnalyzer() )
        //        {
        List<Dimension> dims = new ArrayList<Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        dims.addAll(model.getQueryDims());
        dims.addAll(model.getConstraints().keySet());

        model.setFactTableName(getSuitableTable(cube, dims, model.getMsrs()));
        List<String> errors = null;
        if (!model.getFactTableName().equals(factTableName)) {
            //Fill as per the selected table
            errors = fillMeta(queryImpl, cube, model.getFactTableName(), model);
        }
        //        }

        if (errors != null && errors.size() > 0) {
            factTableName = cube.getFactTableName();
            fillMeta(queryImpl, cube, factTableName, model);
        }

        TopNModel topNModel = null;
        if (topCounts.size() > 0) {
            topNModel = getTopNModel(topCounts.get(topCounts.size() - 1), cube, model.getMsrs(),
                    model.getQueryDimsRows(), model.getQueryDimsCols(), model.getFactTableName(),
                    model.getCalcMsrs());
        }
        if (model.getMsrFilter() != null && model.getMsrFilter().size() > 0) {
            processMeasureFilters(model.getMsrFilter(), model.getQueryDimsRows(),
                    model.getQueryDimsCols());
        }
        model.setCube(cube);
        if (!((null != queryImpl.getExtraProperties() && "true"
                .equals(queryImpl.getExtraProperties().get("ANALYZER_QUERY")) && null != topNModel
                && "min".equals(topNModel.getMeasure().getAggName()))
                || null != queryImpl.getExtraProperties() && "true"
                .equals(queryImpl.getExtraProperties().get("ANALYZER_QUERY")) && model
                .pushTopN())) {
            model.setTopNModel(topNModel);
        }

        setExtraProperties(queryImpl.getExtraProperties(), model);

        return model;
    }

    /**
     * @param cube
     * @param model
     * @throws IOException
     */
    public static String getSuitableTable(Cube cube, List<Dimension> dims, String schemaName,
            String cubeName) throws IOException {
        List<String> aggtables = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        String factTableName = cube.getFactTableName();

        Set<String> tablesList = cube.getTablesList();
        buildAggTablesList(cube, dims, aggtables, factTableName, tablesList);

        if (aggtables.size() == 0) {
            return cube.getFactTableName();
        }

        //        MolapExecutor executor = new InMemoryQueryExecutor(null,schemaName, cubeName);
        //        String selectedTabName = cube.getFactTableName();
        //        long selectedTabCount = Long.MAX_VALUE;
        //        for(String tableName : aggtables)
        //        {
        //            long count = executor.executeTableCount(tableName);
        //            if(true) //count > 0
        //            {
        //                if(count < selectedTabCount)
        //                {
        //                    selectedTabCount = count;
        //                    selectedTabName = tableName;
        //                }
        //            }
        //        }
        return cube.getFactTableName();
    }

    /**
     * @param cube
     * @param model
     * @throws IOException
     */
    public static String getSuitableTable(Cube cube, List<Dimension> dims, List<Measure> measures)
            throws IOException {
        List<String> aggtables = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<String> aggtablesMsrs = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        String factTableName = cube.getFactTableName();

        Set<String> tablesList = cube.getTablesList();
        buildAggTablesList(cube, dims, aggtables, factTableName, tablesList);

        if (measures.size() == 0 && aggtables.size() > 0) {
            return aggtables.get(0);
        }

        for (String tableName : aggtables) {
            List<Measure> aggMsrs = cube.getMeasures(tableName);
            boolean present = false;
            for (Measure msr : measures) {
                boolean found = false;
                for (Measure aggMsr : aggMsrs) {
                    if (msr.getName().equals(aggMsr.getName()) && msr.getAggName()
                            .equals(aggMsr.getAggName())) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    present = true;
                } else {
                    present = false;
                    break;
                }
            }

            if (present) {
                aggtablesMsrs.add(tableName);
            }
        }

        if (aggtablesMsrs.size() == 0) {
            return cube.getFactTableName();
        }

        return getTabName(cube, aggtablesMsrs);
    }

    // TODO SIMIAN

    /**
     * @param cube
     * @param model
     * @throws IOException
     */
    public static String getSuitableTable(Map<String, DimensionAggregatorInfo> dimAggregatorInfos,
            Schema schema, Cube cube, List<Dimension> dims, MolapQueryExecutorModel executorModel,
            String storeLocation, int partitionCount) throws IOException {
        List<String> aggtables = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        Map<String, Integer> tableMeasureCountMapping =
                new HashMap<String, Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<Measure> measures = executorModel.getMsrs();
        AggMeasure[] aggMeasures = null;
        MolapDef.Table table = (MolapDef.Table) schema.cubes[0].fact;
        MolapDef.AggTable[] aggTables = table.aggTables;
        String aggTableName = null;
        AggLevel[] aggLevels = null;
        for (int i = 0; i < aggTables.length; i++) {
            aggTableName = ((MolapDef.AggName) aggTables[i]).getNameAttribute();
            aggLevels = aggTables[i].levels;
            aggMeasures = aggTables[i].measures;
            boolean present = true;
            String dimensionFullName = "";
            for (Dimension dim : dims) {
                boolean found = false;
                for (AggLevel aggDim : aggLevels) {
                    dimensionFullName =
                            '[' + dim.getDimName() + "].[" + dim.getHierName() + "].[" + dim
                                    .getName() + ']';
                    if (dimensionFullName.equals(aggDim.name)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    present = false;
                    break;
                }
            }

            if (present) {
                for (Measure msr : measures) {
                    boolean found = false;
                    for (AggMeasure aggMsrObj : aggMeasures) {
                        if (msr.getName().equals(aggMsrObj.column) && msr.getAggName()
                                .equals(aggMsrObj.aggregator)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        present = false;
                        break;
                    }
                }
                if (present && isDimAggInfoValidForAggTable(dimAggregatorInfos,
                        cube.getDimensions(cube.getFactTableName()), aggMeasures)) {
                    List<Byte> columnPresentList = new ArrayList<Byte>();
                    checkAllColumnsPresent(executorModel.getFilterExpression(),
                            cube.getDimensions(aggTableName), cube.getMeasures(aggTableName),
                            columnPresentList);
                    if (columnPresentList.size() == 0) {
                        aggtables.add(aggTableName);
                        // plus 1 for count measure that is added internally
                        tableMeasureCountMapping.put(aggTableName, (aggMeasures.length + 1));
                    }
                }
            }
        }
        if (aggtables.size() == 0) {
            return cube.getFactTableName();
        } else if (aggtables.size() == 1) {
            return aggtables.get(0);
        } else {
            return getMostSuitableAggregateTable(schema, cube, tableMeasureCountMapping, aggtables,
                    storeLocation, partitionCount);
        }
    }

    private static void checkAllColumnsPresent(Expression expressionTree,
            List<Dimension> dimensions, List<Measure> measures, List<Byte> columnPresentList) {
        if (null == expressionTree || columnPresentList.size() > 0) {
            return;
        }
        ExpressionType filterExpressionType = expressionTree.getFilterExpressionType();
        BinaryExpression currentExpression = null;
        if (null != filterExpressionType) {
            switch (filterExpressionType) {
            case OR:
            case AND:
                currentExpression = (BinaryExpression) expressionTree;
                checkAllColumnsPresent(currentExpression.getLeft(), dimensions, measures,
                        columnPresentList);
                checkAllColumnsPresent(currentExpression.getRight(), dimensions, measures,
                        columnPresentList);
                return;
            default:
                ConditionalExpression condExpression = (ConditionalExpression) expressionTree;
                List<ColumnExpression> columnList = condExpression.getColumnList();
                for (ColumnExpression c : columnList) {
                    if (c.isDimension()) {
                        if (!isDimensionPresent(dimensions, c.getDim())) {
                            columnPresentList.add((byte) 0);
                        }
                    } else {
                        if (!isMeasurePresent(measures, (Measure) c.getDim())) {
                            columnPresentList.add((byte) 0);
                        }
                    }
                }
            }
        }
    }

    private static boolean isMeasurePresent(List<Measure> measures, Measure dim) {
        for (Measure m : measures) {
            if (m.equals(dim)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDimensionPresent(List<Dimension> dimensions, Dimension dim) {
        for (Dimension m : dimensions) {
            if (m.equals(dim)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param schema
     * @param cube
     * @param tableMeasureCountMapping
     * @param aggtables
     * @param storeLocation
     * @param partitionCount
     * @return
     */
    private static String getMostSuitableAggregateTable(Schema schema, Cube cube,
            Map<String, Integer> tableMeasureCountMapping, List<String> aggtables,
            String storeLocation, int partitionCount) {
        int currentRestructNumber = getCurrentRestructureNumber(cube);
        String msrMetadataFilePath = null;
        String schemaName = null;
        String cubeName = null;
        String restructureFolder = null;
        String aggTablePath = null;
        String aggTableName = null;
        String selectedTableName = null;
        double minNoOfrecords = 0;
        MeasureMetaDataModel model = null;
        Object[] maxValue = null;
        Iterator<String> aggTblItr = aggtables.iterator();
        while (aggTblItr.hasNext()) {
            double recordCount = 0;
            aggTableName = aggTblItr.next();
            for (int i = currentRestructNumber; i >= 0; i--) {
                for (int j = 0; j < partitionCount; j++) {
                    schemaName = schema.name + '_' + j;
                    cubeName = cube.getOnlyCubeName() + '_' + j;
                    restructureFolder = MolapCommonConstants.RESTRUCTRE_FOLDER + i;
                    aggTablePath =
                            storeLocation + File.separator + schemaName + File.separator + cubeName
                                    + File.separator + restructureFolder + File.separator
                                    + aggTableName;
                    MolapFile[] listFiles =
                            getLoadFolders(aggTablePath, MolapCommonConstants.LOAD_FOLDER);
                    for (int k = 0; k < listFiles.length; k++) {
                        msrMetadataFilePath = listFiles[k].getAbsolutePath()
                                + MolapCommonConstants.MEASURE_METADATA_FILE_NAME + aggTableName
                                + MolapCommonConstants.MEASUREMETADATA_FILE_EXT;
                        if (!isFileExist(msrMetadataFilePath)) {
                            continue;
                        }
                        model = ValueCompressionUtil.readMeasureMetaDataFile(msrMetadataFilePath,
                                tableMeasureCountMapping.get(aggTableName));
                        maxValue = model.getMaxValue();
                        recordCount = recordCount + (double) maxValue[maxValue.length - 1];
                    }
                }
            }
            if (null == selectedTableName) {
                selectedTableName = aggTableName;
                minNoOfrecords = recordCount;
            } else {
                if (0 != recordCount && recordCount < minNoOfrecords) {
                    selectedTableName = aggTableName;
                    minNoOfrecords = recordCount;
                }
            }
        }
        return selectedTableName;
    }

    /**
     * @param baseStorePath
     * @return
     */
    private static boolean isFileExist(String baseStorePath) {
        FileType fileType = FileFactory.getFileType(baseStorePath);
        try {
            if (FileFactory.isFileExist(baseStorePath, fileType)) {
                return true;
            }
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@@Problem checking file measuremetadata file existence@@@@");
        }
        return false;
    }

    /**
     * @param baseStorePath
     * @param filterType
     * @return
     */
    private static MolapFile[] getLoadFolders(String baseStorePath, final String filterType) {
        MolapFile molapFile =
                FileFactory.getMolapFile(baseStorePath, FileFactory.getFileType(baseStorePath));
        // List of directories
        MolapFile[] listFiles = molapFile.listFiles(new MolapFileFilter() {
            @Override public boolean accept(MolapFile pathname) {
                if (pathname.isDirectory()) {
                    if (pathname.getAbsolutePath().indexOf(filterType) > -1) {
                        return true;
                    }
                }
                return false;
            }
        });
        return listFiles;
    }

    /**
     * @param cube
     * @return
     */
    private static int getCurrentRestructureNumber(Cube cube) {
        int currentRestructNumber = MolapUtil
                .checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath(), "RS_",
                        false);
        if (-1 == currentRestructNumber) {
            currentRestructNumber = 0;
        }
        return currentRestructNumber;
    }

    private static boolean isDimAggInfoValidForAggTable(
            Map<String, DimensionAggregatorInfo> dimAggregatorInfos, List<Dimension> dimensions,
            AggMeasure[] aggMeasures) {
        String dimColumnName = null;
        DimensionAggregatorInfo value = null;
        Dimension dimension = null;
        for (Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos.entrySet()) {
            dimColumnName = entry.getKey();
            value = entry.getValue();
            dimension = findDimension(dimensions, dimColumnName);
            List<String> aggList = value.getAggList();
            for (int i = 0; i < aggList.size(); i++) {
                if (!isDimensionMeasureInAggTable(aggMeasures, dimension, aggList.get(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Find the dimension from metadata by using unique name. As of now we are
     * taking level name as unique name. But user needs to give one unique name
     * for each level,that level he needs to mention in query.
     *
     * @param dimensions
     * @param molapDim
     * @return
     */
    public static Dimension findDimension(List<Dimension> dimensions, String molapDim) {
        Dimension findDim = null;
        for (Dimension dimension : dimensions) {
            // Its just a temp work around to use level name as unique name. we
            // need to provide a way to configure unique name
            // to user in schema.
            if (dimension.getName().equalsIgnoreCase(molapDim)) {
                findDim = dimension.getDimCopy();
                findDim.setActualCol(true);
                break;
            }
        }
        return findDim;
    }

    /**
     * @param schema
     * @param cube
     * @param dim
     * @param agg
     * @param aggTable
     * @return
     */
    public static boolean isDimensionMeasureInAggTable(AggMeasure[] aggMeasures, Dimension dim,
            String agg) {
        // AggMeasure[] aggMeasures = null;
        // MolapDef.Table table = (MolapDef.Table)schema.cubes[0].fact;
        // MolapDef.AggTable[] aggTables = table.aggTables;
        // String aggTableName = null;
        // for(int i = 0;i < aggTables.length;i++)
        // {
        // aggTableName = ((MolapDef.AggName)aggTables[i]).getNameAttribute();
        // if(aggTableName.equals(aggTable))
        // {
        // aggMeasures = aggTables[i].measures;
        // break;
        // }
        // }
        String dimensionFullName = "";
        dimensionFullName =
                '[' + dim.getDimName() + "].[" + dim.getHierName() + "].[" + dim.getName() + ']';
        for (AggMeasure aggMsrObj : aggMeasures) {
            if (dimensionFullName.equals(aggMsrObj.name) && agg.equals(aggMsrObj.aggregator)) {
                return true;
            }
        }
        return false;
    }

    //    /**
    //     *
    //     * @param schema
    //     * @param cube
    //     * @param dim
    //     * @param aggTable
    //     * @return
    //     *
    //     */
    //    public static boolean isLevelInAggTable(Schema schema, Cube cube, Dimension dim, String aggTable)
    //    {
    //        AggLevel[] aggLevels = null;
    //        MolapDef.Table table = (MolapDef.Table)schema.cubes[0].fact;
    //        MolapDef.AggTable[] aggTables = table.aggTables;
    //        String aggTableName = null;
    //        for(int i = 0;i < aggTables.length;i++)
    //        {
    //            aggTableName = ((MolapDef.AggName)aggTables[i]).getNameAttribute();
    //            if(aggTableName.equals(aggTable))
    //            {
    //                aggLevels = aggTables[i].levels;
    //                break;
    //            }
    //        }
    //        String dimensionFullName = "";
    //        dimensionFullName = '[' + dim.getDimName() + "].[" + dim.getHierName() + "].[" + dim.getName() + ']';
    //        for(AggLevel agglevel : aggLevels)
    //        {
    //            if(dimensionFullName.equals(agglevel.name))
    //            {
    //                return true;
    //            }
    //        }
    //        return false;
    //    }

    /**
     * @param cube
     * @param aggtablesMsrs
     * @return
     */
    private static String getTabName(Cube cube, List<String> aggtablesMsrs) {
        //        MolapExecutor executor = new InMemoryQueryExecutor(null);
        String selectedTabName = cube.getFactTableName();
        long selectedTabCount = Long.MAX_VALUE;
        for (String tableName : aggtablesMsrs) {
            long count =
                    cube.getDimensions(tableName).size();//executor.executeTableCount(tableName);
            //            if(true) //count > 0
            //            {
            if (count < selectedTabCount) {
                selectedTabCount = count;
                selectedTabName = tableName;
            }
            //            }
        }
        return selectedTabName;
    }

    /**
     * @param cube
     * @param dims
     * @param aggtables
     * @param factTableName
     * @param tablesList
     */
    private static void buildAggTablesList(Cube cube, List<Dimension> dims, List<String> aggtables,
            String factTableName, Set<String> tablesList) {
        for (String tableName : tablesList) {
            if (!tableName.equals(factTableName)) {
                List<Dimension> aggDims = cube.getDimensions(tableName);
                boolean present = true;
                for (Dimension dim : dims) {
                    boolean found = false;
                    for (Dimension aggDim : aggDims) {
                        if (dim.getDimName().equals(aggDim.getDimName()) && dim.getHierName()
                                .equals(aggDim.getHierName()) && dim.getName()
                                .equals(aggDim.getName())) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        present = false;
                    }
                }

                if (present) {
                    aggtables.add(tableName);
                }
            }
        }
    }

    /**
     * Set the extra properties
     *
     * @param extraProps
     * @param model
     */
    private static void setExtraProperties(Map<String, Object> extraProps, MolapQueryModel model) {
        if (extraProps == null || extraProps.size() == 0) {
            return;
        }
        Object grandTotal = extraProps.get("GRAND_TOTAL_ALL");
        Object relativeFilter = extraProps.get("RELATIVE_FILTER");
        Object subTotal = extraProps.get("SUB_TOTAL");
        if (grandTotal != null) {
            model.setGrandTotalForAllRows(true);
        }
        if (relativeFilter != null) {
            model.setRelativefilter(true);
        }
        if (null != subTotal) {
            model.setSubTotal(true);
        }

        Object pageRequire = extraProps.get(MolapQuery.PAGINATION_REQUIRED);
        if (pageRequire != null) {
            boolean parseBoolean = Boolean.parseBoolean(pageRequire.toString());
            if (parseBoolean) {

                Object range = extraProps.get(MolapQuery.PAGE_RANGE);
                String[] split = range.toString().split("-");
                int[] pageRange = new int[2];
                pageRange[0] = Integer.parseInt(split[0]);
                pageRange[1] = Integer.parseInt(split[1]);
                model.setPaginationRequired(parseBoolean);
                model.setRowRange(pageRange);
                model.setQueryId(extraProps.get(MolapQuery.QUERY_ID).toString());
            }
        }
        Object trans = extraProps.get(MolapQuery.TRANSFORMATIONS);
        model.setMolapTransformations((List) trans);
    }

    /**
     * It fills the model object from query object.
     *
     * @param queryImpl
     * @param cube
     * @param factTableName
     * @param model
     * @return List<String> list of errors
     */
    private static List<String> fillMeta(MolapQueryImpl queryImpl, Cube cube, String factTableName,
            MolapQueryModel model) {

        List<MolapMetadata.Dimension> queryDims =
                new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<MolapMetadata.Dimension> queryDimsRows =
                new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<MolapMetadata.Dimension> queryDimsCols =
                new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<MolapMetadata.Dimension> queryDimsIncludeDynamicLevels =
                new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<MolapMetadata.Dimension> queryDimsRowsIncludeDynamicLevels =
                new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<MolapMetadata.Dimension> queryDimsColsIncludeDynamicLevels =
                new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        List<Measure> msrs = new ArrayList<Measure>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<CalculatedMeasure> calMsrs =
                new ArrayList<CalculatedMeasure>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        Map<Measure, MeasureFilterModel[]> msrFilterModels =
                new HashMap<Measure, MeasureFilterModel[]>(
                        MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        Map<Dimension, MolapFilterInfo> constraints = new HashMap<Dimension, MolapFilterInfo>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        Map<Measure, MeasureFilterModel[]> msrFilterModelsAfterTopN =
                new HashMap<Measure, MeasureFilterModel[]>(
                        MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        Map<Dimension, MolapFilterInfo> constraintsAfterTopN =
                new HashMap<Dimension, MolapFilterInfo>(
                        MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        Map<String, MolapFilterWrapper> filetrWrapperConstMap =
                new HashMap<String, MolapFilterWrapper>(
                        MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        List<Integer> sortTypes = new ArrayList<Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<Integer> sortTypesIncludeDynamicLevels =
                new ArrayList<Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        MeasureSortModel sortModel = null;

        Axis[] axises = queryImpl.getAxises();
        List<String> errors = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for (int i = 0; i < axises.length; i++) {
            List<MolapLevelHolder> dims = axises[i].getDims();
            AxisType axisType = AxisType.ROW;

            if (i == 1) {
                axisType = AxisType.COLUMN;
            } else if (i == 2) {
                axisType = AxisType.SLICE;
            }
            for (MolapLevelHolder holder : dims) {
                if (holder.getLevel().getType().equals(MolapLevelType.DIMENSION)) {
                    processDimension(cube, factTableName, queryDimsRows, queryDimsCols, constraints,
                            constraintsAfterTopN, sortTypes, errors, holder, axisType,
                            model.isAnalyzer(), filetrWrapperConstMap,
                            queryDimsRowsIncludeDynamicLevels, queryDimsColsIncludeDynamicLevels,
                            sortTypesIncludeDynamicLevels);
                } else {
                    sortModel = processMeasure(cube, factTableName, msrs, calMsrs, msrFilterModels,
                            msrFilterModelsAfterTopN, sortModel, errors, holder, axisType,
                            queryImpl.isBreakHierarchyTopN());
                }
            }
        }

        //        for(Entry<String, MolapFilterWrapper> entry : filetrWrapperConstMap.entrySet())
        //        {
        //            MolapFilterWrapper value = entry.getValue();
        //            fillConstraints(queryImpl, constraints, value, cube);
        //        }

        queryDims.addAll(queryDimsRows);
        queryDims.addAll(queryDimsCols);
        model.setQueryDims(queryDims);
        model.setMsrs(msrs);
        model.setCalcMsrs(calMsrs);
        model.setConstraints(constraints);
        model.setConstraintsAfterTopN(constraintsAfterTopN);
        Set<Measure> setOfMeasures = msrFilterModels.keySet();
        if (null != setOfMeasures && setOfMeasures.size() > 0) {
            Iterator<Measure> itr = setOfMeasures.iterator();
            Measure msr;
            boolean hasNext = itr.hasNext();
            while (hasNext) {
                msr = itr.next();
                if ((null != queryImpl.getExtraProperties() && "true"
                        .equals(queryImpl.getExtraProperties().get("ANALYZER_QUERY")) && !"true"
                        .equals(queryImpl.getExtraProperties().get("PAGINATION_REQUIRED")) && ("avg"
                        .equals(msr.getAggName())))) {
                    model.pushTopNToOlapEngine(true);
                }
                hasNext = itr.hasNext();
            }
        }
        if (!model.pushTopN()) {
            model.setMsrFilter(msrFilterModels);
        }
        model.setMsrFilterAfterTopN(msrFilterModelsAfterTopN);
        model.setDimSortTypes(convertToArray(sortTypes));
        model.setQueryDimsCols(queryDimsCols);
        model.setQueryDimsRows(queryDimsRows);
        model.setSortModel(sortModel);

        queryDimsIncludeDynamicLevels.addAll(queryDimsRowsIncludeDynamicLevels);
        queryDimsIncludeDynamicLevels.addAll(queryDimsColsIncludeDynamicLevels);
        model.setGlobalDimSortTypes(convertToArray(sortTypesIncludeDynamicLevels));
        model.setGlobalQueryDimsCols(queryDimsColsIncludeDynamicLevels);
        model.setGlobalQueryDimsRows(queryDimsRowsIncludeDynamicLevels);
        model.setGlobalQueryDims(queryDimsIncludeDynamicLevels);

        //Update the mesure index of sort model as per the requested measure order. 
        if (sortModel != null) {
            int msrIndex = -1;
            Measure msr = sortModel.getMeasure();
            int i = 0;
            for (Measure m : msrs) {
                if (msr.getName().equals(m.getName())) {
                    msrIndex = i;
                    break;
                }
                i++;
            }
            sortModel.setMeasureIndex(msrIndex);

        }

        return errors;
    }

/*
    *//**
     *
     * @param queryImpl
     * @param constraints
     * @param value
     *
     *//*
    private static void fillConstraints(MolapQueryImpl queryImpl, Map<Dimension, MolapFilterInfo> constraints,
            MolapFilterWrapper value, Cube cube)
    {
        if((null != queryImpl.getExtraProperties())
                && "true".equals(queryImpl.getExtraProperties().get("ANALYZER_QUERY")))
        {
            constraints.putAll(value.getFilters(true, cube));
        }
        else
        {
            constraints.putAll(value.getFilters(false, cube));
        }
    }

*/

    /**
     * @param cube
     * @param factTableName
     * @param msrs
     * @param calMsrs
     * @param msrFilterModels
     * @param sortModel
     * @param errors
     * @param i
     * @param holder
     * @param b
     * @return
     */
    private static MeasureSortModel processMeasure(Cube cube, String factTableName,
            List<Measure> msrs, List<CalculatedMeasure> calMsrs,
            Map<Measure, MeasureFilterModel[]> msrFilterModels,
            Map<Measure, MeasureFilterModel[]> msrFilterModelsAfterTopN, MeasureSortModel sortModel,
            List<String> errors, MolapLevelHolder holder, AxisType axisType,
            boolean isBreakHierarchy) {
        MolapMeasure measure = (MolapMeasure) holder.getLevel();
        Measure msr = null;
        if (measure.getType().equals(MolapLevelType.CALCULATED_MEASURE)) {
            if (measure.getType().equals(MolapLevelType.CALCULATED_MEASURE)) {
                MolapCalculatedMeasure mcm = (MolapCalculatedMeasure) measure;
                CalculatedMeasure cmsr = new CalculatedMeasure(null, measure.getName());
                if (mcm.getGroupDimensionLevel() != null) {
                    cmsr.setDistCountDim(cube.getDimensionByLevelName(
                            mcm.getGroupDimensionLevel().getDimensionName(),
                            mcm.getGroupDimensionLevel().getHierarchyName(),
                            mcm.getGroupDimensionLevel().getName(), factTableName));
                }
                msr = cmsr;
            }
        } else {
            msr = cube.getMeasure(factTableName, measure.getName());
        }
        if (null == msr) {
            msr = getMsrFromFactTableIfCalculatedBaseMsr(cube, factTableName, measure);
        }
        if (msr == null) {
            errors.add(measure.getName());
        } else {
            if (axisType == MolapQuery.AxisType.COLUMN || axisType == MolapQuery.AxisType.ROW) {
                if (msr instanceof CalculatedMeasure) {
                    calMsrs.add((CalculatedMeasure) msr);

                } else {
                    msrs.add(msr);
                }

                if (!holder.getSortType().equals(SortType.NONE)) {
                    sortModel = new MeasureSortModel(msr, holder.getSortType().getSortValue());
                    if (holder.getSortType().equals(SortType.BASC) || holder.getSortType()
                            .equals(SortType.BDESC)) {
                        sortModel.setBreakHeir(true);
                    }
                }
            }
        }

        List<MolapMeasureFilter> msrFilters = holder.getMsrFilters();
        if (msrFilters != null && msrFilters.size() > 0) {
            MolapDimensionLevel dimensionLevel = measure.getDimensionLevel();
            Dimension dimensionByLevelName = null;
            if (dimensionLevel != null) {
                dimensionByLevelName =
                        cube.getDimensionByLevelName(dimensionLevel.getDimensionName(),
                                dimensionLevel.getHierarchyName(), dimensionLevel.getName(),
                                factTableName);
            }
            setMeasureFilterModels(msrFilters, dimensionByLevelName, msrFilterModels,
                    msrFilterModelsAfterTopN, msr, isBreakHierarchy);
        }
        return sortModel;
    }

    private static Measure getMsrFromFactTableIfCalculatedBaseMsr(Cube cube, String factTableName,
            MolapMeasure measure) {
        if (!cube.getFactTableName().equals(factTableName)) {
            Measure msr = cube.getMeasure(cube.getFactTableName(), measure.getName());
            //  if(msr instanceof CalculatedMeasure)
            //  {
            return msr;
            //}
        }
        return null;
    }

    /**
     * @param cube
     * @param factTableName
     * @param queryDimsRows
     * @param queryDimsCols
     * @param constraints
     * @param sortTypes
     * @param errors
     * @param i
     * @param holder
     */
    private static void processDimension(Cube cube, String factTableName,
            List<MolapMetadata.Dimension> queryDimsRows,
            List<MolapMetadata.Dimension> queryDimsCols,
            Map<Dimension, MolapFilterInfo> constraints,
            Map<Dimension, MolapFilterInfo> constraintsAfterTopN, List<Integer> sortTypes,
            List<String> errors, MolapLevelHolder holder, AxisType axisType,
            boolean isAnalyzerQuery, Map<String, MolapFilterWrapper> filetrWrapperConstMap,
            List<Dimension> queryDimsRowsIncludeDynamicLevels,
            List<Dimension> queryDimsColsIncludeDynamicLevels,
            List<Integer> sortTypesIncludeDynamicLevels) {
        MolapDimensionLevel dimensionLevel = (MolapDimensionLevel) holder.getLevel();
        Dimension dim = cube.getDimensionByLevelName(dimensionLevel.getDimensionName(),
                dimensionLevel.getHierarchyName(), dimensionLevel.getName(), factTableName);
        if (dim == null) {
            //            if(holder.getLevel().getType().equals(MolapLevelType.DYNAMIC_DIMENSION))
            //            {
            //                Dimension dimension = new Dimension(dimensionLevel.getName(), 0, dimensionLevel.getName(),cube);
            //                dimension.setHierName(dimensionLevel.getHierarchyName());
            //                dimension.setDimName(dimensionLevel.getDimensionName());
            //
            //                if(axisType == AxisType.ROW)
            //                {
            //                    if(!queryDimsRowsIncludeDynamicLevels.contains(dimension))
            //                    {
            //                        queryDimsRowsIncludeDynamicLevels.add(dimension);
            //                    }
            //                }
            //                else if(axisType == AxisType.COLUMN)
            //                {
            //                    if(!queryDimsColsIncludeDynamicLevels.contains(dimension))
            //                    {
            //                        queryDimsColsIncludeDynamicLevels.add(dimension);
            //                    }
            //                }
            //
            //                sortTypesIncludeDynamicLevels.add(getSortByte(holder.getSortType()));
            //            }
            errors.add(dimensionLevel.getName());
        } else {
            if (axisType == AxisType.ROW) {
                if (!queryDimsRows.contains(dim)) {
                    queryDimsRows.add(dim);
                }

                if (!queryDimsRowsIncludeDynamicLevels.contains(dim)) {
                    queryDimsRowsIncludeDynamicLevels.add(dim);
                }
            } else if (axisType == AxisType.COLUMN) {
                if (!queryDimsCols.contains(dim)) {
                    queryDimsCols.add(dim);
                }
                if (!queryDimsColsIncludeDynamicLevels.contains(dim)) {
                    queryDimsColsIncludeDynamicLevels.add(dim);
                }
            }
            sortTypes.add(holder.getSortType() == SortType.DESC ? 1 : 0);

            sortTypesIncludeDynamicLevels.add(getSortByte(holder.getSortType()));

        }
        MolapDimensionLevelFilter dimLevelFilter = holder.getDimLevelFilter();
        //SorceMonitor Fix
        updateConstraints(constraints, constraintsAfterTopN, isAnalyzerQuery, dim, dimLevelFilter,
                filetrWrapperConstMap);
    }

    private static int getSortByte(SortType sortType) {
        if (sortType == SortType.DESC) {
            return 1;
        } else if (sortType == SortType.ASC) {
            return 0;
        } else {
            return -1;
        }
    }

    private static void updateConstraints(Map<Dimension, MolapFilterInfo> constraints,
            Map<Dimension, MolapFilterInfo> constraintsAfterTopN, boolean isAnalyzerQuery,
            Dimension dim, MolapDimensionLevelFilter dimLevelFilter,
            Map<String, MolapFilterWrapper> filetrWrapperConstMap) {
        if (dimLevelFilter != null) {
            MolapFilterInfo molapFilterInfoCurrent = getMolapFilterInfo(dimLevelFilter);
            if (dimLevelFilter.isAfterTopN()) {
                if (isAnalyzerQuery) {
                    molapFilterInfoCurrent = createFilterForTopNAfter(molapFilterInfoCurrent);
                }
                constraintsAfterTopN.put(dim, molapFilterInfoCurrent);
            } else {
                MolapFilterInfo molapFilterInfo = constraints.get(dim);
                //Merge the dimension filters.
                if (molapFilterInfo != null) {
                    if (molapFilterInfo instanceof ContentMatchFilterInfo) {
                        molapFilterInfo.getIncludedMembers()
                                .addAll(molapFilterInfoCurrent.getIncludedMembers());
                        molapFilterInfo.getExcludedMembers()
                                .addAll(molapFilterInfoCurrent.getExcludedMembers());
                        molapFilterInfoCurrent = molapFilterInfo;
                    } else if (molapFilterInfoCurrent instanceof ContentMatchFilterInfo) {
                        molapFilterInfoCurrent.getIncludedMembers()
                                .addAll(molapFilterInfo.getIncludedMembers());
                        molapFilterInfoCurrent.getExcludedMembers()
                                .addAll(molapFilterInfo.getExcludedMembers());
                        if (molapFilterInfo instanceof ContentMatchFilterInfo) {
                            ContentMatchFilterInfo contentMatchFilterInfo =
                                    (ContentMatchFilterInfo) molapFilterInfo;
                            List<String> excludedContentMatchMembers =
                                    ((ContentMatchFilterInfo) molapFilterInfoCurrent)
                                            .getExcludedContentMatchMembers();
                            if (excludedContentMatchMembers == null) {
                                ((ContentMatchFilterInfo) molapFilterInfoCurrent)
                                        .setExcludedContentMatchMembers(contentMatchFilterInfo
                                                .getIncludedContentMatchMembers());
                            } else {
                                excludedContentMatchMembers.addAll(contentMatchFilterInfo
                                        .getIncludedContentMatchMembers());
                            }

                            List<String> includedContentMatchMembers =
                                    ((ContentMatchFilterInfo) molapFilterInfoCurrent)
                                            .getIncludedContentMatchMembers();
                            if (includedContentMatchMembers == null) {
                                ((ContentMatchFilterInfo) molapFilterInfoCurrent)
                                        .setIncludedContentMatchMembers(contentMatchFilterInfo
                                                .getIncludedContentMatchMembers());
                            } else {
                                includedContentMatchMembers.addAll(contentMatchFilterInfo
                                        .getIncludedContentMatchMembers());
                            }
                        }
                    }
                    if (!(molapFilterInfo instanceof ContentMatchFilterInfo)
                            && !(molapFilterInfoCurrent instanceof ContentMatchFilterInfo)) {
                        molapFilterInfoCurrent.getIncludedMembers()
                                .addAll(molapFilterInfo.getIncludedMembers());
                        molapFilterInfoCurrent.getExcludedMembers()
                                .addAll(molapFilterInfo.getExcludedMembers());
                    }

                }

                constraints.put(dim, molapFilterInfoCurrent);
                //                MolapFilterWrapper molapFilterWrapper = filetrWrapperConstMap.get(dim.getDimName() + '_' +dim.getHierName());
                //
                //                if(null == molapFilterWrapper)
                //                {
                //                    molapFilterWrapper = new MolapFilterWrapper();
                //                    filetrWrapperConstMap.put(dim.getDimName() + '_' + dim.getHierName(), molapFilterWrapper);
                //
                //                }
                //                molapFilterWrapper.addDimensionAndFilter(dim, dimLevelFilter);

            }
        }
    }

    //TODO SIMIAN
    private static MolapFilterInfo createFilterForTopNAfter(
            MolapFilterInfo molapFilterInfoCurrent) {
        List<String> includedMembers = molapFilterInfoCurrent.getIncludedMembers();
        List<String> excludedMembers = molapFilterInfoCurrent.getExcludedMembers();
        MolapFilterInfo temp = new MolapFilterInfo();
        Set<String> tempIncludedMembers = new LinkedHashSet<String>();
        Set<String> tempExcludedMembers = new LinkedHashSet<String>();
        String string;
        String[] split;
        StringBuilder builder = new StringBuilder();
        int includeMemberSize = includedMembers.size();
        for (int i = 0; i < includeMemberSize; i++) {
            string = includedMembers.get(i);
            split = string.split("\\.");
            builder.setLength(0);
            for (int j = 0; j < split.length - 1; j++) {
                builder.append("[");
                builder.append(split[j]);
                builder.append("]");
                builder.append(".");
            }
            builder.append("[");
            builder.append(split[split.length - 1]);
            builder.append("]");
            tempIncludedMembers.add(builder.toString());
        }
        int excludeMemberSize = excludedMembers.size();
        for (int i = 0; i < excludeMemberSize; i++) {
            string = excludedMembers.get(i);
            split = string.split("\\.");
            builder.setLength(0);
            for (int m = 0; m < split.length - 1; m++) {
                builder.append("[");
                builder.append(split[m]);
                builder.append("]");
                builder.append(".");
            }
            builder.append("[");
            builder.append(split[split.length - 1]);
            builder.append("]");
            tempExcludedMembers.add(builder.toString());
        }
        temp.addAllIncludedMembers(new ArrayList<String>(tempIncludedMembers));
        temp.addAllExcludedMembers(new ArrayList<String>(tempExcludedMembers));
        return temp;
    }

    /**
     * Convert MolapMeasureFilter to the MeasureFilterModel objects.
     *
     * @param msrFilters
     * @param isBreakHierarchy
     * @return MeasureFilterModel[]
     */
    private static void setMeasureFilterModels(List<MolapMeasureFilter> msrFilters,
            Dimension dimensionByLevelName, Map<Measure, MeasureFilterModel[]> msrFilterModels,
            Map<Measure, MeasureFilterModel[]> msrFilterModelsAfterTopN, Measure msr,
            boolean isBreakHierarchy) {
        //        MeasureFilterModel[] models = new MeasureFilterModel[msrFilters.size()];
        List<MeasureFilterModel> models =
                new ArrayList<MeasureFilterModel>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<MeasureFilterModel> modelsAfterTopN =
                new ArrayList<MeasureFilterModel>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for (MolapMeasureFilter filter : msrFilters) {
            MeasureFilterModel model = new MeasureFilterModel(filter.getFilterValue(),
                    MeasureFilterModel.MeasureFilterType.valueOf(filter.getFilterType().name()));
            model.setDimension(dimensionByLevelName);
            if (!filter.isAfterTopN()) {
                models.add(model);
            } else {
                modelsAfterTopN.add(model);
            }
        }
        if (models.size() > 0) {
            msrFilterModels.put(msr, models.toArray(new MeasureFilterModel[models.size()]));
        }
        if (modelsAfterTopN.size() > 0) {
            msrFilterModelsAfterTopN.put(msr,
                    modelsAfterTopN.toArray(new MeasureFilterModel[modelsAfterTopN.size()]));
        }

    }

    private static MolapFilterInfo getMolapFilterInfo(MolapDimensionLevelFilter dimLevelFilter) {

        MolapFilterInfo filterInfo;
        if (dimLevelFilter.getContainsFilter().size() > 0
                || dimLevelFilter.getDoesNotContainsFilter().size() > 0) {
            filterInfo = new ContentMatchFilterInfo();
        } else {
            filterInfo = new MolapFilterInfo();
        }

        for (Object object : dimLevelFilter.getIncludeFilter()) {
            filterInfo.addIncludedMembers(object.toString());
        }

        for (Object object : dimLevelFilter.getExcludeFilter()) {
            filterInfo.addExcludedMembers(object.toString());
        }

        if (filterInfo instanceof ContentMatchFilterInfo) {
            List<String> contains = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            for (String object : dimLevelFilter.getContainsFilter()) {
                contains.add(object);
            }
            ((ContentMatchFilterInfo) filterInfo).setIncludedContentMatchMembers(contains);

            List<String> notContains =
                    new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            for (String object : dimLevelFilter.getDoesNotContainsFilter()) {
                notContains.add(object);
            }
            ((ContentMatchFilterInfo) filterInfo).setExcludedContentMatchMembers(notContains);
        }

        return filterInfo;
    }

    private static TopNModel getTopNModel(TopCount topCount, Cube cube, List<Measure> msrs,
            List<Dimension> queryDimRows, List<Dimension> queryDimCols, String tableName,
            List<CalculatedMeasure> calcMsrs) {
        MolapDimensionLevel dimensionLevel = topCount.getLevel();
        Dimension dim = cube.getDimensionByLevelName(dimensionLevel.getDimensionName(),
                dimensionLevel.getHierarchyName(), dimensionLevel.getName(), tableName);
        MolapMeasure measure = topCount.getMsr();
        Measure msr = null;
        if (measure.getType().equals(MolapLevelType.CALCULATED_MEASURE)) {
            msr = new CalculatedMeasure(null, measure.getName());
        } else {
            msr = cube.getMeasure(cube.getFactTableName(), measure.getName());
        }
        if (msr == null) {
            return null;
        }
        int msrIndex = -1;

        int i = 0;
        for (Measure m : msrs) {
            if (msr.getName().equals(m.getName())) {
                msrIndex = i;
                break;
            }
            i++;
        }
        if (msrIndex == -1) {
            i = 0;
            for (Measure m : calcMsrs) {
                if (msr.getName().equals(m.getName())) {
                    msrIndex = i + msrs.size();
                    break;
                }
                i++;
            }
        }

        AxisType axisType = null;
        int dimIndex = getTopNDimIndex(queryDimRows, dim);
        if (dimIndex >= 0) {
            axisType = AxisType.ROW;
        } else {
            dimIndex = getTopNDimIndex(queryDimCols, dim);
            if (dimIndex >= 0) {
                axisType = AxisType.COLUMN;
            }
        }

        TopNModel model = new TopNModel(topCount.getCount(),
                TopNModel.MolapTopNType.valueOf(topCount.getType().name()), dim, msr);
        model.setDimIndex(dimIndex);
        model.setMsrIndex(msrIndex);
        model.setAxisType(axisType);
        return model;
    }

    /**
     * getTopNDimIndex
     *
     * @param queryDimRows
     * @param dim
     * @param dimIndex
     * @return
     */
    private static int getTopNDimIndex(List<Dimension> queryDimRows, Dimension dim) {
        int dimIndex = -1;
        int i = 0;
        for (Dimension d : queryDimRows) {
            if (dim.getDimName().equals(d.getDimName()) && dim.getHierName().equals(d.getHierName())
                    && dim.getName().equals(d.getName())) {
                dimIndex = i;
                break;
            }
            i++;
        }
        return dimIndex;
    }

    private static void processMeasureFilters(Map<Measure, MeasureFilterModel[]> msrFilters,
            List<Dimension> queryDimRows, List<Dimension> queryDimCols) {
        for (MeasureFilterModel[] measureFilterModels : msrFilters.values()) {
            for (int i = 0; i < measureFilterModels.length; i++) {
                if (measureFilterModels[i] != null
                        && measureFilterModels[i].getDimension() != null) {
                    int dimIndex =
                            getTopNDimIndex(queryDimRows, measureFilterModels[i].getDimension());
                    AxisType axisType = null;
                    if (dimIndex >= 0) {
                        axisType = AxisType.ROW;
                    } else {
                        dimIndex = getTopNDimIndex(queryDimCols,
                                measureFilterModels[i].getDimension());
                        if (dimIndex >= 0) {
                            axisType = AxisType.COLUMN;
                        }
                    }
                    measureFilterModels[i].setAxisType(axisType);
                }
            }
        }

    }

    private static byte[] convertToArray(List<Integer> integers) {
        byte[] vals = new byte[integers.size()];

        for (int i = 0; i < vals.length; i++) {
            vals[i] = integers.get(i).byteValue();
        }
        return vals;
    }

    /**
     * Returns the parent member specified by a token index (identifies level)
     *
     * @param member
     * @param tokenIndex
     * @param includeAll - whether to return the parent tokens or just the indexed token
     * @return String
     */
    public static String getTokensByIndex(String member, int tokenIndex, boolean includeAll) {
        int length = member.length();
        int tokenCount = 0;
        int i = 0;
        int openBracketIndex = 0;
        for (i = 0; i < length; i++) {
            if (member.charAt(i) == ']') {
                // Check for escape sequence
                if (i + 1 < length && member.charAt(i + 1) == ']') {
                    i++;
                    continue;
                }
                if (tokenCount == tokenIndex) {
                    break;
                }

                tokenCount++;

                // Advance to the start of the next token
                while (i + 1 < length) {
                    i++;
                    if (member.charAt(i) == '[') {
                        if (!includeAll) {
                            openBracketIndex = i;
                        }
                        break;
                    }
                }
            }
        }
        /**
         * Modified for DTS2013092604161
         *
         * Getting StringIndexOutOfBoundsException and NullPointerException in
         * UniBIServer.log while creating molap report with empty data
         *
         */
        boolean isProperRange = i + 1 <= length && openBracketIndex < i + 1;
        if (isProperRange) {
            return member.substring(openBracketIndex, i + 1);
        } else {
            return "";
        }

    }

    /**
     * Strips brackets from input token (if exists)
     *
     * @param token
     * @return String
     */
    public static String stripBrackets(String token) {
        if (token.length() == 0) {
            return token;
        }

        boolean hasOpenBracket = false;
        boolean hasCloseBracket = false;
        if (token.charAt(0) == '[') {
            hasOpenBracket = true;
        }

        if (token.charAt(token.length() - 1) == ']') {
            hasCloseBracket = true;
        }

        return token.substring(hasOpenBracket ? 1 : 0,
                hasCloseBracket ? token.length() - 1 : token.length());
    }

    public static String getTokenAtIndex(String member, int tokenIndex) {
        return getTokensByIndex(member, tokenIndex, false);
    }

    /**
     * This method will remove all columns form cache which have been dropped
     * from the cube
     *
     * @param listOfLoadFolders
     * @param columns
     * @param schemaName
     * @param cubeName
     * @param partitionCount
     */
    public static void removeDroppedColumnsFromLevelLRUCache(List<String> listOfLoadFolders,
            List<String> columns, String schemaName, String cubeName, int partitionCount) {
        String schemaNameWithPartition = null;
        String cubeNameWithPartition = null;
        String levelCacheKey = null;
        String columnActualName = null;
        String cubeUniqueName = null;
        MolapLRULevelCache levelCacheInstance = MolapLRULevelCache.getInstance();
        for (String columnName : columns) {
            for (String loadName : listOfLoadFolders) {
                for (int i = 0; i < partitionCount; i++) {
                    schemaNameWithPartition = schemaName + '_' + i;
                    cubeNameWithPartition = cubeName + '_' + i;
                    cubeUniqueName = schemaNameWithPartition + '_' + cubeNameWithPartition;
                    Cube cube = MolapMetadata.getInstance().getCube(cubeUniqueName);
                    if (null != cube) {
                        Dimension dimension = cube.getDimension(columnName);
                        columnActualName = null != dimension ? dimension.getColName() : columnName;
                        levelCacheKey = cubeUniqueName + '_' + loadName + '_' + columnActualName;
                        LevelInfo levelInfo = levelCacheInstance.get(levelCacheKey);
                        if (null != levelInfo) {
                            if (levelInfo.isLoaded()) {
                                InMemoryCubeStore.getInstance()
                                        .unloadLevelFile(cubeUniqueName, levelInfo);
                            }
                            levelCacheInstance.remove(levelCacheKey);
                        }
                    }
                }
            }
        }
    }
}
