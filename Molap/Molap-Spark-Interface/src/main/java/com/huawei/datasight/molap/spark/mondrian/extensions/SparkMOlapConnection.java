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

package com.huawei.datasight.molap.spark.mondrian.extensions;


/**
 * @author K00900207
 */
public class SparkMOlapConnection //extends MolapConnection
{

//    private static Map<String, SparkQueryExecutor> executorsMap = new HashMap<String, SparkQueryExecutor>();
//    
//    private SparkQueryExecutor exec = null;
//    
//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(SparkMOlapConnection.class.getName());
//
//    private int molapResultLimit = -1;
//    
//    private void initializeExecutor(String schemaPath)
//    {
//        MondrianDef.Schema xmlSchema = MolapSchemaParser.loadXML(schemaPath);
//        try
//        {
//            String sparkURL = MolapProperties.getInstance().getProperty(MolapCommonConstants.SPARK_URL,"local");
//            String sparkHome = MolapProperties.getInstance().getProperty(MolapCommonConstants.SPARK_HOME,"G:/spark-1.0.0-rc3");
//            String molapStorePath = MolapProperties.getInstance().getProperty(MolapCommonConstants.SPARK_STORE_LOCATION,"hdfs://master:54310/sparkmolapstore");
//            String molapSparkResultLimit = MolapProperties.getInstance().getProperty("molap.spark.resultlimit","-1");
//            
//            MondrianDef.Schema sparkSchema = xmlSchema;
//            molapResultLimit = Integer.parseInt(molapSparkResultLimit);
//            SparkQueryExecutor exec = new SparkQueryExecutor(sparkURL,sparkHome,sparkSchema,molapStorePath);
//            exec.init();
//            executorsMap.put(schemaPath, exec);
//        }
//        catch(Throwable e)
//        {
//            System.out.println("########## Error while making SparkQueryExecutor " + e.getMessage());
//           e.printStackTrace();
//        }
//    }
//    
//  //Util.readEncryptedVirtualFile(connectInfo.get("catalog"))
//    
//    
//    public SparkMOlapConnection(MondrianServer server, PropertyList connectInfo, DataSource dataSource)
//    {
//        super(server, connectInfo, dataSource);
//        if(executorsMap.get(connectInfo.get("catalog")) == null)
//        {
//            initializeExecutor(connectInfo.get("catalog"));
//        }
//        
//        exec = executorsMap.get(connectInfo.get("catalog"));
//    }
//
//    public SparkMOlapConnection(MondrianServer server, Util.PropertyList connectInfo, RolapSchema schema,
//            DataSource dataSource)
//    {
//        super(server, connectInfo, schema, dataSource);
//        if(executorsMap.get(connectInfo.get("catalog")) == null)
//        {
//            initializeExecutor(connectInfo.get("catalog"));
//        }
//        
//        exec = executorsMap.get(connectInfo.get("catalog"));
//    }
//
//    protected DataSource createMolapDatasource(Util.PropertyList connectInfo)
//    {
//        return new MolapDataSourceImpl(connectInfo);
//
//    }
//
//    /**
//     * Executes a statement.
//     * 
//     * @param execution
//     *            Execution context (includes statement, query)
//     * 
//     * @throws ResourceLimitExceededException
//     *             if some resource limit specified in the property file was exceeded
//     * @throws QueryCanceledException
//     *             if query was canceled during execution
//     * @throws QueryTimeoutException
//     *             if query exceeded timeout specified in the property file
//     */
//    public Result execute(final Execution execution)
//    {
//        String cubeName = execution.getMondrianStatement().getQuery().getCube().getName();
//        long queryId = execution.getMondrianStatement().getQuery().getId();
//        String schemaName = execution.getMondrianStatement().getQuery().getCube().getSchema().getName();
//        queryStart(schemaName, cubeName, queryId);
//        try
//        {
//            return RolapResultShepherd.shepherdExecution(execution,
//                    getcallableExecution(execution, schemaName, cubeName, queryId));
//        }
//        finally
//        {
//            queryEnd(schemaName, cubeName, queryId);
//        }
//    }
//
//    public void setRole(Role role)
//    {
//        assert role != null;
//
//        this.role = role;
//        this.schemaReader = new SparkSchemaReader(role, schema);
//    }
//
//    
//    public MolapResult execute(MolapQueryImpl molapQuery, String cubeName, List<Dataset> datasetList)
//    {
//        
//        mondrian.olap.Cube[] cubes = getSchemaReader().withLocus().getCubes();
//        mondrian.olap.Cube queryCube = null;
//        
//        for (mondrian.olap.Cube cube : cubes) {
//            String aCubeName = unquoteCubeName(cube.getUniqueName());
//            if ((null != aCubeName) && (aCubeName.equals(cubeName)))
//                queryCube = cube;
//        }
//        queryStart(queryCube.getSchema().getName(), cubeName, 1);
//        
//        MolapResult output =  execute(molapQuery, queryCube, datasetList);
//        queryEnd(queryCube.getSchema().getName(), cubeName, 1);
//        
//        return output;
//    }
//    
//    protected Result executeInternal(final Execution execution)
//    {
//        Query query = execution.getMondrianStatement().getQuery();
//
//        mondrian.olap.Cube cube = query.getCube();
//        MolapQueryImpl molapQuery = (MolapQueryImpl)query.getAttribute("MOLAP_QUERY");
//        
//        return execute(molapQuery, cube, null);
//    }
//    
//    public MolapResult execute(MolapQueryImpl molapQuery,  mondrian.olap.Cube cube, List<Dataset> datasetList)
//    {
//        Map<String, Level> dynamicLevels = new HashMap<String, Level>();
//        
//        //Required output related variables declaration STARTS here
//        Map<Hierarchy, Integer> hierarchyToLevelDepthMap = new HashMap<Hierarchy, Integer>();
//        List<Hierarchy> rowHierarchies = new ArrayList<Hierarchy>();
//        List<Hierarchy> colHierarchies = new ArrayList<Hierarchy>();
//        Map<Level, Integer> levelToColumnsIndexMap = new HashMap<Level, Integer>();
//        
//        List<MolapLevel> selectedRowLevels = new ArrayList<MolapLevel>();
//        List<MolapLevel> selectedColLevels = new ArrayList<MolapLevel>();
//        List<MolapLevel> selectedMeasures = new ArrayList<MolapLevel>();
//        //Required output related variables declaration ENDS here
//        
//        StringBuffer selectClause = new StringBuffer();
//        StringBuffer whereClause = new StringBuffer();
//        StringBuffer orderBYClause = new StringBuffer();
//        
//        processSelectedLevels(molapQuery, selectedRowLevels, selectedColLevels, selectedMeasures, orderBYClause);
//        
//        fillWhereColumns(molapQuery,whereClause);
//        
//        //Make select clause using the selected row Levels
//        int currentRowIndex = makeSelectClauseForLevels(cube, hierarchyToLevelDepthMap, rowHierarchies, levelToColumnsIndexMap,
//                selectedRowLevels, selectClause,0, dynamicLevels);
//        
//        //Make select clause using the selected column Levels
//        int currentColIndex = makeSelectClauseForLevels(cube, hierarchyToLevelDepthMap, colHierarchies, levelToColumnsIndexMap,
//                selectedColLevels, selectClause,0, dynamicLevels);
//        
//        // Add the required non selected levels to the query 
//        for(Map.Entry<Hierarchy, Integer> entry: hierarchyToLevelDepthMap.entrySet())
//        {
//            Hierarchy currentHierarchy = entry.getKey();
//            Integer lowestDepth = entry.getValue();
//            
//            Level[] levels = currentHierarchy.getLevels();
//            for(int i=0; i<=lowestDepth; i++)
//            {
//                String subName = ((RolapHierarchy)currentHierarchy).getSubName();
//                subName =  subName==null? currentHierarchy.getName(): subName;
//                
//                boolean presentInRow = isLevelPresentInSelection(selectedRowLevels, levels[i].getName(), subName);
//                boolean presentInColumn = isLevelPresentInSelection(selectedColLevels, levels[i].getName(), subName);
//                
//                if(!levels[i].isAll() && !presentInRow && !presentInColumn)
//                {
//                    MolapDimensionLevel temp = new MolapDimensionLevel(currentHierarchy.getDimension().getName(), subName, levels[i].getName());
//                    addLevelToSelectClause(selectClause, temp.getName());
//                    
//                    
//                    //Also fill the selected levels list with newly added one
//                    List<MolapLevel> currentList = identifyListForHierarchy(selectedRowLevels, selectedColLevels, subName);
//                    currentList.add(temp);
//                    if(currentList == selectedRowLevels)
//                    {
//                        // Update MOLAPQuery with the current level. TODO move this correction to ReportManagerImpl itself
//                        molapQuery.addDimensionLevel(temp, null, SortType.NONE, AxisType.ROW);
//                        fillColumnIndex(levels[i], levelToColumnsIndexMap, currentRowIndex++);
//                    }
//                    else
//                    {
//                        // Update MOLAPQuery with the current level. TODO move this correction to ReportManagerImpl itself 
//                        molapQuery.addDimensionLevel(temp, null, SortType.NONE, AxisType.COLUMN);
//                        fillColumnIndex(levels[i], levelToColumnsIndexMap, currentColIndex++);
//                    }
//                }
//            }
//        }
//        
//        //Finally add the measures to query
//        for( MolapLevel measure :  selectedMeasures)
//        {
//            addLevelToSelectClause(selectClause, measure.getName());
//            
//            //Also fill the selected levels list with newly added one (Measures are present in molap column axis always)
//            selectedColLevels.add(measure);
//        }
//        
//        if(selectedMeasures.size() >0) 
//        {
//            Hierarchy msrHierarchy = MolapResult.getHierarchy(cube, "Measures");
//            Level msrLevel = msrHierarchy.getLevels()[0];
//            
//            colHierarchies.add(msrHierarchy);
//            
//            fillColumnIndex(msrLevel, levelToColumnsIndexMap, currentColIndex++);
//            fillLevelDepth(msrHierarchy, msrLevel, hierarchyToLevelDepthMap);
//        }
//        
//        
//        selectClause.append(" FROM  " + cube.getName());
//        
//        //Add the where clause 
//        if(whereClause.length()>0)
//        {
//            selectClause.append(" WHERE ");
//            selectClause.append(whereClause.toString());
//        }
//        
//        // Add the ORDER BY clause 
//        if(orderBYClause.length()>0)
//        {
//            selectClause.append(" ORDER BY ");
//            selectClause.append(orderBYClause.toString());
//        }
//        
//        String sqlString =  selectClause.insert(0, "SELECT ").toString();
//        
//        //Fill the result mapping with all required(constructed) fields
//        ResultMappinDTO resultMappinDTO = new ResultMappinDTO();
//        resultMappinDTO.hierarchyToLevelDepthMap = hierarchyToLevelDepthMap;
//        resultMappinDTO.rowHierarchies = rowHierarchies;
//        resultMappinDTO.colHierarchies = colHierarchies;
//        resultMappinDTO.levelToSQLColumnsMap = levelToColumnsIndexMap;
//        resultMappinDTO.selectedRowLevels = selectedRowLevels;
//        resultMappinDTO.selectedColLevels = selectedColLevels;
//        resultMappinDTO.selectedMeasures = selectedMeasures;
//        resultMappinDTO.sqlQuery = sqlString;
//        resultMappinDTO.dynamicLevels = dynamicLevels;
//        
//        // Make a connection to Spark engine and get result here
//        return executeSQLOverSpark(cube, molapQuery, resultMappinDTO, datasetList);
//    }
//
//    
//    private void fillLevelDepth(Hierarchy hierarchy, Level level, Map<Hierarchy, Integer> hierarchyToLevelDepthMap)
//    {
//        Integer depth = hierarchyToLevelDepthMap.get(hierarchy);
//        if(depth == null)
//        {
//            depth = level.getDepth();
//        }
//        else
//        {
//            depth = level.getDepth() > depth ? level.getDepth() : depth;
//        }
//        
//        hierarchyToLevelDepthMap.put(hierarchy, depth);
//    }
//    
//    private void fillColumnIndex(Level levelName, Map<Level, Integer> dimensionToColumns, int currentPosition)
//    {
//        dimensionToColumns.put(levelName, currentPosition);
//    }
//    
//    private boolean isLevelPresentInSelection(List<MolapLevel> selectedLevels, String levelName, String hierarchyName)
//    {
//        for(MolapLevel molapLevel : selectedLevels)
//        {
//            if(levelName.equals(molapLevel.getName()) && hierarchyName.equals(molapLevel.getHierarchyName()))
//            {
//                return true;
//            }
//        }
//        
//        return false;
//    }
//    
//    private List<MolapLevel> identifyListForHierarchy(List<MolapLevel> selectedRowLevels, List<MolapLevel> selectedColLevels, String hierarchyName)
//    {
//        for(MolapLevel molapLevel : selectedRowLevels)
//        {
//            if(hierarchyName.equals(molapLevel.getHierarchyName()))
//            {
//                return selectedRowLevels;
//            }
//        }
//        
//        for(MolapLevel molapLevel : selectedColLevels)
//        {
//            if(hierarchyName.equals(molapLevel.getHierarchyName()))
//            {
//                return selectedColLevels;
//            }
//        }
//        
//        return null;
//    }
//    
//    private void addLevelToSelectClause(StringBuffer selectClause, String levelName)
//    {
//        if(selectClause.length()>0)
//        {
//            selectClause.append(", ");
//        }
//        
//        selectClause.append(levelName);
//    }
//    
//    private int makeSelectClauseForLevels(mondrian.olap.Cube cube, Map<Hierarchy, Integer> hierarchyToLevelDepthMap,
//            List<Hierarchy> hierarchyList, Map<Level, Integer> levelToColumns, List<MolapLevel> selectedLevels,
//            StringBuffer selectClause, int currentIndex, Map<String, Level> dynamicLevels)
//    {
//        for(MolapLevel molapLevel : selectedLevels)
//        {
//            if(molapLevel.getType() == MolapLevelType.DYNAMIC_DIMENSION)
//            {
//                Level level = SchemaUtil.createDynamicLevel((RolapSchema)cube.getSchema(), (RolapCube)cube, molapLevel.getName(), molapLevel.getHierarchyName(), molapLevel.getDimensionName());
//                Hierarchy hierarchy = level.getHierarchy();
//                
//                if(!hierarchyList.contains(hierarchy))
//                {
//                    hierarchyList.add(hierarchy);
//                }
//                fillLevelDepth(hierarchy, level, hierarchyToLevelDepthMap);
//
//                addLevelToSelectClause(selectClause, molapLevel.getName());
//                fillColumnIndex(level, levelToColumns, currentIndex++);
//                dynamicLevels.put(molapLevel.getName(), level);
//            }
//            else
//            {
//                Hierarchy hierarchy = MolapResult.getHierarchy(cube, molapLevel.getHierarchyName());
//                Level[] levels = hierarchy.getLevels();
//                for(Level level : levels)
//                {
//                    if(molapLevel.getName().equals(level.getName()))
//                    {
//                        if(!hierarchyList.contains(hierarchy))
//                        {
//                            hierarchyList.add(hierarchy);
//                        }
//                        fillLevelDepth(hierarchy, level, hierarchyToLevelDepthMap);
//                        
//                        addLevelToSelectClause(selectClause, molapLevel.getName());
//                        fillColumnIndex(level, levelToColumns, currentIndex++);
//                        
//                        break;
//                    }
//                }
//            }
//        }
//        return currentIndex;
//    }
//    
//   
//    
//
//    private void processSelectedLevels(MolapQueryImpl molapQuery, List<MolapLevel> selectedRowLevels,
//            List<MolapLevel> selectedColLevels, List<MolapLevel> selectedMeasures, StringBuffer orderBYClause)
//    {
//        boolean orderBYComma = false;
//        
//        Axis[] axises = molapQuery.getAxises();
//        for(int index = 0;index < 2;index++)
//        {
//            Axis axis = axises[index];
//            List<MolapLevel> currentList = index == AxisType.ROW.getIndex() ? selectedRowLevels : selectedColLevels;
//            if(axis.getDims().size() > 0)
//            {
//                for(int i = 0;i < axis.getDims().size();i++)
//                {
//                    MolapLevelHolder levelHolder = axis.getDims().get(i);   
//                    MolapLevel level = levelHolder.getLevel();
//                    
//                    SortType sortType = levelHolder.getSortType();
//                    
//                    if(SortType.NONE != sortType)
//                    {
//                        if(orderBYComma)
//                        {
//                            orderBYClause.append(" , ");
//                        }
//                        
//                        orderBYClause.append(level.getName() + " " + sortType);
//                        
//                        orderBYComma = true;
//                    }
//                    
//                    
//                    if(level.getType() == MolapLevelType.MEASURE || level.getType() == MolapLevelType.CALCULATED_MEASURE)
//                    {
//                        selectedMeasures.add(level);
//                    }
//                    else
//                    {
//                        currentList.add(level);
//                    }
//                }
//            }
//        }
//    }
//    
//    /**
//     * 1) Range level filter to be supported
//     * 2) Quotes when column names has spaces
//     * 3) Top N
//     * 4) Ranges as per user segmentation 
//     * 5) Order by  
//     * @param molapQuery 
//     *
//     *  Returns the rowDimensionsCount;
//     */
//    public void fillWhereColumns(MolapQueryImpl molapQuery, StringBuffer whereClause)
//    {
//        // Start filling WHERE clause from all the axes (row, column & slice)
//        Axis[] axises = molapQuery.getAxises();
//        boolean andRequired = false;
//        for(int a = 0;a < 3;a++)
//        {
//            Axis axis = axises[a];
//            
//            for(int i = 0;i < axis.getDims().size();i++)
//            {
//                MolapLevelHolder levelHolder = axis.getDims().get(i);
//                
//                if(levelHolder.getMsrFilters() != null)
//                {
//                    for(MolapMeasureFilter msrFilter : levelHolder.getMsrFilters())
//                    {
//                        if(andRequired)
//                        {
//                            whereClause.append(" AND ");
//                        }
//                        whereClause.append(msrFilter.toSQLConstruct(levelHolder.getLevel().getName()));
//                        andRequired = true;
//                    }
//                }
//                
//                
//                if(levelHolder.getDimLevelFilter() != null)
//                {
//                    if(andRequired)
//                    {
//                        whereClause.append(" AND ");
//                    }
//                    whereClause
//                    .append(levelHolder.getDimLevelFilter().toSQLConstruct(levelHolder.getLevel().getName()));
//                    andRequired = true;
//                }
//            }
//        }
//    }
//    
//    private MolapResult executeSQLOverSpark(mondrian.olap.Cube cube, MolapQueryImpl molapQuery, ResultMappinDTO resultMappinDTO, List<Dataset> datasetList)
//    {
//        System.out.println(resultMappinDTO.sqlQuery);
//        molapQuery.getExtraProperties().put("DATA_SETS", datasetList);
////        MolapResultHolder  resultHolder = exec.execute(resultMappinDTO.sqlQuery, molapQuery, datasetList);
//        
//        long t1 = System.currentTimeMillis();
//        MolapResultHolder  resultHolder = exec.execute(molapQuery);
//        
//        
//        if(molapResultLimit >0)
//        {
//            int rowCounter=0;
//            //Just iterate and find the actual result size
//            while(resultHolder.isNext())
//            {
//                rowCounter++;
//            }
//            resultHolder.reset();
//            
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkMOlapConnection result limit applied to : " + molapResultLimit + "Original result size is : "+ rowCounter);
//            
//            resultHolder.setRowLimit(molapResultLimit);
//        }
//        
//        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "SparkQueryExecutor time taken (ms) : " + (System.currentTimeMillis()-t1));
//        
//        MolapResultStreamImpl streamImpl = makeMolapResultStream(cube.getSchema().getName(), cube.getName(), resultHolder, resultMappinDTO);
//        MolapResultStreamHolder holder = new MolapResultStreamHolder();
//        holder.setResultStream(streamImpl);
//        
//        MolapResult molapResult = new MolapResult(holder, molapQuery, resultMappinDTO, resultMappinDTO.selectedRowLevels.size()>0 ? 2 :1);
//        return molapResult;
//    }
//    
//    private MolapResultStreamImpl makeMolapResultStream(String schemaName, String cubeName, MolapResultHolder  resultHolder, ResultMappinDTO resultMappinDTO)
//    {
//        List<Dimension> rowDimensionList = new ArrayList<Dimension>();
//        for(MolapLevel molapLevel : resultMappinDTO.selectedRowLevels)
//        {
//            Dimension dim = new Dimension(molapLevel.getName(), 0, molapLevel.getName(),null);
//            rowDimensionList.add(dim);
//        }
//        
//        List<Dimension> colDimensionList = new ArrayList<Dimension>();
//        for( MolapLevel molapLevel: resultMappinDTO.selectedColLevels)
//        {
//            if(molapLevel.getType() == MolapLevelType.DIMENSION)
//            {
//                Dimension dim = new Dimension(molapLevel.getName(), 0, molapLevel.getName(),null);
//                colDimensionList.add(dim);
//            }
//        }
//        
//        List<Measure> measuresList = new ArrayList<Measure>();
//        for( MolapLevel molapLevel: resultMappinDTO.selectedMeasures)
//        {
//            Measure msr = new Measure(molapLevel.getName(), 0, ((MolapMeasure)molapLevel).getName(),null,
//                    molapLevel.getName(), SqlStatement.Type.DOUBLE,null, false);
//            measuresList.add(msr);
//        }
//        
//        return MolapResultConverter.convertUsingNameMapping(resultHolder, rowDimensionList, colDimensionList, measuresList, false);
//    }
//    
//    
//    public static void main(String[] args)
//    {
//        SchemaUtil.createDynamicLevel(null, null, "1", "1", "1");
//    }
//    
//   
//    
//    /**
//     * Extending the basic RolapSchemaReader to support required methods used by Analyser
//     * 
//     * @author K00900207
//     *
//     */
//    private class SparkSchemaReader extends RolapSchemaReader
//    {
//        
//        public mondrian.olap.Cube[] getCubes() 
//        {
//            mondrian.olap.Cube[] cubes = super.getCubes();
//            
//            return cubes;
//        }
//        
//        SparkSchemaReader(Role role, RolapSchema schema)
//        {
//            super(role, schema);
//        }
//
//        /**
//         * Make a MOLAP query with only this level on row axis and get result from back end.
//         *  Parse OLAP result for the given level and return members  list.    
//         */
//        public List<Member> getLevelMembers(Level level, boolean includeCalculated)
//        {
//            if(level.getDimension().isMeasures())
//            {
//                //Measure members can be delegated to super as they can be made from schema reader itself
//                return super.getLevelMembers(level, includeCalculated);
//            }
//                    
////            String cubeName = schema.getName();
//            RolapCube cube = null;
//            if(level instanceof RolapCubeLevel)
//            {
//                RolapCubeLevel cubeLevel = (RolapCubeLevel)level;
//                cube = cubeLevel.getDimension().getCube();
////                cubeName = cube.getName();
//            }
//            
//            MolapQueryImpl molapQueryImpl  = createMolapQueryForLevel(level);
//            
//            MolapResult molapResult = execute(molapQueryImpl, cube,  null);
//            
//            return molapResult.getAxes()[AxisType.ROW.getIndex()].getPositions().get(level.getDepth());
//        }
//        
//        /**
//         * Make a MOLAP query with only this level on row axis  
//         */
//        private MolapQueryImpl createMolapQueryForLevel(Level level)
//        {
//            MolapQueryImpl molapQuery = new MolapQueryImpl();
//           
//            MolapDimensionLevel level1 = new MolapDimensionLevel(level.getDimension().getName(), level.getHierarchy().getName(), level.getName());
//            
//            molapQuery.addDimensionLevel(level1, null, SortType.NONE, AxisType.ROW);
//            
//            return molapQuery;
//        }
//    }
//
//    
//    public static String unquoteCubeName(String cubeName)
//    {
//        if (cubeName == null)
//            return null;
//        
//        
//        int beginIndex = 0;
//        int endIndex = cubeName.length() - 1;
//        
//        if (cubeName.charAt(beginIndex) == '[') {
//            beginIndex += 1;
//        }
//        
//        if (cubeName.charAt(endIndex) == ']') {
//            endIndex -= 1;
//        }
//        
//        return cubeName.substring(beginIndex, endIndex + 1);
//    }
    
}
