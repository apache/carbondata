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
package com.huawei.unibi.molap.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.eigenbase.xom.NodeDef;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.olap.LevelType;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.AggMeasure;
import com.huawei.unibi.molap.olap.MolapDef.AggName;
import com.huawei.unibi.molap.olap.MolapDef.AggTable;
import com.huawei.unibi.molap.olap.MolapDef.Annotation;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Level;
import com.huawei.unibi.molap.olap.MolapDef.Property;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.olap.SqlStatement;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;


/**
 * It has all meta related to the cube. like dimensions and fact table and aggregate tables etc.
 * @author R00900208
 * 
 */
@SuppressWarnings("deprecation")
public final class MolapMetadata
{
    
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapMetadata.class.getName());
    
    /**
     *  MolapMetadata metadata.
     */
    private static MolapMetadata metadata = null;

    /**
     * List<Cube> metaData variable.
     */
    private List<Cube> metaData = new CopyOnWriteArrayList<MolapMetadata.Cube>();
    
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    private final Lock readLock = lock.readLock();
    
    private final Lock writeLock = lock.writeLock();

    public String parent;

    /**
     * create the instance of MolapMetadata.
     * @return MolapMetadata.
     */
    public static synchronized MolapMetadata getInstance()
    {
        if(metadata == null)
        {
            metadata = new MolapMetadata();
        }

        return metadata;
    }

    /**
     * Method gets the cube instnace.
     * @param name
     * @return Cube
     */
    public Cube getCube(String cubeUniqueName)
    {
        try
        {
            readLock.lock();
            for(Cube cube : metaData)
            {
                if(cubeUniqueName.equalsIgnoreCase(cube.cubeName))
                {
                    return cube;
                }
            }
        }
        finally
        {
            readLock.unlock();
        }
        return null;
    }
    
    /**
     * Method gets the cubes list starts with schema name
     * @param name
     * @return Cube
     */
    public List<Cube> getCubesStartWith(String schemaName)
    {
        List<Cube> cubes = new ArrayList<Cube>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        try
        {
            readLock.lock();
            for(Cube cube : metaData)
            {
                if(cube.cubeName.startsWith(schemaName))
                {
                    cubes.add(cube);
                }
            }
        }
        finally
        {
            readLock.unlock();
        }
        return cubes;
    }

    /**
     * Method removes the Cube from metadata.
     * 
     * @param name
     * 
     */
    public void removeCube(String cubeUniqueName)
    {
        try
        {
            writeLock.lock();
            removeCubeFromMetadata(cubeUniqueName);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void removeCubeFromMetadata(String cubeUniqueName)
    {
        Cube cubeToFind = null;
        for(Cube cube : metaData)
        {
            if(cubeUniqueName.equals(cube.cubeName))
            {
                cubeToFind = cube;
                break;
            }
        }
        if(cubeToFind != null)
        {
            metaData.remove(cubeToFind);
        }
    }
    
    /**
     * Method removes all the Cubes from metadata.
     */
    public void removeAllCubes()
    {
        try
        {
            writeLock.lock();
            metaData.clear();
        }
        finally
        {
            writeLock.unlock();
        }
    }

//    /**
//     * load the cube metadata.
//     * @param cubeMap
//     */
//    public void load(Map<String, MolapDef.Cube> cubeMap)
//    {
//        for(MolapDef.Cube cube : cubeMap.values())
//        {
//            String cubeUniqueName = cube.getSchema().getName()+'_'+cube.getName();
//            if(getCube(cubeUniqueName) == null)
//            {
//                loadCube(cube);
//            }
//        }
//    }

    public Cube getCubeWithCubeName(String cubeName, String schemaName)
    {
        try
        {
            readLock.lock();
            for(Cube cube : metaData)
            {
                if(cube.onlyCubeName.equalsIgnoreCase(cubeName) && cube.schemaName.equalsIgnoreCase(schemaName))
                {
                    return cube;
                }
            }
        }
        finally
        {
            readLock.unlock();
        }
        return null;
    }

    
    /**
     * load the cube metadata.
     * @param cube
     */
    public void loadCube(MolapDef.Schema schema,String orginalSchemaName, String orginalCubeName,MolapDef.Cube cube)
    {
        Cube locCube = new Cube(schema, cube, orginalSchemaName, orginalCubeName);
        locCube.setOnlyCubeName(cube.name);
        String table = MolapSchemaReader.getFactTableName(cube);
        locCube.setFactTableName(table);
        locCube.setMode(cube.mode);
        locCube.setAutoAggregateType("NONE");

        // Process all the dimensions and fact table
        Map<String, String> levelsToColMap = loadMetaToCube(table, locCube, schema);

        List<Measure> measures = locCube.getMeasures(table);
        prepareComplexDimensions(locCube.getDimensions(table));
        HashMap<String, Measure> measuresMap = new HashMap<String, MolapMetadata.Measure>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        for(Measure measure : measures)
        {
            measuresMap.put("[Measures].[" + measure.getName() + ']', measure);
        }

        MolapDef.Cube xmlCube = cube;

        locCube.setAutoAggregateType(xmlCube.autoAggregationType);

        // if(true)
        // //MondrianProperties.instance().UseAggregates.booleanValue()
        // {
        // Process aggregate tables from XML schema.
        // Because, Aggregate tables are not loaded yet to RolapSchema
        // TODO I am assuming here that I have only 1 cube in the schema
        MolapDef.AggTable[] aggtables = ((MolapDef.Table)xmlCube.fact).aggTables;

        for(AggTable aggTable : aggtables)
        {

            processAggregateTable(aggTable, locCube, levelsToColMap, measuresMap);
        }
        // }
        locCube.isFullyDenormalized = checkIfCubeHasOnlyDegeneratedDimensions(locCube);

        updateSurrogateBasedMeasureIfCubeIsNotFullyDenormalized(locCube);
        try
        {
            writeLock.lock();
            removeCubeFromMetadata(schema.name + '_' + cube.name);
            metaData.add(locCube);
        }
        finally
        {
            writeLock.unlock();
        }
    }
    
    /**
     * 
     * @param cube
     * @param aggregateTableName
     * @param columnNames
     */
    public List<MolapDef.AggLevel> getAggLevelsForAggTable(Cube cube,String aggregateTableName,List<String> columnNames)
    {
        String factTableName = cube.factTableName;
        List<Measure> measures = cube.getMeasures(factTableName);
        List<String> measureNames = new ArrayList<String>(15);
        Set<String> metadataColumns = cube.getMetaTableColumns(factTableName);

        List<MolapDef.AggLevel> listOfAggLevel = new ArrayList<MolapDef.AggLevel>(15);
        for(Measure measure : measures)
        {

            measureNames.add(measure.getName());
        }
        for(String columnName : columnNames)
        {
            Dimension factDimension = cube.getDimension(columnName);
            if(null == factDimension)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "factDimension is null for coulmn" + columnName);
                continue;
            }
            if(metadataColumns.contains(columnName) && !measureNames.contains(columnName))
            {
                MolapDef.AggLevel aggLevel = new MolapDef.AggLevel();
                String dimensionName = factDimension.getDimName();
                String hierarchyName = factDimension.getHierName();
                String levelName = '[' + dimensionName + "]." + '[' + hierarchyName + "]." + '[' + columnName + ']';
                aggLevel.name = levelName;
                aggLevel.column = columnName;
                listOfAggLevel.add(aggLevel);
            }
        }
        return listOfAggLevel;

    }
    
    /**
     * load the cube metadata.
     * @param cube
     */
    public void loadSchema(Schema schema)
    {
        com.huawei.unibi.molap.olap.MolapDef.Cube[] cubes = schema.cubes;
        
        for(int i = 0;i < cubes.length;i++)
        {
            loadCube(schema, schema.name, cubes[i].name, cubes[i]);
        }
    }
    
    private void updateSurrogateBasedMeasureIfCubeIsNotFullyDenormalized(Cube locCube)
    {
        List<Measure> listOfMeasures = null;
        if(!locCube.isFullyDenormalized)
        {
            Map<String, List<Measure>> measures = locCube.measures;//CHECKSTYLE:OFF    Approval No:Approval-367
            for(Entry<String, List<Measure>> entry : measures.entrySet())//CHECKSTYLE:ON
            {//CHECKSTYLE:OFF    Approval No:Approval-367
                listOfMeasures = entry.getValue();
                for(Measure m : listOfMeasures)//CHECKSTYLE:ON
                {
                    if(m.getAggName().equals(MolapCommonConstants.DISTINCT_COUNT) && m.isSurrogateGenerated)
                    {
                        m.isSurrogateGenerated=false;
                    }
                }
            }
        }
    }

    private boolean checkIfCubeHasOnlyDegeneratedDimensions(Cube cube)
    {
        Set<String> tablesList = cube.getTablesList();
        
        Iterator<String> iterator = tablesList.iterator();
        List<Dimension> dimensions = null;
        //CHECKSTYLE:OFF
        while(iterator.hasNext())
        {//CHECKSTYLE:ON
            dimensions = cube.getDimensions(iterator.next());
            for(Dimension dim:dimensions)
            {
                if(cube.getHierarchiesMapping(dim.getDimName()+'_'+dim.getHierName()).size()>1)
                {
                    return false;
                }
            }
        }
        return true;
    }
    
    /**
     * processAggregateTable.
     * @param agg
     * @param cube
     * @param levelsToColMap
     * @param measuresMap
     */
    public void processAggregateTable(AggTable agg, Cube cube, Map<String, String> levelsToColMap,
            Map<String, Measure> measuresMap)
    {
        MolapDef.AggName aggTable = (AggName)agg;
        String aggTableName = aggTable.name;
        cube.factCountColumnMapping.put(aggTableName, (aggTable.factcount != null) ? aggTable.factcount.column : null);

        Set<String> metaAggTableCols = cube.getMetaTableColumns(aggTableName);
        int counter = 0;
        if(null != aggTable.levels)
        {
            for(MolapDef.AggLevel aggLevel : aggTable.levels)
            {
                // [Hierarchy].[Level]
                // Find the level column name from levels to column name map
                String columnName = levelsToColMap.get(aggLevel.name);
                
                Dimension factDimension = cube.getDimension(columnName);
                if (null == factDimension)
                {
                    LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "factDimension is null for coulmn" + columnName);
                    continue;
                }
                Dimension aggDimension = factDimension.getDimCopy();
                aggDimension.setAggColumn(aggLevel.column);
                aggDimension.setAggTable(aggTableName);
                aggDimension.setOrdinal(counter++);
                cube.getDimensions(aggTableName).add(aggDimension);
                
                // Add level column for aggregate table meta data.
                metaAggTableCols.add(aggLevel.column);
            }
        }

        for(AggMeasure aggMeasure : aggTable.measures)
        {
            String aggMsrName = aggMeasure.name.trim();
            Measure newMeasure = null;
            Measure actualMeasure = measuresMap.get(aggMsrName);
            if(null == actualMeasure)
            {
                Dimension factDimension = cube.getDimension(aggMeasure.column);
                newMeasure = new Measure();
                
                if(null != factDimension)
                {
                    newMeasure.setColName(factDimension.colName);
                    newMeasure.setName(factDimension.name);
                }
                newMeasure.setAggName(aggMeasure.aggregator);
                newMeasure.setDataType(MolapMetadata.makeSQLDataTye("Numeric"));
            }
            else
            {
                newMeasure = actualMeasure.getCopy();
                newMeasure.setColName(aggMeasure.column);
            }
            newMeasure.setOrdinal(cube.getMeasures(aggTableName).size());
            cube.getMeasures(aggTableName).add(newMeasure);

            // Add measure column for aggregate table meta data.
            metaAggTableCols.add(aggMeasure.column);
        }

        // Add fact count column for aggregate table meta data.
        if(null!=aggTable.factcount)
        {
        metaAggTableCols.add(aggTable.factcount.column);
        }
    }
    
    
    private void prepareComplexDimensions(List<Dimension> currentDimTables)
    {
        Map<String, ArrayList<Dimension>> complexDimensions = new HashMap<String, ArrayList<Dimension>>();
        for(int i = 0;i < currentDimTables.size();i++)
        {
            ArrayList<Dimension> dimensions = complexDimensions.get(currentDimTables.get(i).getHierName());
            if(dimensions != null)
            {
                dimensions.add(currentDimTables.get(i));
            }
            else
            {
                dimensions = new ArrayList<Dimension>();
                dimensions.add(currentDimTables.get(i));
            }
            complexDimensions.put(currentDimTables.get(i).getHierName(), dimensions);
        }
        
        for (Map.Entry<String, ArrayList<Dimension>> entry : complexDimensions.entrySet())
        {
            int[] blockIndexsForEachComplexType = new int[entry.getValue().size()];
            for(int i=0;i<entry.getValue().size();i++)
            {
                blockIndexsForEachComplexType[i] = entry.getValue().get(i).getDataBlockIndex();
            }
            entry.getValue().get(0).setAllApplicableDataBlockIndexs(blockIndexsForEachComplexType);
        }
    }
    
    /**
     * Process all the dimensions and fact table.
     * @param table
     * @param cube
     * @param schema
     * @return Map<String, String>.
     */
    private Map<String, String> loadMetaToCube(String table, Cube cube,  MolapDef.Schema schema)
    {
        ArrayList<MondrianLevelHolder> levelList = new ArrayList<MondrianLevelHolder>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        // var to hold normalized
        ArrayList<LevelNormalizedProps> levelNormalizedList = new ArrayList<LevelNormalizedProps>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        Map<String, String> levelToColumnMap = new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        Set<String> metaFactTableCols = cube.getMetaTableColumns(table);
       
        Map<String,Integer> msrCols = new LinkedHashMap<String, Integer>();
        // TODO I am assuming here that I have only 1 cube in my schema
        com.huawei.unibi.molap.olap.MolapDef.Cube mondrianCube = MolapSchemaReader.getMondrianCube(schema, cube.getCubeName());
        if(null == mondrianCube)
        {
            return new HashMap<String, String>(0);
        }
        String factTableName = ((MolapDef.Table)mondrianCube.fact).name;
        NodeDef[] nodeDefs = mondrianCube.getChildren();
        List<MolapDef.CalculatedMember> xmlCalcMembers = new ArrayList<MolapDef.CalculatedMember>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(NodeDef nd : nodeDefs)
        {
            updateMeasureAndDimensions(table, cube, schema, levelList, levelNormalizedList, levelToColumnMap,
                    metaFactTableCols, msrCols, factTableName, xmlCalcMembers, nd);
        }
        //Add the calculated measures if any.
//        addCalculatedMeasures(xmlCalcMembers, cube.getMeasures(table), rolapCube);
        
        int inc = -1;
        int keyOrdinal;
        // index for normalizedList
        int indexNormalized = 0;
        int blockIndex = 0;
        List<MolapMetadata.Dimension> normalizedDimList = new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(MondrianLevelHolder levHolder : levelList)
        {
            MolapDef.Level lev = levHolder.rolapLevel;
            LevelType type = LevelType.valueOf(("TimeHalfYear".equals(lev.levelType)) ? "TimeHalfYears" : lev.levelType);
            
            inc = getIncCount(levelNormalizedList, inc, indexNormalized);

            //Removing the keyordinal part as it is not handled completly.
//            if(-1==lev.keyOrdinal)
//            {
//                keyOrdinal = inc;
//            }
//            else
//            {
//                keyOrdinal = lev.keyOrdinal;
//            }
            keyOrdinal = inc<0?0:inc;
            
            Dimension dimension = new Dimension(lev.column, keyOrdinal, lev.name,cube);
            dimension.setLevelType(type);
            //dimension.setNoOfbits((byte)Long.toBinaryString(lev.levelCardinality).length());
//            dimension.setNoOfbits(lev.levelCardinality);
            dimension.setDataType(makeSQLDataTye(lev.type));
            dimension.setParentName(lev.parentname);
            dimension.setOrdinalCol(false);
            dimension.setTableName(levHolder.molapTableName);
            dimension.setHierName(levHolder.molapHierName);
            dimension.setActualTableName(levHolder.actualTableName);
            dimension.setDimName(levHolder.molapDimName);
            dimension.setDataBlockIndex(blockIndex++);
            boolean hasNameColumn = hasNameColumn(lev);
            
            dimension.setNameColumnIndex(getNameColumnIndexInSchemaOrder(levHolder, levelList));
            dimension.setPropertyIndexes(getPropertyColumnIndexes(levHolder, levelList));
            updateDimensionWithHighCardinalityVal(dimension,schema.cubes[0].dimensions);
            
            // Update properties and its data types in dimension
            Property[] properties = lev.properties;
            List<SqlStatement.Type> propertyTypes = new ArrayList<SqlStatement.Type>(MolapCommonConstants.CONSTANT_SIZE_TEN);

            // Add string type for name column
            if(hasNameColumn)
            {
                propertyTypes.add(makeSQLDataTye("String"));
            }
            for(int pCounter = 0;pCounter < lev.properties.length;pCounter++)
            {
                propertyTypes.add(makeSQLDataTye(properties[pCounter].type));
            }
            // Check if normalized
            checkAndHandleNormalizedDim(levelNormalizedList, indexNormalized, normalizedDimList, dimension);
            
            dimension.setPropertyCount(propertyTypes.size());
            dimension.setPropertyTypes(propertyTypes.toArray(new SqlStatement.Type[propertyTypes.size()]));
            dimension.setSchemaOrdinal(inc);
            dimension.setHasNameColumn(hasNameColumn);
            cube.getDimensions(table).add(dimension);
            indexNormalized++;
        }
        // Now put hierarchy and dimension info
        putHeirAndDimInfo(table, cube);
        return levelToColumnMap;
    }

    private void updateDimensionWithHighCardinalityVal(Dimension dimension, CubeDimension[] dimensions)
    {
        for(CubeDimension cubeDimension:dimensions)
        {
            if(dimension.getName().equals(cubeDimension.name) && cubeDimension.highCardinality)
            {
                dimension.setHighCardinalityDims(cubeDimension.highCardinality);
                break;
            }
            
        }
        
    }

    private void updateMeasureAndDimensions(String table, Cube cube, MolapDef.Schema schema,
            ArrayList<MondrianLevelHolder> levelList, ArrayList<LevelNormalizedProps> levelNormalizedList,
            Map<String, String> levelToColumnMap, Set<String> metaFactTableCols, Map<String, Integer> msrCols,
            String factTableName, List<MolapDef.CalculatedMember> xmlCalcMembers, NodeDef nd)
    {
        if(nd == null)
        {
            return;
        }
        // Handle dimensions
        else if(nd instanceof MolapDef.Dimension)
        {
            formDimensions(cube, levelList, levelNormalizedList, levelToColumnMap, metaFactTableCols, nd,factTableName,((MolapDef.Dimension)nd).name);
        }
        else if(nd instanceof MolapDef.DimensionUsage)
        {
            MolapDef.Dimension dimension = null;
            dimension = getDimensionFromDimensionUsage(schema, nd);
            formDimensions(cube, levelList, levelNormalizedList, levelToColumnMap, metaFactTableCols, dimension,factTableName,((MolapDef.DimensionUsage)nd).name);
        }
        // Handle measures
        else if(nd instanceof MolapDef.Measure)
        {
            MolapDef.Measure measure = (MolapDef.Measure)nd;

            Integer ordinal = msrCols.get(measure.column);
            if(ordinal == null)
            {
                ordinal = msrCols.size();
                msrCols.put(measure.column, ordinal);
            }
            boolean isSurrogateGenerated=false;
            
            if(null!=measure.annotations)
            {
                Annotation[] array = measure.annotations.array;
                if(measure.aggregator.equals(MolapCommonConstants.DISTINCT_COUNT))
                {
                    for(int i=0; i< array.length; i++)
                    {
                        if(array[i].name.equals(MolapCommonConstants.MEASURE_SRC_DATA_TYPE))
                        {
                            if("String".equalsIgnoreCase(array[i].cdata))
                            {
                                isSurrogateGenerated=true;
                                break;
                            }
                        }
                    }
                }
            }
            Measure measure2 = new Measure(measure.column, ordinal, measure.aggregator, measure.aggClass,
                    measure.name, MolapMetadata.makeSQLDataTye(measure.datatype),cube,isSurrogateGenerated);
            cube.getMeasures(table).add(measure2);
            metaFactTableCols.add(measure.column);
        }
        else if(nd instanceof MolapDef.CalculatedMember)
        {
            xmlCalcMembers.add((MolapDef.CalculatedMember)nd);
        }
    }

    private void putHeirAndDimInfo(String table, Cube cube)
    {
        // This would be use 
        int i = 0;
        for(Dimension d : cube.getDimensions(table))
        {
            // Form the key as dimName_HierName
            String key = d.getDimName()+ '_' + d.getHierName();
            Dimension tmpD = d.getDimCopy();
            tmpD.setOrdinal(i);
            cube.addToHierarchiesMapping(key, tmpD);
            i++;
        }
        for(Entry<String, List<Dimension>> entry :  cube.hierarchiesMapping.entrySet())
        {
            List<Dimension> dims = entry.getValue();
            int l = 0;
            for(Dimension dim : dims)
            {
                dim.setOrdinal(l++);
            }
        }
    }
    
    
    private int getNameColumnIndexInSchemaOrder(MondrianLevelHolder levHolder, ArrayList<MondrianLevelHolder> levelList)
    { 
        int counter = -1;
        
        MolapDef.Level lev = levHolder.rolapLevel;
        
        for(MondrianLevelHolder level : levelList)
        {
            MolapDef.Level localLevel = level.rolapLevel;
            
            if(levHolder.molapTableName.equals(level.molapTableName) && lev.column.equals(localLevel.column))
            {
                if(levHolder == level)
                {
                    if(null != localLevel.nameColumn)
                    {
                        return ++counter;
                    }
                    break;
                }
                else
                {
                    
                    if(null != localLevel.nameColumn)
                    {
                        counter++;
                    }
                    
                    int length = localLevel.properties.length;
                    counter +=length;
                }
            }
        }
        

        return -1;
    }
    
    private int[] getPropertyColumnIndexes(MondrianLevelHolder levHolder, ArrayList<MondrianLevelHolder> levelList)
    { 
        int[] propIndexes = null;
        int counter = -1;
        
        MolapDef.Level lev = levHolder.rolapLevel;
        
        for(MondrianLevelHolder level : levelList)
        {
            MolapDef.Level localLevel = level.rolapLevel;
            
            if(levHolder.molapTableName.equals(level.molapTableName) && lev.column.equals(localLevel.column))
            {
                if(levHolder == level)
                {
                    if(null != localLevel.nameColumn)
                    {
                        ++counter;
                    }
                    int size = localLevel.properties.length;
                    propIndexes = new int[size];
                    
                    for(int i = 0;i < size;i++)
                    {
                        counter = counter+1;
                        propIndexes[i] = counter;
                    }
                    
                    return propIndexes;
                    
                }
                else
                {
                    
                    if(null != localLevel.nameColumn)
                    {
                        counter++;
                    }
                    
                    int length = localLevel.properties.length;
                    counter +=length;
                }
            }
        }
        
        return propIndexes;
    }

//    private void addCalculatedMeasures(List<MolapDef.CalculatedMember> xmlCalcMembers,List<Measure> measures,RolapCube rolapCube)
//    {
//        Query query = MolapCalcExpUtil.resolveCalcMembers(xmlCalcMembers, rolapCube);
//        Formula[] formulas = query.getFormulas();
//        int i = 0;
//        for(MolapDef.CalculatedMember calculatedMember : xmlCalcMembers)
//        {
//            CalculatedMeasure measure = new CalculatedMeasure(formulas[i].getExpression(), calculatedMember.name);
//            measures.add(measure);
//            i++;
//        }
//    }

    /**
     * @param levelNormalizedList
     * @param inc
     * @param indexNormalized
     * @return
     */
    private int getIncCount(ArrayList<LevelNormalizedProps> levelNormalizedList, int inc, int indexNormalized)
    {
        if(!levelNormalizedList.get(indexNormalized).isLevelNormalized())
        {
            // level not normalized
            inc++;
        }
        else
        {
            // level normalized
            // only increase for last level
            if(indexNormalized == levelNormalizedList.size()-1)
            {
                // last level of list
                inc++;
            }
            
            else if(!(levelNormalizedList.get(indexNormalized).getHierName().equalsIgnoreCase(levelNormalizedList.get(indexNormalized+1).getHierName()) && levelNormalizedList.get(indexNormalized).getDimName().equalsIgnoreCase(
                    levelNormalizedList.get(indexNormalized+1).getDimName())))
            {
                // Change in hierarchy name
                // i.e. last level of normalized hierarchy
                inc++;
            }
        }
        return inc;
    }

    /**
     * @param levelNormalizedList
     * @param indexNormalized
     * @param normalizedDimList
     * @param dimension
     */
    private void checkAndHandleNormalizedDim(ArrayList<LevelNormalizedProps> levelNormalizedList, int indexNormalized,
            List<MolapMetadata.Dimension> normalizedDimList, Dimension dimension)
    {
        LevelNormalizedProps levelNormalizedProps = levelNormalizedList.get(indexNormalized);
        dimension.setHasAll(levelNormalizedProps.isHasAll());
        if(levelNormalizedProps.isLevelNormalized())
        {
            if(!levelNormalizedProps.isDimInFact())
            {
                normalizedDimList.add(dimension);
            }
            else 
            {
                // We have come to last dimension which would be present in fact
                // For all above levels in this hierarchy, set this one
                for(MolapMetadata.Dimension dim : normalizedDimList)
                {
                    dim.setNormalized(true);
                    dim.setDimInFact(dimension);
                }
                //dimension.setNormalized(true);
                dimension.setDimInFact(dimension);
                // clear the list so that next normalized hierarchy can use it.
                normalizedDimList.clear();
            }
        }
    }

    /**
     * Extracts dimension for a dimension usage
     * 
     * @param schema
     * @param nd
     * @param globalDimension
     * @return 
     */
    private com.huawei.unibi.molap.olap.MolapDef.Dimension getDimensionFromDimensionUsage(MolapDef.Schema schema, NodeDef nd)
    {
        com.huawei.unibi.molap.olap.MolapDef.Dimension[] globalDimensions = schema.dimensions;
        for(MolapDef.Dimension globalDimension : globalDimensions)
        {
            if(((MolapDef.DimensionUsage)nd).name.equals(globalDimension.name))
            {
                return formDimension(nd, globalDimension);
            }
        }
        return null;
    }

    /**
     * Creates teh copy of the passed global dimension with name and foregin key substituted
     * @param nd
     * @param globalDimension
     * @return 
     */
    private com.huawei.unibi.molap.olap.MolapDef.Dimension formDimension(NodeDef nd, MolapDef.Dimension globalDimension)
    {
        MolapDef.Dimension copy;
        copy = new MolapDef.Dimension();
        copy.caption = globalDimension.caption;
        copy.annotations=globalDimension.annotations;
        copy.description=globalDimension.description;
        copy.hierarchies=globalDimension.hierarchies;
        copy.highCardinality=globalDimension.highCardinality;
        copy.type=globalDimension.type;
        copy.usagePrefix=globalDimension.usagePrefix;
        copy.visible=globalDimension.visible;
        copy.name=globalDimension.name;
        copy.foreignKey=((MolapDef.DimensionUsage)nd).foreignKey;
        return copy;
        
    }

    private void formDimensions(Cube cube, ArrayList<MondrianLevelHolder> levelList,
            ArrayList<LevelNormalizedProps> levelNormalizedList, Map<String, String> levelToColumnMap, Set<String> metaFactTableCols, NodeDef nd, String factTableName,String dimName)
    {
        /**
         * Fortify Fix: NULL_RETURNS
         */
        if(null == nd)
        {
            return;
        }
        MolapDef.Dimension dim = (MolapDef.Dimension)nd;
        // Add foreignKey key as a column in fact table meta columns
        if(null != dim.foreignKey)
        {
            metaFactTableCols.add(dim.foreignKey);
        }
        NodeDef[] hierarchies = ((MolapDef.Dimension)dim).getChildren();
        for(NodeDef hdef : hierarchies)
        {
            if(hdef instanceof MolapDef.Hierarchy)
            {
                MolapDef.Hierarchy hier = (MolapDef.Hierarchy)hdef;
                Set<String> metaHierTableCols = null;
                String hName = hier.name;
                // Hierarchy name can be empty. So, take it as dimension
                // name
                if(hName == null)
                {
                    hName = dimName;
                }
                NodeDef[] levels = hier.getChildren();
                for(NodeDef ldef : levels)
                {
                    if(ldef instanceof MolapDef.Level)
                    {
                        MolapDef.Level lev = (MolapDef.Level)ldef;
                        MondrianLevelHolder holder = new MondrianLevelHolder();
                        holder.molapDimName = dimName;
                        holder.molapHierName = hName;
                        holder.molapLevelName = lev.name;
                        holder.rolapLevel = lev;
                        String actualTableName=hier.relation == null ?factTableName:hier.relation.toString();
                        String tableName = hier.relation == null ?factTableName:getTableNameFromHierarchy(hier);
                        LevelNormalizedProps levelNormalizedProps = createLevelNormalizedProps(hier, lev,
                                dimName);
                        
                        if(tableName.contains("."))
                        {
                            tableName = tableName.split("\\.")[1];
                        }
                        if(actualTableName.contains("."))
                        {
                            actualTableName = actualTableName.split("\\.")[1];
                        }
                        if(factTableName.equals(tableName))
                        {
                            holder.molapTableName = factTableName;
                        }
                        else
                        {
                            holder.molapTableName = tableName;
                        }
                        
                        if(factTableName.equals(actualTableName))
                        {
                            holder.actualTableName = factTableName;
                        }
                        else
                        {
                            holder.actualTableName = actualTableName;
                        }
                        
                        
                        
                        levelList.add(holder);
                        // Add levelNormalizedProps
                        levelNormalizedList.add(levelNormalizedProps);
                        levelToColumnMap.put('[' + hName + "].[" + lev.name + ']', lev.column);
                        levelToColumnMap.put('['+dimName+"].[" + hName + "].[" + lev.name + ']', lev.column);
                        levelToColumnMap.put('['+dimName+'.' + hName + "].[" + lev.name + ']', lev.column);
                        if(metaHierTableCols != null)
                        {
                            metaHierTableCols.add(lev.column);
                        }
                        if(dim.foreignKey == null  && hier.relation == null)
                        {
                            metaFactTableCols.add(lev.column);
                        }
                    }
                    else if(ldef instanceof MolapDef.Table)
                    {
                        MolapDef.Table hTable = (MolapDef.Table)ldef;
                        metaHierTableCols = cube.getMetaTableColumns(hTable.name);
                    }
                }
            }
        }
    }

    /**
     * @Author s71955
     * @Description : getTableNameFromHierarchy
     * @param hier
     * @return
     */
    private String getTableNameFromHierarchy(MolapDef.Hierarchy hier)
    {
        String tableName = hier.relation.toString();
        if(hier.relation instanceof MolapDef.Table && null!=((MolapDef.Table)hier.relation).alias)
        {
            tableName=((MolapDef.Table)hier.relation).alias;
        }
        else if(hier.relation instanceof MolapDef.InlineTable && null!=((MolapDef.Table)hier.relation).alias)
        {
            tableName=((MolapDef.InlineTable)hier.relation).alias; 
        }
        
        else if(hier.relation instanceof MolapDef.View && null!=((MolapDef.Table)hier.relation).alias)
        {
            tableName=((MolapDef.View)hier.relation).alias; 
        }
        return tableName;
    }
    
    /**
     * Holder object for Mondrian level
     * @author R00900208
     *
     */
    private static class MondrianLevelHolder
    {
        private MolapDef.Level rolapLevel;
        
        private String molapDimName;
        
        private String molapHierName;
        
        private String molapLevelName;
        
        private String molapTableName;
        
        private String actualTableName;
        
        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((molapDimName == null) ? 0 : molapDimName.hashCode());
            result = prime * result + ((molapHierName == null) ? 0 : molapHierName.hashCode());
            result = prime * result + ((rolapLevel == null) ? 0 : rolapLevel.hashCode());
            result = prime * result + ((molapLevelName == null) ? 0 : molapLevelName.hashCode());
            return result;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj)
        {
            if(this == obj)
            {
                return true;
            }
            if(obj == null)
            {
                return false;
            }
            if(obj instanceof MondrianLevelHolder)
            {
                return false;
            }
            MondrianLevelHolder other = (MondrianLevelHolder)obj;
            if(molapDimName == null)
            {
                if(other.molapDimName != null)
                {
                    return false;
                }
            }
            else if(!molapDimName.equals(other.molapDimName))
            {
                return false;
            }
            if(molapHierName == null)
            {
                if(other.molapHierName != null)
                {
                    return false;
                }
            }
            else if(!molapHierName.equals(other.molapHierName))
            {
                return false;
            }
            if(rolapLevel == null)
            {
                if(other.rolapLevel != null)
                {
                    return false;
                }
            }
            else if(!rolapLevel.equals(other.rolapLevel))
            {
                return false;
            }
            if(molapLevelName == null)
            {
                if(other.molapLevelName != null)
                {
                    return false;
                }
            }
            else if(!molapLevelName.equals(other.molapLevelName))
            {
                return false;
            }
            return true;
        }
    }

    /**
     * Create levelNormalizedProps
     * @param hier
     * @param lev
     * @param dimName 
     * @return
     */
    private LevelNormalizedProps createLevelNormalizedProps(MolapDef.Hierarchy hier, MolapDef.Level lev, String dimName)
    {
        LevelNormalizedProps levelNormalizedProps = new LevelNormalizedProps();
        levelNormalizedProps.setLevelNormalized(hier.normalized);
        levelNormalizedProps.setHierName(hier.name);
        levelNormalizedProps.setDimName(dimName);
        levelNormalizedProps.setHasAll(true);
        if(levelNormalizedProps.isLevelNormalized())
        {
            if(lev.name.equals(hier.levels[hier.levels.length -1].name))
            {
                levelNormalizedProps.setDimInFact(true);
            }
            
        }
        return levelNormalizedProps;
    }

    /**
     * Process the given aggregate table
     */
    // private void loadAggrMeta(String table, Cube cube, RolapCube actCube)
    // {
    // TableDef includeByTableDef =
    // actCube.getAggGroup().getIncludeByTableDef(table);
    // int size = includeByTableDef.getLevels().size();
    // for (int i = 0; i < size; i++)
    // {
    // mondrian.rolap.aggmatcher.ExplicitRules.TableDef.Level level =
    // includeByTableDef.getLevels().get(i);
    // Dimension dimension = new Dimension(level.getColumnName(),
    // level.getRolapLevel().keyOrdinal,level.getName());
    // dimension.setLevelType(level.getRolapLevel().getLevelType());
    // dimension.setDataType(makeSQLDataTye(level.getRolapLevel().getDatatype()));
    // dimension.setNoOfbits(cube.getDimension(level.getColumnName()).getNoOfbits());
    //
    // dimension.setOrdinalCol( assignOrderKeys &&
    // (!level.getRolapLevel().getOrdinalExp().equals(level.getRolapLevel().getKeyExp())));
    //
    // // Update properties and its data types in dimension
    // RolapProperty[] properties = level.getRolapLevel().getProperties();
    // SqlStatement.Type[] propertyTypes = new
    // SqlStatement.Type[properties.length];
    //
    // dimension.setPropertyCount(properties.length);
    // dimension.setPropertyTypes(propertyTypes);
    // dimension.setSchemaOrdinal(i);
    //
    // for (int pCounter = 0; pCounter < properties.length; pCounter++)
    // {
    // propertyTypes[pCounter] = makeSQLDataTye(properties[pCounter].getType());
    // }
    //
    // cube.getDimensions(table).add(dimension);
    // }
    // }

    /**
     * private constructor.
     */
    private MolapMetadata()
    {

    }
//CHECKSTYLE:OFF    Approval No:Approval-253
    /**
     * For Measure metadata.
     * @author S71955
     *
     */
    public static class Measure extends Dimension
    {//CHECKSTYLE:ON
        /**
         * 
         */
        private static final long serialVersionUID = -4047315664600047485L;

        /**
         * aggName.
         */
        private String aggName;
        
        /**
         * aggClassName.
         */
        private String aggClassName;
        
        /**
         * isSurrogateGenerated
         */
        private boolean isSurrogateGenerated;
        
        /**
         * Minimum value in that cube.Used for distinct-count agg.
         */
        private double minValue;
        
        /**
         * isDistinctQuery
         */
        private boolean isDistinctQuery;
        
        /**
         * queryOrder
         */
//        private int queryOrder;

       /**
        * private constructor.
        */
        public Measure()
        {

        }

        /**
         * Measure type constructor.
         * @param colName
         * @param ordinal
         * @param aggName
         * @param aggClassName
         * @param name
         * @param dataType
         * @param cube
         */
        public Measure(String colName, int ordinal, String aggName, String aggClassName, String name,
                SqlStatement.Type dataType, Cube cube, boolean isSurrogateGenerated)
        {
            super(colName, ordinal, name,cube);
            this.aggName = aggName;
            this.dataType = dataType;
            this.aggClassName = aggClassName;
            this.isSurrogateGenerated = isSurrogateGenerated;
        }

        /**
         * Method will get the AggClassName
         * @return String
         */
        public String getAggClassName()
        {
            if(this.aggClassName == null)
            {
                return "";
            }
            return aggClassName;
        }

        /**
         * Method will get the getAggName
         * @return String
         */
        public String getAggName()
        {
            return aggName;
        }

        /**
         * Method will set the aggName
         * @param aggName
         */
        public void setAggName(String aggName)
        {
            this.aggName = aggName;
        }

        /**
         * Method will get the copy of Measure
         */
        public Measure getCopy()
        {
            Measure copy = new Measure();
            copy.aggName = this.aggName;
            copy.dataType = this.dataType;
            copy.colName = this.colName;
            copy.levelType = this.levelType;
            copy.name = this.name;
            copy.keyOrdinal = this.keyOrdinal;
            copy.schemaOrdinal = this.schemaOrdinal;
            copy.hasOrdinalCol = this.hasOrdinalCol;
//            copy.noOfbits = this.noOfbits;
            copy.propertyCount = this.propertyCount;
            copy.propertyTypes = this.propertyTypes;
            copy.aggClassName = this.aggClassName;
            copy.isSurrogateGenerated=this.isSurrogateGenerated;
            copy.minValue = minValue;
            copy.isDistinctQuery=isDistinctQuery;
            copy.queryOrder=queryOrder;
            return copy;
        }
        
        
        @Override
        public boolean equals(Object obj)
        {
            Measure that = null;

            if(obj instanceof Measure)
            {

                that = (Measure)obj;
                return that.name.equals(name);
            }
            // Added this to fix Find bug
            // Symmetric issue
            if(obj instanceof Dimension)
            {
                return super.equals(obj);
            }
            return false;

        }

        @Override
        public int hashCode()
        {
            return colName.hashCode();
        }

        /**
         * @return the isSurrogateGenerated
         */
        public boolean isSurrogateGenerated()
        {
            return isSurrogateGenerated;
        }

        /**
         * @param isSurrogateGenerated the isSurrogateGenerated to set
         */
        public void setSurrogateGenerated(boolean isSurrogateGenerated)
        {
            this.isSurrogateGenerated = isSurrogateGenerated;
        }

        /**
         * @return the minValue
         */
        public double getMinValue()
        {
            return minValue;
        }

        /**
         * @param minValue the minValue to set
         */
        public void setMinValue(double minValue)
        {
            this.minValue = minValue;
        }
        public boolean isDistinctQuery()
        {
            return isDistinctQuery;
        }
        public void setDistinctQuery(boolean isDistinctQuery)
        {
            this.isDistinctQuery = isDistinctQuery;
        }

        /*public int getQueryOrder()
        {
            return queryOrder;
        }

        public void setQueryOrder(int queryOrder)
        {
            this.queryOrder = queryOrder;
        }*/

    }
//CHECKSTYLE:OFF    Approval No:Approval-254
    /**
     * For class Dimension.
     * @author S71955
     *
     */
    public static class Dimension implements Serializable
    {
/**
     * 
     */
    private static final long serialVersionUID = -2508947804631947544L;

//CHECKSTYLE:ON
        /**
         * dataType.
         */
        protected SqlStatement.Type dataType;

        /**
         * colName.
         */
        protected String colName;

        /**
         * levelType.
         */
        protected LevelType levelType;

        /**
         * name.
         */
        protected String name;

        /**
         * keyOrdinal.
         */
        protected int keyOrdinal;

        /**
         * schemaOrdinal.
         */
        protected int schemaOrdinal;

        /**
         * hasOrdinalCol.
         */
        protected boolean hasOrdinalCol;

//        /**
//         * noOfbits.
//         */
//        protected int noOfbits = 32;

        /**
         * propertyCount.
         */
        protected int propertyCount;

        /**
         * propertyTypes.
         */
        protected SqlStatement.Type[] propertyTypes;

        /**
         * aggColumn.
         */
        private String aggColumn;
        
        /**
         * tableName.
         */
        private String tableName;
        
        /**
         * cube.
         */
        private Cube cube;
        
        /**
         * isActualCol.
         */
        private boolean isActualCol;
        
        /**
         * isNormalized.
         */
        protected boolean isNormalized;
        
        /**
         * Dimension present in fact
         * Only in case of isNormalized = true
         */
        private Dimension dimInFact;
        
        /**
         * Hierarchy Name
         */
        private String hierName;
        
        /**
         * Dimension Name
         */
        private String dimName;
        
        /**
         * It has name column or not
         */
        private boolean hasNameColumn;
        
        /**
         * Aggregation table name
         */
        private String aggTable;
        
        /**
         * Is hasAll set in hirarchy
         */
        private boolean hasAll = true;
        
        /**
         * Name column Index
         */
        private int nameColumnIndex=-1;
        
        /**
         * property index
         */
        private int[] propertyIndexes;
        
        /**
         * actualTableName
         */
        private String actualTableName;
        
        /**
         * isQueryForDistinctCount
         */
        private boolean isQueryForDistinctCount;
        
        /**
         * queryOrder
         */
        protected int queryOrder;
        
        /**
         * parentName
         */
        private String parentName; 

        private boolean highCardinalityDim;

        /**
         * dataBlockIndexs
         */
        private int dataBlockIndexs; 

        /**
         * dataBlockIndexs
         */
        private int[] allApplicableDataBlockIndexs; 
        
        public void setAllApplicableDataBlockIndexs(int[] allApplicableDataBlockIndexs)
        {
            this.allApplicableDataBlockIndexs = allApplicableDataBlockIndexs;
        }
        
        public int[] getAllApplicableDataBlockIndexs()
        {
            return allApplicableDataBlockIndexs;
        }

        public int getDataBlockIndex()
        {
            return dataBlockIndexs;
        }

        public void setDataBlockIndex(int dataBlockIndexs)
        {
            this.dataBlockIndexs = dataBlockIndexs;
        }

        public String getParentName()
        {
            return parentName;
        }

        public void setParentName(String parentName)
        {
            this.parentName = parentName;
        }

        /**
         * @return
         */
        public String getDimName()
        {
            return dimName;
        }

        /**
         * 
         * @param highCardinalityDim
         */
        public void setHighCardinalityDims(boolean highCardinalityDim)
        {
            this.highCardinalityDim=highCardinalityDim;
            
        }
        
        /**
         * isHighCardinalityDim.
         * @return
         */
        public boolean isHighCardinalityDim()
        {
            return highCardinalityDim;
        }


        /**
         * @param dimName
         */
        public void setDimName(String dimName)
        {
            this.dimName = dimName;
        }

        /**
         * @return
         */
        public String getHierName()
        {
            return hierName;
        }

        /**
         * @param hierName
         */
        public void setHierName(String hierName)
        {
            this.hierName = hierName;
        }

        /**
         * 
         * @return
         */
        public Dimension getDimInFact()
        {
            return dimInFact;
        }

        /**
         * 
         * @param dimInFact
         */
        public void setDimInFact(Dimension dimInFact)
        {
            this.dimInFact = dimInFact;
        }

        /**
         * Currently set only for Dimension object
         * Measure will have cube=null
         * 
         * @return
         *
         */
        public Cube getCube()
        {
            return cube;
        }

        /**
         * setCube object.
         * @param cube
         */
        public void setCube(Cube cube)
        {
            this.cube = cube;
        }

        /**
         * constrcutor for dimension.
         */
        private Dimension()
        {

        }

        /**
         * setAggColumn.
         * @param aggColumn
         */
        public void setAggColumn(String aggColumn)
        {
            this.aggColumn = aggColumn;
        }

        /**
         * getAggColumn().
         * @return String.
         */
        public String getAggColumn()
        {
            return aggColumn;
        }

        /**
         * get the copy of Dimension.
         * @return Dimension instance.
         */
        public Dimension getDimCopy()
        {
            Dimension copy = new Dimension();
            copy.dataType = this.dataType;
            copy.colName = this.colName;
            copy.parentName = this.parentName;
            copy.levelType = this.levelType;
            copy.name = this.name;
            copy.keyOrdinal = this.keyOrdinal;
            copy.schemaOrdinal = this.schemaOrdinal;
            copy.hasOrdinalCol = this.hasOrdinalCol;
            copy.highCardinalityDim=this.highCardinalityDim;
//            copy.noOfbits = this.noOfbits;
            copy.propertyCount = this.propertyCount;
            copy.propertyTypes = this.propertyTypes;
            copy.tableName = this.tableName;
            copy.hierName=this.hierName;
            copy.dimName=this.dimName;
            copy.isNormalized=this.isNormalized;
            copy.dimInFact=this.dimInFact;
            copy.hasNameColumn=this.hasNameColumn;
            copy.aggTable = this.aggTable;
            copy.aggColumn = this.aggColumn;
            copy.hasAll = this.hasAll;
            copy.nameColumnIndex = this.nameColumnIndex;
            copy.propertyIndexes = this.propertyIndexes;
            copy.actualTableName=this.actualTableName;
            copy.isQueryForDistinctCount=this.isQueryForDistinctCount;
            copy.queryOrder=queryOrder;
            return copy;
        }

        /**
         * setDataType.
         * @param dataType
         */
        public void setDataType(SqlStatement.Type dataType)
        {
            this.dataType = dataType;
        }

        /**
         * getDataType
         * @return SqlStatement.Type.
         */
        public SqlStatement.Type getDataType()
        {
            return dataType;
        }

        /**
         * hasOrdinalCol()
         * @return boolean.
         */
        public boolean hasOrdinalCol()
        {
            return hasOrdinalCol;
        }

        /**
         * setOrdinalCol.
         * @param hasOrdinalCol
         */
        public void setOrdinalCol(boolean hasOrdinalCol)
        {
            this.hasOrdinalCol = hasOrdinalCol;
        }

        /**
         * getSchemaOrdinal()
         * @return int.
         */
        public int getSchemaOrdinal()
        {
            return schemaOrdinal;
        }

        /**
         * setSchemaOrdinal().
         * @param schemaOrdinal
         */
        public void setSchemaOrdinal(int schemaOrdinal)
        {
            this.schemaOrdinal = schemaOrdinal;
        }

        /**
         * getPropertyTypes()
         * @return SqlStatement.Type[].
         */
        public SqlStatement.Type[] getPropertyTypes()
        {
            return propertyTypes;
        }

        public void setPropertyTypes(SqlStatement.Type[] propertyTypes)
        {
            this.propertyTypes = propertyTypes;
        }

        public Dimension(String colName, int keyOrdinal, String name, Cube cube)
        {
            this.colName = colName;
            this.keyOrdinal = keyOrdinal;
            this.name = name;
            this.cube=cube;
        }
        
        public Dimension(String colName, int keyOrdinal, String name)
        {
            this.colName = colName;
            this.keyOrdinal = keyOrdinal;
            this.name = name;
        }

        // public Dimension(String colName, int ordinal)
        // {
        // this.colName = colName;
        // this.ordinal = ordinal;
        // }
        //
        // public Dimension(String colName, LevelType levelType, String
        // levelName)
        // {
        // this.colName = colName;
        // this.levelType = levelType;
        // this.levelName = levelName;
        // }

        /**
         * @return the colName
         */
        public String getColName()
        {
            return colName;
        }

        /**
         * @param colName
         *            the colName to set
         */
        public void setColName(String colName)
        {
            this.colName = colName;
        }

        /**
         * @return the levelType
         */
        public LevelType getLevelType()
        {
            return levelType;
        }

        /**
         * @param levelType
         *            the levelType to set
         */
        public void setLevelType(LevelType levelType)
        {
            this.levelType = levelType;
        }

        /**
         * @return the levelName
         */
        public String getName()
        {
            return name;
        }

        /**
         * @param molapLevelName
         *            the levelName to set
         */
        public void setName(String name)
        {
            this.name = name;
        }
        
        /**
         * 
         * @return
         */
        public boolean isNormalized()
        {
            return isNormalized;
        }

        /**
         * 
         * @param isNormalized
         */
        public void setNormalized(boolean isNormalized)
        {
            this.isNormalized = isNormalized;
        }

        /**
         * @return the ordinal
         */
        public int getOrdinal()
        {
            return keyOrdinal;
        }

        public void setOrdinal(int keyOrdinal)
        {
            this.keyOrdinal = keyOrdinal;
        }

        public void setPropertyCount(int length)
        {
            propertyCount = length;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((dimName == null) ? 0 : dimName.hashCode());
            result = prime * result + ((hierName == null) ? 0 : hierName.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj)
        {
            if(this == obj)
            {
                return true;
            }
            if(obj == null)
            {
                return false;
            }
            if(!(obj instanceof Dimension))
            {
                return false;
            }
            Dimension other = (Dimension)obj;
            if(dimName == null)
            {
                if(other.dimName != null)
                {
                    return false;
                }
            }
            else if(!dimName.equals(other.dimName))
            {
                return false;
            }
            if(hierName == null)
            {
                if(other.hierName != null)
                {
                    return false;
                }
            }
            else if(!hierName.equals(other.hierName))
            {
                return false;
            }
            if(name == null)
            {
                if(other.name != null)
                {
                    return false;
                }
            }
            else if(!name.equals(other.name))
            {
                return false;
            }
            return true;
        }

        public int getPropertyCount()
        {
            return propertyCount;
        }

        /**
         * Gives the count of additional columns (ordinalColumn +
         * propertiesCount + etc)
         */
        public int getTotalAttributeCount()
        {
            return propertyCount + (hasOrdinalCol ? 1 : 0);
        }

//        /**
//         * @return the noOfbits
//         */
//        public int getNoOfbits()
//        {
//            return noOfbits;
//        }
//
//        /**
//         * @param noOfbits
//         *            the noOfbits to set
//         */
//        public void setNoOfbits(int noOfbits)
//        {
//            this.noOfbits = noOfbits;
//        }

        /**
         * @return the isActualCol
         */
        public boolean isActualCol()
        {
            return isActualCol;
        }

        /**
         * @param isActualCol the isActualCol to set
         */
        public void setActualCol(boolean isActualCol)
        {
            this.isActualCol = isActualCol;
        }

        /**
         * @return the tableName
         */
        public String getTableName()
        {
            return tableName;
        }

        /**
         * @param tableName the tableName to set
         */
        public void setTableName(String tableName)
        {
            if(tableName.contains("."))
            {
                tableName = tableName.split("\\.")[1];
            }
            this.tableName = tableName;
        }

        /**
         * @return the hasNameColumn
         */
        public boolean isHasNameColumn()
        {
            return hasNameColumn;
        }

        /**
         * @param hasNameColumn the hasNameColumn to set
         */
        public void setHasNameColumn(boolean hasNameColumn)
        {
            this.hasNameColumn = hasNameColumn;
        }

        /**
         * @return the aggTable
         */
        public String getAggTable()
        {
            return aggTable;
        }

        /**
         * @param aggTable the aggTable to set
         */
        public void setAggTable(String aggTable)
        {
            this.aggTable = aggTable;
        }

        /**
         * @return the hasAll
         */
        public boolean isHasAll()
        {
            return hasAll;
        }

        /**
         * @param hasAll the hasAll to set
         */
        public void setHasAll(boolean hasAll)
        {
            this.hasAll = hasAll;
        }
        /**
         * 
         * @Author s71955
         * @Description : getNameColumnIndex
         * @return
         */

        public int getNameColumnIndex()
        {
            return nameColumnIndex;
        }

        /**
         * 
         * @Author s71955
         * @Description : setNameColumnIndex
         * @param nameColumnIndex
         */
        public void setNameColumnIndex(int nameColumnIndex)
        {
            this.nameColumnIndex = nameColumnIndex;
        }

        /**
         * 
         * @Author s71955
         * @Description : getPropertyIndexes
         * @return
         */
        public int[] getPropertyIndexes()
        {
            return propertyIndexes;
        }

        /**
         * 
         * @Author s71955
         * @Description : setPropertyIndexes
         * @param propertyIndexes
         */
        public void setPropertyIndexes(int[] propertyIndexes)
        {
            this.propertyIndexes = propertyIndexes;
        }

        /**
         * 
         * @Author s71955
         * @Description : getActualTableName
         * @return
         */
        public String getActualTableName()
        {
            return actualTableName;
        }

        /**
         * 
         * @Author s71955
         * @Description : setActualTableName
         * @param actualTableName
         */
        public void setActualTableName(String actualTableName)
        {
            this.actualTableName = actualTableName;
        }

        public boolean isQueryForDistinctCount()
        {
            return isQueryForDistinctCount;
        }
        public void setQueryForDistinctCount(boolean isQueryForDistinctCount)
        {
            this.isQueryForDistinctCount = isQueryForDistinctCount;
        }

        public int getQueryOrder()
        {
            return queryOrder;
        }

        public void setQueryOrder(int queryOrder)
        {
            this.queryOrder = queryOrder;
        }

        public void getParent()
        {
            // TODO Auto-generated method stub
            
        }
    }
//CHECKSTYLE:OFF    Approval No:Approval-257
    /**
     * Cube class.
     * @author S71955
     *
     */
    public static class Cube implements Serializable
    {//CHECKSTYLE:ON
        /**
         * 
         */
        private static final long serialVersionUID = 3674964637028252706L;

        /**
         * TableName, Dimensions list
         */
        private Map<String, List<Dimension>> dimensions = new HashMap<String, List<Dimension>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        /**
         * TableName, factCount Column Name
         */
        private Map<String, String> factCountColumnMapping = new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        /**
         * hierarchiesMapping
         */
        private Map<String, List<Dimension>> hierarchiesMapping = new LinkedHashMap<String, List<Dimension>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        /**
         * complexTypeMapping
         */
//        private Map<String, HashMap<Integer, ComplexColumnMetadata>> complexTypesMap = new HashMap<String, HashMap<Integer, ComplexColumnMetadata>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        /**
         * TableName, columns list [Meta data i.e. from original database]
         */
        private Map<String, Set<String>> metaTables = new HashMap<String, Set<String>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        private String mode = MolapCommonConstants.MOLAP_MODE_DEFAULT_VAL;
        
        /**
         * measures.
         */
        private Map<String, List<Measure>> measures = new HashMap<String, List<Measure>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        /**
         * TableName, Measures list.
         */
        private String factTableName;
        
        /**
         * schemaName.
         */
        private String schemaName;

        private String autoAggregationType;
        
        /**
         * isFullyDenormalized
         */
        private boolean isFullyDenormalized;
        
        private String metaDataFilepath;

        /**
         * cubeName.
         */
        private String cubeName;
        /**
         * cubeName.
         */
        private String onlyCubeName;
        
        private long schemaLastUpdatedTime;


        /**
         * schema.
         */
        private Schema schema;

        /**
         * cube.
         */
        private com.huawei.unibi.molap.olap.MolapDef.Cube cube;

        /**
         * @param name
         */
        public Cube(String schemaName, String cubeName,String orgSchemaName, String orgCubeName)
        {
            String basePath = MolapUtil.getCarbonStorePath(orgSchemaName, orgCubeName)/*MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
                    MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)*/;
            
            this.schemaName = schemaName;
            this.cubeName = schemaName + '_'+ cubeName;
            this.metaDataFilepath = basePath + "/schemas/" + schemaName + '/' + cubeName;
        }



        /**
         * 
         * @param schema
         * @param cube
         * @param orginalSchemaName
         * @param orginalCubeName
         */
        public Cube(Schema schema, com.huawei.unibi.molap.olap.MolapDef.Cube cube, String orginalSchemaName,
                String orginalCubeName)
        {
            this(schema.name, cube.name,orginalSchemaName,orginalCubeName);
            this.schema = schema;
            this.cube = cube;
        }

        public String getFactCountColMapping(String tableName)
        {
            return factCountColumnMapping.get(tableName);
        }
        
        /**
         * 
         * @param schema
         * @param cube
         */
        public Schema getSchema()
        {
            return schema;
        }

        /**
         * 
         * @param schema
         * @param cube
         */
        public void setSchema(Schema schema)
        {
            this.schema = schema;
        }

        /**
         * 
         * @param schema
         * @param cube
         */
        public com.huawei.unibi.molap.olap.MolapDef.Cube getCube()
        {
            return cube;
        }

        /**
         * 
         * @param schema
         * @param cube
         */
        public void setCube(com.huawei.unibi.molap.olap.MolapDef.Cube cube)
        {
            this.cube = cube;
        }


        /**
         * Find Dimension by column name
         */
        public Dimension getDimension(String colName, String table)
        {
            List<Dimension> list = dimensions.get(table);
            for(Dimension dimension : list)
            {
                if(colName.equals(dimension.getColName()))
                {
                    return dimension;
                }
            }
            return null;
        }
        /**
         * 
         * @Author S71955
         * @Description : getDimension
         * @param bitPosition
         * @param colName
         * @param table
         * @return
         */
        public Dimension getDimension(String uniqueNameCol, String colName, String table)
        {
            List<Dimension> list = dimensions.get(table);
            for(Dimension dimension : list)
            {
                String uniqueName=null;
                if(null!=dimension.getHierName() && !dimension.getDimName().equals(dimension.getHierName()))
                {
                    uniqueName='['+ dimension.getDimName()+'.'+dimension.getHierName()+']'+'.'+'['+dimension.name+']';
                }
                else
                {
                    uniqueName='['+dimension.getDimName()+']'+'.'+'['+dimension.name+']';
                }
                if(colName.equals(dimension.getColName()) && uniqueNameCol.equals(uniqueName))
                {
                    return dimension;
                }
            }
            return null;
        }

        public List<Dimension> getChildren(String dimName) {
            List<Dimension> retList = new ArrayList<Dimension>();
            for(List<Dimension> list : dimensions.values())
            {
                for(Dimension dimension : list)
                {
                    if(null != dimension.getParentName() && 
                            dimension.getParentName().equalsIgnoreCase(dimName)){
                        retList.add(dimension);
                    }
                }
            }
            return retList;
        }
        
        /**
         * Find Dimension by column name
         */
        public Dimension getAggDimension(String colName, String table)
        {
            List<Dimension> list = dimensions.get(table);
            for(Dimension dimension : list)
            {
                if(colName.equals(dimension.getAggColumn()))
                {
                    return dimension;
                }
            }
            return null;
        }
        
        
        /**
         * Find Dimension by unique name (table name_column name)
         */
        public Dimension getDimensionByUniqueName(String tableColName, String table)
        {
            List<Dimension> list = dimensions.get(table);
            for(Dimension dimension : list)
            {
                if(tableColName.equals(dimension.getTableName() + '_' + dimension.getColName()))
                {
                    return dimension; 
                }
            }
            return null;
        }
        
        /**
         * Find Dimension by unique name (table name_column name)
         */
        public Dimension getDimensionByUniqueDimensionAndHierName(String tableColumnName, String table, String dimName, String hierName)
        {
            List<Dimension> list = dimensions.get(table);
            for(Dimension dimension : list)
            {
                if(dimName.equals(dimension.getDimName()) && (null==hierName || hierName.equals(dimension.getHierName()))
                        && tableColumnName.equals(dimension.getTableName() + '_' + dimension.getColName()))
                {
                    return dimension;
                }
            }
            return null;
        }

        /**
         * Find Dimension by column name
         */
        public Dimension getDimension(String dimName)
        {
            for(List<Dimension> list : dimensions.values())
            {
                for(Dimension dimension : list)
                {
                    if(dimName.equals(dimension.getDimName()))
                    {
                        return dimension;
                    }
                }
            }
            return null;
        }
        
        /**
         * Find Dimension by column name
         */
        public Measure getMeasure(String mesName)
        {
            for(List<Measure> list : measures.values())
            {
                for(Measure measure : list)
                {
                    if(mesName.equals(measure.getName()))
                    {
                        return measure;
                    }
                }
            }
            return null;
        }
        
        /**
         * Find Dimension by column name
         */
        public Dimension getDimensionByLevelName(String dimName, String hierName,String levelName,String tableName)
        {
            List<Dimension> list = dimensions.get(tableName);
            for(Dimension dimension : list)
            {
                if(dimName.equals(dimension.getDimName()) && (null==hierName || hierName.equals(dimension.getHierName()))
                        && levelName.equals(dimension.getName()))
                {
                    return dimension;
                }
            }
            return null;
        }
        
        /**
         * Find Dimension by column name
         */
        public Dimension getDimensionByLevelName(String dimName, String hierName,String levelName)
        {
            for(List<Dimension> list : dimensions.values())
            {
                for(Dimension dimension : list)
                {
                    if(dimName.equals(dimension.getDimName()) && (null==hierName || hierName.equals(dimension.getHierName()))
                            && levelName.equals(dimension.getName()))
                    {
                        return dimension;
                    }
                }
            }
            return null;
        }

        /**
         * @return the dimensions
         */
        public List<Dimension> getDimensions(String table)
        {
            List<Dimension> list = dimensions.get(table);
            if(list == null)
            {
                list = new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                dimensions.put(table, list);
            }
            return list;
        }

//        public KeyGenerator getKeyGenerator(String table)
//        {
//            List<Dimension> list = dimensions.get(table);
//            int[] lens = new int[list.size()];
//            for(int i = 0;i < lens.length;i++)
//            {
//                lens[i] = list.get(i).getNoOfbits();
//            }
//           // return new MultiDimKeyVarLengthGenerator(lens);
//            return KeyGeneratorFactory.getKeyGenerator(lens);
//        }

        /**
         * Get measure with given name
         */
        public Measure getMeasure(String table, String name)
        {
            for(Measure measure : measures.get(table))
            {
                if(measure.getName().equals(name))
                {
                    return measure;
                }
            }
            return null;
        }

        /**
         * Returns all the tables list (fact + aggregate tables)
         */
        public Set<String> getTablesList()
        {
            return dimensions.keySet();
        }

        /**
         * @return the measures
         */
        public List<Measure> getMeasures(String table)
        {
            List<Measure> list = measures.get(table);

            if(list == null)
            {
                list = new ArrayList<Measure>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                measures.put(table, list);
            }
            return list;
        }

        /**
         * @return the measures
         */
        public Set<String> getMetaTableColumns(String table)
        {
            Set<String> columns = metaTables.get(table);

            if(columns == null)
            {
                columns = new LinkedHashSet<String>();
                metaTables.put(table, columns);
            }
            return columns;
        }

        /**
         * @return the measures
         */
        public Set<String> getMetaTableColumnsForAgg(String table)
        {
            return metaTables.get(table);
        }
        /**
         * @return the name
         */
        public String getCubeName()
        {
            return cubeName;
        }

        /**
         * @param name
         *            the name to set
         */
        public void setCubeName(String name)
        {
            this.cubeName = name;
        }

        public Set<String> getMetaTableNames()
        {
            return metaTables.keySet();
        }

        /**
         * @param hierName
         * @return
         */
        public List<Dimension> getHierarchiesMapping(String hierName)
        {
            return hierarchiesMapping.get(hierName);
        }

        /**
         * Get all hierarchies
         * @return
         */
        public Map<String, List<Dimension>> getAllHierarchiesMapping()
        {
            return hierarchiesMapping;
        }

       /**
         * @param hierName
         * @param dimension
         */
        public void addToHierarchiesMapping(String hierName, Dimension dimension)
        {
            List<Dimension> listOfDims = hierarchiesMapping.get(hierName);
            if(null == listOfDims)
            {
                listOfDims = new ArrayList<MolapMetadata.Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                hierarchiesMapping.put(hierName, listOfDims);
            }
            listOfDims.add(dimension);
        }

    /**
     * @return the onlyCubeName
     */
    public String getOnlyCubeName()
    {
        return onlyCubeName;
    }

    /**
     * @param onlyCubeName the onlyCubeName to set
     */
    public void setOnlyCubeName(String onlyCubeName)
    {
        this.onlyCubeName = onlyCubeName;
    }

    /**
     * @return the isFullyDenormalized
     */
    public boolean isFullyDenormalized()
    {
        return isFullyDenormalized;
    }

    /**
     * @param isFullyDenormalized the isFullyDenormalized to set
     */
    public void setFullyDenormalized(boolean isFullyDenormalized)
    {
        this.isFullyDenormalized = isFullyDenormalized;
    }

    public String getMetaDataFilepath()
    {
        return metaDataFilepath;
    }

    /**
     * @return
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    public void setAutoAggregateType(String autoAggregationType)
    {
        this.autoAggregationType = autoAggregationType;
        
    }
    
    /**
     * @return
     */
    public String getAutoAggregateType()
    {
        return autoAggregationType;
    }

    /**
     * @param schemaName
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * @return
     */
    public String getFactTableName()
    {
        return factTableName;
    }

    
    /**
     * getMode
     * @return String
     */
    public String getMode()
    {
        return mode;
    }
    
    /**
     * setMode
     * @param mode void
     */
    public void setMode(String mode)
    {
        this.mode=mode;
    }

    /**
     * @param factTableName
     */
    public void setFactTableName(String factTableName)
    {
        this.factTableName = factTableName;
    }

    /**
     * 
     * @return Returns the schemaLastUpdatedTime.
     * 
     */
    public long getSchemaLastUpdatedTime()
    {
        return schemaLastUpdatedTime;
    }

    /**
     * 
     * @param schemaLastUpdatedTime The schemaLastUpdatedTime to set.
     * 
     */
    public void setSchemaLastUpdatedTime(long schemaLastUpdatedTime)
    {
        this.schemaLastUpdatedTime = schemaLastUpdatedTime;
    }
    
    }
//CHECKSTYLE:OFF    Approval No:Approval-373
    
    /**
     * 
     */
    private static final HashMap<String, SqlStatement.Type> TYPESMAPPING = new HashMap<String, SqlStatement.Type>(20);
    static
    {
        TYPESMAPPING.put("String", SqlStatement.Type.STRING);
        TYPESMAPPING.put("Numeric", SqlStatement.Type.DOUBLE);
        TYPESMAPPING.put("Integer", SqlStatement.Type.INT);
        TYPESMAPPING.put("Boolean", SqlStatement.Type.INT);
        TYPESMAPPING.put("Date", SqlStatement.Type.OBJECT);
        TYPESMAPPING.put("Time", SqlStatement.Type.OBJECT);
        TYPESMAPPING.put("Timestamp", SqlStatement.Type.TIMESTAMP);
        TYPESMAPPING.put("Array", SqlStatement.Type.ARRAY);
        TYPESMAPPING.put("Struct", SqlStatement.Type.STRUCT);
    }//CHECKSTYLE:ON
//CHECKSTYLE:OFF    Approval No:Approval-374
    /**
     * 
     */
    private static final HashMap<String, String> DBTYPEMAPPING = new HashMap<String, String>();
    static
    {
        DBTYPEMAPPING.put("String", "varchar(50)");
        DBTYPEMAPPING.put("PropString", "text");
        DBTYPEMAPPING.put("Numeric", "real");
        DBTYPEMAPPING.put("Integer", "INT");
        DBTYPEMAPPING.put("Boolean", "TINYINT(1)");
        DBTYPEMAPPING.put("Date", "DATE");
        DBTYPEMAPPING.put("Time", "TIME");
        DBTYPEMAPPING.put("Timestamp", "DATETIME");
    }
//CHECKSTYLE:ON
    public static SqlStatement.Type makeSQLDataTye(String type)
    {
        SqlStatement.Type sqlTYpe = TYPESMAPPING.get(type);
        if(sqlTYpe == null)
        {
            sqlTYpe = SqlStatement.Type.OBJECT;
        }
        return sqlTYpe;
    }

//    public static SqlStatement.Type makeSQLDataTye(Dialect.Datatype datatype)
//    {
//        switch(datatype)
//        {
//        case String:
//            return SqlStatement.Type.STRING;
//        case Numeric:
//            return SqlStatement.Type.DOUBLE;
//        case Integer:
//        case Boolean:
//            return SqlStatement.Type.INT;
//            // return SqlStatement.Type.INT;
//            
//            //Findbugs fix
//        /*case Date:
//            return SqlStatement.Type.OBJECT;
//        case Time:
//            return SqlStatement.Type.OBJECT;
//        case Timestamp:
//            return SqlStatement.Type.OBJECT;*/
//        default:
//            return SqlStatement.Type.OBJECT;
//        }
//    }
//
//    public static SqlStatement.Type makeSQLDataTye(mondrian.olap.Property.Datatype type)
//    {
//        switch(type)
//        {
//        case TYPE_STRING:
//            return SqlStatement.Type.STRING;
//        case TYPE_NUMERIC:
//            return SqlStatement.Type.DOUBLE;
//        case TYPE_BOOLEAN:
//            return SqlStatement.Type.INT;
//            
//            //Find bugs fix
//            //Anything other than above, is OBJECT
//       /* case TYPE_DATE:
//            return SqlStatement.Type.OBJECT;
//        case TYPE_TIME:
//            return SqlStatement.Type.OBJECT;
//        case TYPE_TIMESTAMP:
//            return SqlStatement.Type.OBJECT;
//        case TYPE_OTHER:
//            return SqlStatement.Type.OBJECT;*/
//        default:
//            return SqlStatement.Type.OBJECT;
//        }
//    }

    public static String getDBDataType(String type, boolean isProperty)
    {
        if(isProperty && "String".equals(type))
        {
            type = "PropString";
        }
        String dbType = DBTYPEMAPPING.get(type);
        if(null == dbType)
        {
            dbType = "varchar(50)";
        }
        return dbType;
    }

//    /**
//     * 
//     */
//    public final static boolean assignOrderKeys = MondrianProperties.instance().CompareSiblingsByOrderKey.get();

//    /**
//     * Check whether to consider Ordinal column separately if it is configured.
//     */
//    public static boolean hasOrdinalColumn(Level level)
//    {
//        return (assignOrderKeys && null != level.ordinalColumn && !level.column.equals(level.ordinalColumn));
//    }
//
//    /**
//     * Check whether to consider Ordinal column separately if it is configured.
//     */
//    public static boolean hasOrdinalColumn(RolapLevel level)
//    {
//        return (assignOrderKeys && null != level.getOrdinalExp() && !level.getKeyExp().equals(level.getOrdinalExp()));
//    }

    /**
     * Check whether to consider name column separately if it is configured.
     */
    public static boolean hasNameColumn(Level level)
    {
        return level.getNameExp() != null;
    }
    
    public static boolean hasOrdinalColumn(Level level)
    {
        return level.ordinalColumnIndex>0;
    }

//    /**
//     * Check whether to consider name column separately if it is configured.
//     */
//    public static boolean hasNameColumn(Mon level)
//    {
//        return level.getNameExp() != null;
//    }
//    
 }
