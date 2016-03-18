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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3VUTS0K9xjXhlG2M0DSQKnJKijfRv/FrZXc1/uTZE9gls+jupgjkjiUEjy2gBmRynLEP
ibIOK9w0Htkac2QFj0WQ2ey+j3gh170zWPBb31lIUAymKRf3zxZr5hBxDUN5fg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 */
package com.huawei.unibi.molap.engine.mondrian.extensions;


/**
 * @author R00900208
 * 
 */
public class MolapStatement //extends SqlStatement
{

//    /**
//     * separator
//     */
//    private static final String SEPERATOR = "_";
//
//    /**
//     * 
//     */
//    private List<Accessor> accessors = new ArrayList<SqlStatement.Accessor>();
//
//    /**
//     * 
//     */
//    private DataSource dataSource = null;
//
//    /**
//     * 
//     */
//    private MolapResultHolder iter;
//
//    /**
//     * 
//     */
//    private List<Type> types = new ArrayList<SqlStatement.Type>();
//
//    /**
//     * 
//     */
//    private List<Dimension> dimensionsList = new ArrayList<Dimension>();
//    
//    /**
//     * top n option
//     */
//    private static boolean enableTopNopt = Boolean.parseBoolean(MolapProperties.getInstance().getProperty("molap.enableTopNOptimization", "true")); 
//
//    /**
//     * Attribute for Molap LOGGER
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(MolapStatement.class.getName());
//    
//    /**
//     * Zero sized Dimension to pass in Collection.toArray
//     */
//    private static final Dimension[] d = new Dimension[0];
//    
//    public MolapStatement(DataSource dataSource, String sql, List<Type> types, int maxRows, int firstRowOrdinal,
//            Locus locus, int resultSetType, int resultSetConcurrency)
//    {
//        super(dataSource, sql, types, maxRows, firstRowOrdinal, locus, resultSetType, resultSetConcurrency);
//        this.dataSource = dataSource;
//    }
//
//    public MolapStatement(DataSource dataSource)
//    {
//        super(null, null, null, -1, 0, null, 0, -1);
//        this.dataSource = dataSource;
//    }
//
//    /**
//     * Gets called when final query output can be got from agg table.
//     * 
//     * @param spec
//     */
//    public void execute(AggQuerySpec spec)
//    {
//
//        String factTable = null;
//        // List<Dimension> dims = new ArrayList<Dimension>();
//        Map<Dimension, MolapFilterInfo> pred = new HashMap<Dimension, MolapFilterInfo>();
//        List<Measure> msrs = new ArrayList<Measure>();
//        MolapMetadata metadata = MolapMetadata.getInstance();
//        String cubeUniqueName = spec.getStar().getSchema().getName()+SEPERATOR+spec.getStar().getCubeName();
//        Cube cube = metadata.getCube(cubeUniqueName);
//        // Map<String, Integer> measureOffsetMap =
//        // getMeasureOffsets(spec.getStar().getSchema().getXMLSchema());
//        
//        AggStar.FactTable.Measure msr = (AggStar.FactTable.Measure)spec.getMeasureAsColumn(0);
//
//        factTable = msr.getExpression().getTableAlias();
//        
//        try
//        {
////        long size = MolapExecutorFactory.getMolapExecutor(null, dataSource).executeTableCount(factTable);
////        if(size == 0)
////        {
////            factTable = cube.getFactTableName();
////        }
//        
//        for(int i = 0;i < spec.getMeasureCount();i++)
//        {
//            AggStar.FactTable.Measure msrLocal = (AggStar.FactTable.Measure)spec.getMeasureAsColumn(i);
//            
//            Measure measure = null;
//            /*
//             * This added for fixing the defect "Add count functionality does not work if there is default count function in schema"
//             */
//            if(msrLocal.getExpression() == null)
//            {
//                measure = cube.getMeasures(factTable).get(0);
//                measure = measure.getCopy();
//                measure.setAggName(MolapCommonConstants.COUNT);
//                measure.setName("CALC"+measure.getName()+measure.getAggName());
//            }
//            else
//            {
//                // Measure should be looked up using name because multiple measures
//                // can use same column.
//                measure = cube.getMeasure(factTable, msrLocal.getName());
//            }
//
//
////            Measure molapMeasure = cube.getMeasure(factTable, measureLocal.getName());
//            msrs.add(measure);
//
//        }
//        
//        for(int i = 0;i < spec.getColumnCount();i++)
//        {
//            AggStar.Table.Column tmpColumn = spec.getColumn(i);
//            if(null != tmpColumn)
//            {
//                Dimension dimension = cube.getAggDimension(((MondrianDef.Column)tmpColumn.getExpression()).name,
//                        factTable);
//                getPredicateandDimensionTypes(spec, pred, i, dimension);
//            }
//        }
//
//
//
//            List<Dimension> dimTables = cube.getDimensions(factTable);
//
//            for(Iterator<Measure> iterator = msrs.iterator();iterator.hasNext();)
//            {
//                Measure measure = iterator.next();
//                types.add(measure.getDataType());
//            }
//
//            iter = new MolapResultHolder(types);
//            MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimTables, dataSource);
//
//            String property = cube.getAutoAggregateType();
//            boolean isAutoAgg=false;
//            if(MolapCommonConstants.MOLAP_AUTO_TYPE_VALUE.equalsIgnoreCase(property)
//                    || MolapCommonConstants.MOLAP_MANUAL_TYPE_VALUE.equalsIgnoreCase(property))
//            {
//                isAutoAgg = true;
//            }
//            // For segment query always the columns should be in order as per
//            // schema file
//            // Collections.sort(dims, new DimensionComparator());
//            // Collections.sort(msrs, new DimensionComparator());
//            delegateToExecutor(cube, iter, factTable, dimensionsList.toArray(d), getUpdatedMeasureFor(msrs, true,isAutoAgg),
//                    pred, false, executor, false, -1, false, true, new ArrayList<Measure>(), null, false,false,false,false);
//        }
//        catch (MemoryLimitExceededException e) 
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//            throw e;
//        }
//        catch (ResourceLimitExceededException e) 
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//            throw e;
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//    }
//
//    private void getPredicateandDimensionTypes(AggQuerySpec spec, 
//                                                Map<Dimension, 
//                                                MolapFilterInfo> pred,
//                                                int index, 
//                                                Dimension dimension)
//    {
//        if(null == dimension)
//        {
//            return;
//        }
//
//        dimension = dimension.getDimCopy();
//        dimension.setActualCol(true);
//        dimensionsList.add(dimension);
//        List list = new ArrayList();
//        if(!(spec.getPredicate(index) instanceof LiteralStarPredicate))
//        {
//            spec.getPredicate(index).values(list);
//            if(list.size() > 0)
//            {
//                List<String> inclList = new ArrayList<String>(list.size());
//                for(Object object : list)
//                {
//                    if(RolapUtil.sqlNullValue == object || null == object)
//                    {
//                        inclList.add(addBraces(MolapCommonConstants.MEMBER_DEFAULT_VAL));
//                    }
//                    else
//                    {
//                        inclList.add(addBraces(object.toString()));
//                    }
//                }
//
//                pred.put(dimension, new MolapFilterInfo(null, inclList));
//            }
//        }
//        types.add(dimension.getDataType());
//    }
//    
//    /**
//     * Add braces to member.
//     * @param member
//     * @return
//     */
//    private String addBraces(String member)
//    {
//        return '['+member+']';
//    }
//
//
//    /**
//     * Gets called when final query output is got from fact table.
//     * 
//     * @param spec
//     * 
//     */
//    public void execute(MolapSegmentArrayQuerySpec spec,String tableName) 
//    {
//        String factTable = spec.getStar().getFactTable().getTableName();
//        
//        boolean isAggTable= false;
//        if(null!=tableName)
//        {
//            factTable=tableName;
//            isAggTable=true;
//        }
//        RolapStar.Column[] columns = spec.getColumns();
//        MolapMetadata metadata = MolapMetadata.getInstance();
//        String cubeUniqueName = spec.getStar().getSchema().getName()+SEPERATOR+spec.getStar().getCubeName();
//        Cube cube = metadata.getCube(cubeUniqueName);
//        String property = cube.getAutoAggregateType();
//        boolean isAutoAgg=false;
//        if(MolapCommonConstants.MOLAP_AUTO_TYPE_VALUE.equalsIgnoreCase(property)
//                || MolapCommonConstants.MOLAP_MANUAL_TYPE_VALUE.equalsIgnoreCase(property))
//        {
//            isAutoAgg = true;
//        }
//        // Map<String, Integer> measureOffsetMap =
//        // getMeasureOffsets(spec.getStar().getSchema().getXMLSchema());
//        // List<Dimension> dims = new ArrayList<Dimension>();
//        
//        List<Measure> msrs = new ArrayList<Measure>();
//        Map<Dimension, MolapFilterInfo> pred = new HashMap<Dimension, MolapFilterInfo>();
//        for(int i = 0;i < columns.length;i++)
//        {
//            //Dimension dimension = cube.getDimension(((MondrianDef.Column)columns[i].getExpression()).name, factTable);
//            Dimension dimension =cube.getDimension(columns[i].getUniqueName(),((MondrianDef.Column)columns[i].getExpression()).name, factTable);
//            /**
//             * Fortify Fix: NULL_RETURNS
//             */
//            if(null == dimension)
//            {
//               continue;
//            }
//            dimension = dimension.getDimCopy();
//            dimension.setActualCol(true);
//            dimensionsList.add(dimension);
//            List list = new ArrayList();
//            if(spec.getColumnPredicate(i) != null && !(spec.getColumnPredicate(i) instanceof LiteralStarPredicate))
//            // if(!(spec.getColumnPredicate(i) instanceof LiteralStarPredicate))
//            {
//                spec.getColumnPredicate(i).values(list);
//                if(list.size() > 0)
//                {
//                    List<String> inclList = new ArrayList<String>(list.size());
//                    for(Object object : list)
//                    {
//                        if(RolapUtil.sqlNullValue == object || null == object)
//                        {
//                            inclList.add(addBraces(MolapCommonConstants.MEMBER_DEFAULT_VAL));
//                        }
//                        else
//                        {
//                            inclList.add(addBraces(object.toString()));
//                        }
//                    }
//                    pred.put(dimension, new MolapFilterInfo(null, inclList));
//                }
//            }
//            types.add(dimension.getDataType());
////            if(dimension.hasOrdinalCol())
////            {
////                types.add(SqlStatement.Type.INT);
////            }
////            for(int j = 0;j < dimension.getPropertyCount();j++)
////            {
////                types.add(dimension.getPropertyTypes()[j]);
////            }
//        }
//        addCompoundPredicates(spec, factTable, cube, pred);
//        boolean isUpdatedFilter=false;
//        
//        boolean isCompoundPredicatePresent = false;
//        
//        
//        
////        if(null!=columns && columns.length<1)
////        {
////            isUpdatedFilter=true;
////            pred= getUpdatedFilters(cube,pred, factTable);
////        }
//        
//        List<StarPredicate> compoundPredicateList = spec.getCompoundPredicateList();
//        if(compoundPredicateList != null && compoundPredicateList.size() > 0)
//        {
//            isCompoundPredicatePresent = true;
//        }
//        
//        if(columns.length < 1 && isCompoundPredicatePresent)
//        {
//            Set<Entry<Dimension,MolapFilterInfo>> entrySet = pred.entrySet();
//            int size = -1;
//            for(Entry<Dimension,MolapFilterInfo> entry : entrySet)
//            {
//                int tempSize = entry.getValue().getIncludedMembers().size();
//                if(size == -1)
//                {
//                    size = tempSize;
//                }
//                else if(tempSize != size)
//                {
//                    isCompoundPredicatePresent = false;
//                    break;
//                }
//            }
//            
//            if(isCompoundPredicatePresent)
//            {
//                isUpdatedFilter=true;
//                updatePred(pred);
//            }
//        }
//        
////        if(null != columns && columns.length < 1)
////        {
////
////            Set<Entry<Dimension, MolapFilterInfo>> entrySet = pred.entrySet();
////            int jk = 0;
////            List<String> localInclude1 = new ArrayList<String>();
////            for(Entry<Dimension, MolapFilterInfo> entry : entrySet)
////            {
////                if(jk++ == 0)
////                {
////                    continue;
////                }
////
////                MolapFilterInfo value = entry.getValue();
////                List<String> includedMembers = value.getIncludedMembers();
////                for(String str : includedMembers)
////                {
////                    localInclude.add("[1997]." + str);
////                }
////
////                includedMembers.clear();
////                includedMembers.addAll(localInclude);
////                break;
////            }
////            
////            for(Entry<Dimension, MolapFilterInfo> entry : entrySet)
////            {
////                MolapFilterInfo value = entry.getValue();
////                List<String> includedMembers = value.getIncludedMembers();
////                List<String> localInclude12 = new ArrayList<String>();
////                int count = 0;
////                String[] array1 = localInclude.toArray(new String[localInclude.size()]);
////                for(String str : includedMembers)
////                {
////                    localInclude1.add(array1[count++] + "."+str);
////                }
////
////                includedMembers.clear();
////                includedMembers.addAll(localInclude1);
////                break;
////            }
////        }
//        for(int i = 0;i < spec.getMeasureCount();i++)
//        {
//            RolapStar.Measure msr = spec.getMeasure(i);
//            Measure measure = null;
//            /*
//             * This added for fixing the defect "Add count functionality does not work if there is default count function in schema"
//             */
//            if(msr.getExpression() == null)
//            {
//                if(isAggTable && isAutoAgg)
//                {
//                    measure=getMeasureForAggregateTable(cube,factTable);
//                }
//                else
//                {
//                    measure = cube.getMeasures(factTable).get(0);
//                }
//                measure = measure.getCopy();
//                measure.setAggName(MolapCommonConstants.COUNT);
//                measure.setName("CALC"+measure.getName()+measure.getAggName());
//            }
//            else
//            {
//                measure = cube.getMeasure(factTable, msr.getName());
//            }
//            
//
//            types.add(measure.getDataType());
//            msrs.add(measure);
//        }
//
//        try
//        {
//
//            iter = new MolapResultHolder(types);
//
//            List<Dimension> dimTables = cube.getDimensions(factTable);
//
//            MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimTables, dataSource);
//            // For segment query always the columns should be in order as per
//            // schema file
//            // Collections.sort(dims, new DimensionComparator());
//            // Collections.sort(msrs, new DimensionComparator());
////            if(dimensionsList.size()==0)
////            {
////            int minSize=Integer.MAX_VALUE;
////            for(Entry<Dimension,MolapFilterInfo> info:pred.entrySet())
////            {
////                if(info.getValue().getIncludedMembers().size()<minSize)
////                {
////                    minSize=info.getValue().getIncludedMembers().size();
////                }
////            }
////            for(Entry<Dimension,MolapFilterInfo> info:pred.entrySet())
////            {
////                if(info.getValue().getIncludedMembers().size()>minSize)
////                {
////                    List<String> subList = info.getValue().getIncludedMembers().subList(0, minSize);
////                    info.getValue().addAllIncludedMembers(subList);
////                }
////            }
////            }
//            delegateToExecutor(cube, iter, factTable, dimensionsList.toArray(d),
//                    isAggTable?getUpdatedMeasureFor(msrs, isAggTable, isAutoAgg):msrs, pred, false, executor, isUpdatedFilter, -1,
//                    false, isAggTable, new ArrayList<Measure>(), null, false, false, true, isCompoundPredicatePresent);
//            accessors.clear();
//            for(Type type : guessTypes())
//            {
//                accessors.add(createAccs(accessors.size(), type));
//            }
//
//        }
//        catch (MemoryLimitExceededException e) 
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//            throw e;
//        }
//        catch (ResourceLimitExceededException e) 
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//            throw e;
//        }
//        catch(Exception e)
//        {
//
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//
//    }
//
//    private Measure getMeasureForAggregateTable(Cube cube, String aggTableName)
//    {
//        List<Measure> measures = cube.getMeasures(aggTableName);
//        for(Measure aggMeasuremeasure:measures)
//        {
//            if(!aggMeasuremeasure.getAggName().equals(MolapCommonConstants.CUSTOM)
//                    && !aggMeasuremeasure.getAggName().equals(MolapCommonConstants.DISTINCT_COUNT))
//            {
//                return aggMeasuremeasure;
//            }
//        }
//        return measures.get(0);    
//    }
//    private void updatePred(Map<Dimension, MolapFilterInfo> pred)
//    {
//        Set<Dimension> keySet = pred.keySet();
//        Map<Integer, Dimension> ordinalOrder = new TreeMap<Integer, MolapMetadata.Dimension>();
//        for(Dimension dim : keySet)
//        {
//            int ordinal = dim.getOrdinal();
//            ordinalOrder.put(ordinal, dim);
//        }
//
//        Set<Entry<Integer, Dimension>> entrySet2 = ordinalOrder.entrySet();
//        boolean isFirst = true;
//        String[] array = null;
//        List<String> localInclude = new ArrayList<String>();
////        List<String> processDims = new ArrayList<String>();
//        for(Entry<Integer, Dimension> entry : entrySet2)
//        {
//            MolapFilterInfo molapFilterInfo = pred.get(entry.getValue());
////            if(checkIfNewDimension(processDims,entry.getValue()))
////            {
////                array= null;
////                isFirst=true;
////            }
//            List<String> includedMembers = molapFilterInfo.getIncludedMembers();
//            if(isFirst)
//            {
//                array = includedMembers.toArray(new String[includedMembers.size()]);
//                isFirst = false;
//                continue;
//            }
//            int i = 0;
//            localInclude.clear();
//            for(String str : includedMembers)
//            {
//                localInclude.add(array[i++] + '.' + str);
//            }
//
//            includedMembers.clear();
//            includedMembers.addAll(localInclude);
//
//            array = localInclude.toArray(new String[localInclude.size()]);
//        }
//    }
//
////    private Map<Dimension,MolapFilterInfo> getUpdatedFilters(Cube cube,Map<Dimension,MolapFilterInfo> constrainsts,String factTableName)
////    {
////        Map<Dimension,MolapFilterInfo> newConstrainsts = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>();
////        
////        Map<Dimension,MolapFilterInfo> temp = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(constrainsts);
////        List<Dimension> processedDims=new ArrayList<MolapMetadata.Dimension>();
////        for(Entry<Dimension,MolapFilterInfo> entry:temp.entrySet())
////        {
////            Dimension key = entry.getKey();
////            if(processedDims.contains(key))
////            {
////                continue;
////            }
////            List<Dimension> dimList = cube.getHierarchiesMapping(key.getDimName()+'_'+key.getHierName());
////            int lastLevelIndex=0;
////            for(int i = dimList.size()-1;i >=0;i--)
////            {
////                if(temp.containsKey(dimList.get(i)))
////                {
////                    lastLevelIndex=i;
////                    break;
////                }
////            }
////            List<MolapFilterInfo> molapFilterInfoList = new ArrayList<MolapFilterInfo>();
////            for(int i = 0;i <=lastLevelIndex;i++)
////            {
////                Dimension dimension = dimList.get(i);
////                processedDims.add(dimension);
////                molapFilterInfoList.add(temp.get(dimension));
////            }
////            List<String> incFilterList = new ArrayList<String>();
////            MolapFilterInfo molapFilterInfo = new MolapFilterInfo(null, incFilterList);
////            newConstrainsts.put(dimList.get(lastLevelIndex),molapFilterInfo);
////            int counter=molapFilterInfoList.get(0).getIncludedMembers().size();
////            Set<String> stringSet = new LinkedHashSet<String>();
////            for(int i = 0;i < counter;i++)
////            {
////                StringBuilder builder = new StringBuilder();
////                for(int j = 0;j < molapFilterInfoList.size() - 1;j++)
////                {
////                    builder.append(molapFilterInfoList.get(j).getIncludedMembers().get(i));
////                    builder.append(".");
////                }
////                builder.append(molapFilterInfoList.get(molapFilterInfoList.size() - 1).getIncludedMembers().get(i));
////                stringSet.add(builder.toString());
////            }
////            incFilterList.addAll(stringSet);
////        }
////        return newConstrainsts;
////    }
//
//    /**
//     * Adds the compound predicates to filters.
//     * @param spec
//     * @param factTable
//     * @param cube
//     * @param pred
//     */
//    private void addCompoundPredicates(MolapSegmentArrayQuerySpec spec, String factTable, Cube cube,
//            Map<Dimension, MolapFilterInfo> pred)
//    {
//        List<StarPredicate> predicateList = spec.getPredicateList();
//        if(predicateList != null && predicateList.size() > 0)
//        {
//            for (StarPredicate predicate : predicateList) 
//            {
////                List<Dimension> dimensions = new ArrayList<Dimension>();
////                for (RolapStar.Column column
////                    : predicate.getConstrainedColumnList())
////                {
////                    Dimension dimension = cube.getDimension(((MondrianDef.Column)column.getExpression()).name, factTable);
////                    dimension = dimension.getDimCopy();
////                    dimension.setActualCol(true);
////                    dimensions.add(dimension);
////                }
//                if(predicate instanceof ValueColumnPredicate)
//                {
//                    addValueColumnPred(pred, predicate,cube,factTable,false);
//                }
//                else if(predicate instanceof ListPredicate)
//                {
//                    extractListPredicates(pred, predicate, cube,factTable);
//                }
//                else if(predicate instanceof ListColumnPredicate)
//                {
//                    addListColumnPredicates(pred, predicate, cube,factTable,false);
//                }
//            }
//        }
//    }
//
//    /**
//     * @param pred
//     * @param predicate
//     * @param dimensions
//     */
//    private void addListColumnPredicates(Map<Dimension, MolapFilterInfo> pred, StarPredicate predicate,Cube cube,String factTable,boolean or)
//    {
//        ListColumnPredicate listPredicate = (ListColumnPredicate)predicate;
//        Column constrainedColumn = listPredicate.getConstrainedColumn();
//        Dimension dimension = cube.getDimension(((MondrianDef.Column)constrainedColumn.getExpression()).name, factTable);
//        if(dimension != null)
//        {
//            dimension = dimension.getDimCopy();
//            dimension.setActualCol(true);
//        }
//        List list = new ArrayList();
//        listPredicate.values(list);
//        if(list.size() > 0)
//        {
//            List<String> inclList = new ArrayList<String>(list.size());
//            for(Object object : list)
//            {
//                if(RolapUtil.sqlNullValue == object || null == object)
//                {
//                    inclList.add(addBraces(MolapCommonConstants.MEMBER_DEFAULT_VAL));
//                }
//                else
//                {
//                    inclList.add(addBraces(object.toString()));
//                }
//            }
//            MolapFilterInfo molapFilterInfo = pred.get(dimension);
//            if(molapFilterInfo != null)
//            {
//                if(or)
//                {
//                    molapFilterInfo.getIncludedOrMembers().addAll(inclList);
//                }
//                else
//                {
//                    molapFilterInfo.getIncludedMembers().addAll(inclList);
//                }
//            }
//            else
//            {
//                if(or)
//                {
//                    MolapFilterInfo filterInfo = new MolapFilterInfo(null, null);
//                    filterInfo.getIncludedOrMembers().addAll(inclList);
//                    pred.put(dimension, filterInfo);
//                }
//                else
//                {
//                    pred.put(dimension, new MolapFilterInfo(null, inclList));
//                }
//            }
//        }
//    }
//
//    /**
//     * @param pred
//     * @param predicate
//     * @param dimension
//     */
//    private void extractListPredicates(Map<Dimension, MolapFilterInfo> pred, StarPredicate predicate,Cube cube,String factTable)
//    {
//        ListPredicate listPredicate = (ListPredicate)predicate;
//        boolean or = false;
//        if(listPredicate instanceof OrPredicate)
//        {
//            or = true;
//        }
//        List<StarPredicate> children = listPredicate.getChildren();
//        for(StarPredicate starPredicate : children)
//        {
//            if(starPredicate instanceof ValueColumnPredicate)
//            {
//                addValueColumnPred(pred, starPredicate, cube,factTable,or);
//            }
//            else if(starPredicate instanceof ListPredicate)    
//            {
//                extractListPredicates(pred, starPredicate, cube,factTable);
//            }
//            else if(starPredicate instanceof ListColumnPredicate)
//            {
//                addListColumnPredicates(pred, starPredicate, cube,factTable,or); 
//            }
//        }
//    }
//    
//
//    /**
//     * @param pred
//     * @param predicate
//     * @param dimension
//     */
//    private void addValueColumnPred(Map<Dimension, MolapFilterInfo> pred, StarPredicate predicate,Cube cube,String factTable,boolean or)
//    {
//        
//        ValueColumnPredicate valueColumnPredicate = (ValueColumnPredicate)predicate;
//        Column constrainedColumn = valueColumnPredicate.getConstrainedColumn();
//        String uniqueName = constrainedColumn.getUniqueName();
//        String[] split = uniqueName.split("\\]\\.\\[");
//        String[] hierDimName = null;
//        if(split[0].contains("."))
//        {
//            String dimHierName = removeBraces(split[0]);
//            hierDimName = dimHierName.split("\\.");
//            
//        }
//        Dimension dimension = null;
//        if(hierDimName == null)
//        {
//             dimension = cube.getDimension(((MondrianDef.Column)constrainedColumn.getExpression()).name, factTable);
//        }
//        else
//        {
//            String table_columnName = ((MondrianDef.Column)constrainedColumn.getExpression()).table + '_'
//                    + ((MondrianDef.Column)constrainedColumn.getExpression()).name;
//            dimension = cube.getDimensionByUniqueDimensionAndHierName(table_columnName, factTable, hierDimName[0],
//                    hierDimName[1]);
//        }
//        
//        if(dimension != null)
//        {
//            dimension = dimension.getDimCopy();
//            dimension.setActualCol(true);
//        }
//        MolapFilterInfo molapFilterInfo = pred.get(dimension);
//        String string = valueColumnPredicate.getValue().toString();
//        if(molapFilterInfo == null)
//        {
//            List<String> inclList = new ArrayList<String>();
//            molapFilterInfo = new MolapFilterInfo(null, inclList);
//            pred.put(dimension,molapFilterInfo);
//        }
//        if(null == string || RolapUtil.sqlNullValue.equals(string))
//        {
//            if(or)
//            {
//                molapFilterInfo.getIncludedOrMembers().add(addBraces(MolapCommonConstants.MEMBER_DEFAULT_VAL));
//            }
//            else
//            {
//                molapFilterInfo.getIncludedMembers().add(addBraces(MolapCommonConstants.MEMBER_DEFAULT_VAL));
//            }
//        }
//        else
//        {
//            if(or)
//            {
//                molapFilterInfo.getIncludedOrMembers().add(addBraces(string));
//            }
//            else
//            {
//                molapFilterInfo.getIncludedMembers().add(addBraces(string));
//            }
//        }
//    }
//    
//    
//    
//    private String removeBraces(String string)
//    {
//        if(string.charAt(0)=='[')
//        {
//            string = string.substring(1,string.length());
//        }
//        
//        if(string.charAt(string.length()-1)==']')
//        {
//            string = string.substring(0, string.length()-1);
//        }
//        
//        return string;
//        
//    }
//    
//    /**
//     * It return the MOLAP SQL query from spec.
//     * 
//     * @param spec
//     * 
//     */
//    public String getMolapQuery(DrillThroughQuerySpec spec) 
//    {
//        String factTable = spec.getStar().getFactTable().getTableName();
//        RolapStar.Column[] columns = spec.getColumns();
//        MolapSQLQuery query = new MolapSQLQuery();
//        query.addTable(factTable);
//        
//        MolapMetadata metadata = MolapMetadata.getInstance();
//        String cubeUniqueName = spec.getStar().getSchema().getName()+SEPERATOR+spec.getStar().getCubeName();
//        Cube cube = metadata.getCube(cubeUniqueName);
//        query.setCubeName(spec.getStar().getCubeName());
//        query.setSchemaName(spec.getStar().getSchema().getName());
//        for(int i = 0;i < columns.length;i++)
//        {
//            if(!spec.countOnly)
//            {
//                query.addColumn(((MondrianDef.Column)columns[i].getExpression()).name);
//            }
//            List list = new ArrayList();
//            if(spec.getColumnPredicate(i) != null && !(spec.getColumnPredicate(i) instanceof LiteralStarPredicate))
//            {
//                spec.getColumnPredicate(i).values(list);
//                if(list.size() > 0)
//                {
//                    List<String> inclList = new ArrayList<String>(list.size());
//                    for(Object object : list)
//                    {
//                        if(RolapUtil.sqlNullValue == object || null == object)
//                        {
//                            inclList.add(MolapCommonConstants.MEMBER_DEFAULT_VAL);
//                        }
//                        else
//                        {
//                            inclList.add(object.toString());
//                        }
//                    }
//                    query.addFilter(((MondrianDef.Column)columns[i].getExpression()).name, inclList);
//                }
//            }
//        }
//
//        for(int i = 0;i < spec.getMeasureCount();i++)
//        {
//            RolapStar.Measure msr = spec.getMeasure(i);
//            Measure measure = null;
//            /*
//             * This added for fixing the defect "Add count functionality does not work if there is default count function in schema"
//             */
//            if(msr.getExpression() == null)
//            {
//                measure = cube.getMeasures(factTable).get(0);
//                measure = measure.getCopy();
//                measure.setAggName(MolapCommonConstants.COUNT);
//            }
//            else
//            {
//                measure = cube.getMeasure(factTable, msr.getName());
//                if(spec.countOnly)
//                {
//                    measure = measure.getCopy();
//                    measure.setAggName(MolapCommonConstants.COUNT);
//                }
//
//            }
//            
//            query.addAggColumn(measure.getColName(), measure.getAggName());
//        }
//
//        return query.toString();
//    }
//    
//    /**
//     * It executes the query and returns the iterartor
//     * @param query
//     */
//    public MolapResultHolder executeQuery(String query)
//    {
//        MolapSQLQuery sqlQuery = MolapSQLQuery.parseQuery(query);
//        
//        String factTable = sqlQuery.getTables().get(0);
//        MolapMetadata metadata = MolapMetadata.getInstance();
//        String cubeUniqueName = sqlQuery.getSchemaName()+SEPERATOR+sqlQuery.getCubeName();
//        Cube cube = metadata.getCube(cubeUniqueName);
//        
//        List<String> selects = sqlQuery.getSelects();
//        Map<String, List<String>> predMap = sqlQuery.getPred();
//        List<Measure> msrs = new ArrayList<Measure>();
//        Map<Dimension, MolapFilterInfo> pred = new HashMap<Dimension, MolapFilterInfo>();
//        List<String> colHeaders = new ArrayList<String>();
//        for(int i = 0;i < selects.size();i++)
//        {
//            String col = selects.get(i);
//            Dimension dimension = cube.getDimension(col, factTable);
//            if(dimension == null)
//            {
//                continue;
//            }
//            dimension = dimension.getDimCopy();
//            dimension.setActualCol(true);
//            dimensionsList.add(dimension);
//            if(dimension.isHasNameColumn())
//            {
//                colHeaders.add(dimension.getName() +" (key)");
//            }
//            colHeaders.add(dimension.getName());
//            types.add(dimension.getDataType());
//        }
//        
//        for(Entry<String, List<String>> entry : predMap.entrySet())
//        {
//            List<String> list = entry.getValue();
//            if(list != null && list.size() > 0)
//            {
//
//                List<String> inclList = new ArrayList<String>(list.size());
//                for(Object object : list)
//                {
//                    if(RolapUtil.sqlNullValue == object || null == object)
//                    {
//                        inclList.add(addBraces(MolapCommonConstants.MEMBER_DEFAULT_VAL));
//                    }
//                    else
//                    {
//                        inclList.add(addBraces(object.toString()));
//                    }
//                }
//                Dimension dimension = cube.getDimension(entry.getKey().trim(), factTable);
//                dimension = dimension.getDimCopy();
//                dimension.setActualCol(true);
//                pred.put(dimension, new MolapFilterInfo(null, inclList));
//            }
//        }
//        
//        Map<String, String> aggs = sqlQuery.getAggs();
//
//        for(Entry<String, String> entry : aggs.entrySet())
//        {
//            Measure measure = getMeasureFromCol(entry.getKey(), entry.getValue(), cube.getMeasures(factTable));
//            //Coverity fix added null check
//            if( null != measure)
//            {
//                types.add(measure.getDataType());
//                msrs.add(measure);
//                colHeaders.add(measure.getName());
//            }
//        }
//
//        try
//        {
//
//            iter = new MolapResultHolder(types);
//            iter.setColHeaders(colHeaders);
//            List<Dimension> dimTables = cube.getDimensions(factTable);
//            
//            RolapConnection.THREAD_LOCAL.get().put(RolapConnection.SCHEMA_NAME, sqlQuery.getSchemaName());
//            RolapConnection.THREAD_LOCAL.get().put(RolapConnection.CUBE_NAME, sqlQuery.getCubeName());
//
//            MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimTables, dataSource);
//            // For segment query always the columns should be in order as per
//            // schema file
//            // Collections.sort(dims, new DimensionComparator());
//            // Collections.sort(msrs, new DimensionComparator());
//            delegateToExecutor(
//                    cube,
//                    iter,
//                    factTable,
//                    dimensionsList.toArray(d),
////                    dimensionsList.size() == 0 && msrs.size() == 1
////                            && msrs.get(0).getAggName().equals(MolapCommonConstants.COUNT) ? msrs :
//                            msrs, pred, false, executor,false,5000,true,false,new ArrayList<Measure>(),null,false,false,false,false);
//            accessors.clear();
//            for(Type type : guessTypes())
//            {
//                accessors.add(createAccs(accessors.size(), type));
//            }
//
//        }
//        catch (MemoryLimitExceededException e) 
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//            throw e;
//        }
//        catch(Exception e)
//        {
//
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//        return iter;
//    }
//    
//    
//    /**
//     * It finds the Measure with same column name and agg name.
//     * @param colName
//     * @param aggName
//     * @param msrs
//     * @return
//     */
//    private Measure getMeasureFromCol(String colName, String aggName, List<Measure> msrs)
//    {
//        Measure measure = null;
//        Measure msrWithSameCol = null;
//        
//        for(Measure msr : msrs)
//        {
//            if(msr.getColName() != null && msr.getColName().equals(colName))
//            {
//                msrWithSameCol = msr;
//                if(msr.getAggName().equals(aggName))
//                {
//                    measure = msr;
//                    break;
//                }
//            }
//            
//        }
//        
//        /**
//         * If Measure with same col name found and agg name not found then add the current aggregator and return
//         */
//        if(measure == null && msrWithSameCol != null)
//        {
//            measure = msrWithSameCol.getCopy();
//            measure.setAggName(aggName);
//        }
//        
//        return measure;
//    }
//    
//    
//    
//    /**
//     * Project Name NSE V3R7C00 
//     * Module Name : MOLAP
//     * Author :C00900810
//     * Created Date :25-Jun-2013
//     * FileName : MolapStatement.java
//     * Class Description : 
//     * Version 1.0
//     */
//    private class DimensionComparatorSchemaBased implements Comparator<Dimension>
//    {
//        public int compare(Dimension dim1, Dimension dim2)
//        {
//            int ordinal1 = dim1.getSchemaOrdinal();
//            int ordinal2 = dim2.getSchemaOrdinal();
//            return ordinal1 - ordinal2;
//        }
//    }
//
//
//    /**
//     * It provides count of number of dimension members
//     * 
//     * @param dim
//     * @param star
//     * 
//     */
//    public void executeCount(String dim, RolapStar star)
//    {
//        try
//        {
//            String factTable = star.getFactTable().getTableName();
//            MolapMetadata metadata = MolapMetadata.getInstance();
//            //
//            String cubeUniqueName = star.getSchema().getName()+SEPERATOR+star.getCubeName();
//            Cube cube = metadata.getCube(cubeUniqueName);
//            types.add(SqlStatement.Type.INT);
//            iter = new MolapResultHolder(types);
//
//            List<Dimension> dimTables = cube.getDimensions(factTable);
//            //
//            MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimTables, dataSource);
//            executor.executeDimensionCount(cube.getDimension(dim, factTable), iter);
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//    }
//    
//    
//    /**
//     * It provides count of number of dimension members
//     * 
//     * @param dim
//     * @param star
//     * 
//     */
//    public void executeCount(String dim, String schemaName)
//    {
//        try
//        {
//            String cubeName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.CUBE_NAME);
//            MolapMetadata metadata = MolapMetadata.getInstance();
//            String cubeUniqueName = schemaName+'_'+cubeName;
//            Cube cube = metadata.getCube(cubeUniqueName);
//            String factTable = cube.getFactTableName();
//           
//            //String factTable = star.getFactTable().getTableName();
//           
//
//            
//            types.add(SqlStatement.Type.INT);
//            iter = new MolapResultHolder(types);
//            //
//            List<Dimension> dimTables = cube.getDimensions(factTable);
//            //
//            MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimTables, dataSource);
//            executor.executeDimensionCount(cube.getDimension(dim, factTable), iter);
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//    }
//
//    /**
//     * For the given aggregation table, it will provide the number of rows in
//     * it. It provides the size of the agg table
//     * 
//     * @param table
//     * @param star
//     * 
//     */
//    public void executeAggCount(String table, RolapStar star)
//    {
//        try
//        {
//            types.add(SqlStatement.Type.INT);
//            iter = new MolapResultHolder(types);
//            MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(null, dataSource,
//                    star.getSchema().getName(), star.getCubeName());
//            executor.executeAggTableCount(table, iter);
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//    }
//
//    /**
//     * Executes to get the tuple members from either fact table or aggregation
//     * table;
//     * 
//     * @param cube
//     * @param cols
//     * @param constraints
//     * @param msrs
//     * @param aggStar
//     *            will be null if needs to be got from fact table
//     * @param properties
//     * 
//     */
//    public void execute(Cube locCube, List<String> cols, Map<String, MolapFilterInfo> constraints, List<Measure> msrs,
//            AggStar aggStar,List<Measure> calcMsrs, boolean properties,Map<Measure, MeasureFilterModel[]> msrFilterConstraints,boolean readingMembers,boolean inludeTupleFilters)
//    {
//        boolean isAggTable = false;
//        try
//        {
//            // cube.getHierarchies()
//            String factTable = null;
//            if(aggStar != null && MolapExecutorFactory.getMolapExecutor(null, dataSource).executeTableCount(aggStar.getFactTable().getName()) > 0)
//            {
//                factTable = aggStar.getFactTable().getName();
//                List<Measure> aggMeasures = locCube.getMeasures(factTable);
//                List<Measure> updatedMsrs = new ArrayList<MolapMetadata.Measure>();
//                isAggTable = true;
//                for(Measure measure  : msrs)
//                {
//                    boolean found = false;
//                    for(Measure aggMsr : aggMeasures)
//                    {
//                        if(measure.getName().equals(aggMsr.getName()))
//                        {
//                            updatedMsrs.add(aggMsr);
//                            found = true;
//                            break;
//                        }
//                    }
//                    if(!found)
//                    {
//                        isAggTable = false; 
//                        factTable = locCube.getFactTableName();
//                        break;
//                    }
//                }
//                if(isAggTable)
//                {
//                    msrs = updatedMsrs;
//                }
//            }
//            else
//            {
//                factTable = locCube.getFactTableName();
//            }
//
//            // Cube locCube = metadata.getCube(cube.getSchema().getName());
//            // Map<String, Integer> measureOffsetMap =
//            // getMeasureOffsets(cube.getSchema().getXMLSchema());
//            // List<Dimension> dims = new ArrayList<Dimension>();
//            Map<Dimension, MolapFilterInfo> pred = new HashMap<Dimension, MolapFilterInfo>();
//
//            for(int i = 0;i < cols.size();i++)
//            {
//                String colName = cols.get(i);
//                String[] columnDimeHierNames = colName.split("#");
//                String dimHierName = columnDimeHierNames[1];
//                colName = columnDimeHierNames[0];
//                String[] dimHIerSplitted = dimHierName.split(",");
//                Dimension dimension = locCube.getDimensionByUniqueDimensionAndHierName(colName, factTable,
//                        dimHIerSplitted[0], dimHIerSplitted[1]);
////                Dimension dimension = locCube.getDimensionByUniqueName(colName, factTable);
//                /**
//                 * Fortify Fix: NULL_RETURNS
//                 */
//                if(null != dimension)
//                {
//                    dimensionsList.add(dimension);
//                    types.add(dimension.getDataType());
//    
//                    // Add types for level properties also
//                    if(properties)
//                    {
//                        if(dimension.hasOrdinalCol())
//                        {
//                            types.add(SqlStatement.Type.INT);
//                        }
//                        for(SqlStatement.Type type : dimension.getPropertyTypes())
//                        {
//                            types.add(type);
//                        }
//                    }
//                }
//
//            }
//
//            for(Measure msr : msrs)
//            {
//                if(msr!=null)
//                {
//                    types.add(msr.getDataType());
//                }
//            }
//
//            for(Map.Entry<String, MolapFilterInfo> entri : constraints.entrySet())
//            {
//                pred.put(locCube.getDimensionByUniqueName(entri.getKey(), factTable), entri.getValue());
//            }
//           
//            iter = new MolapResultHolder(types);
//
//            List<Dimension> dimTables = locCube.getDimensions(factTable);
//
//            MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimTables, dataSource);
//
//            String property = locCube.getAutoAggregateType();
//            boolean isAutoAgg=false;
//            if(MolapCommonConstants.MOLAP_AUTO_TYPE_VALUE.equalsIgnoreCase(property)
//                    || MolapCommonConstants.MOLAP_MANUAL_TYPE_VALUE.equalsIgnoreCase(property))
//            {
//                isAutoAgg = true;
//            }
//            delegateToExecutor(locCube, iter, factTable, dimensionsList.toArray(d),
//                    isAggTable ? getUpdatedMeasureFor(msrs, isAggTable,isAutoAgg) : msrs, pred, properties, executor, true, -1,
//                    false, isAggTable, calcMsrs, msrFilterConstraints,readingMembers,inludeTupleFilters,false, false);
//            accessors.clear();
//            for(Type type : guessTypes())
//            {
//                accessors.add(createAccs(accessors.size(), type));
//            }
//        }
//        catch (MemoryLimitExceededException e) 
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//            throw e;
//        }
//        catch(ResourceLimitExceededException r)
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,r,r.getMessage());
//            throw r;
//        }
//        catch(Exception e)
//        {
//            if(e.getCause() instanceof ResourceLimitExceededException )
//            {
//                ResourceLimitExceededException r = (ResourceLimitExceededException)e.getCause(); 
//                LOGGER.error(
//                        MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,r,r.getMessage());
//                throw r;
//            }
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//
//    }
//
//    private List<Measure>getUpdatedMeasureFor(List<Measure> actualMsr, boolean isAgg, boolean isAutoAgg)
//    {
//        List<Measure> newMsr = new ArrayList<MolapMetadata.Measure>(actualMsr.size());
//        for(int i = 0;i < actualMsr.size();i++)
//        {
//            Measure copy = actualMsr.get(i);
//            if(copy.getAggName().equals(MolapCommonConstants.COUNT) && isAgg)
//            {
//                copy = copy.getCopy();
//                copy.setAggName(MolapCommonConstants.SUM);
//            }
//            else if(copy.getAggName().equals(MolapCommonConstants.DISTINCT_COUNT) && isAgg && !isAutoAgg)
//            {
//                copy = copy.getCopy();
//                copy.setAggName(MolapCommonConstants.SUM);
//            }
//            newMsr.add(copy);
//        }
//        return newMsr;
//    }
//    /**
//     * @param columnContraints
//     * @param hName
//     * @param levels
//     * @param aggStar
//     * @param properties
//     * @param cubeUniqueName 
//     * @param level
//     */
//    public void execute(Map<String, MolapFilterInfo> columnContraints, String hName, Level[] levels, AggStar aggStar,
//            boolean properties, String cubeUniqueName,Map<Measure, MeasureFilterModel[]> msrFilterConstraints, boolean readingMembers,RolapLevel... level)
//    {
//        //
//        try
//        {
//            Cube locCube = null;
//            if(aggStar != null && MolapExecutorFactory.getMolapExecutor(null, dataSource).executeTableCount(aggStar.getFactTable().getName()) > 0)
//            {
//                //
//                String factTable = aggStar.getFactTable().getName();
//                MolapMetadata metadata = MolapMetadata.getInstance();
//                //String cubeUniqueName = aggStar.getStar().getSchema().getName()+SEPERATOR+aggStar.getStar().getCubeName();
//                locCube = metadata.getCube(cubeUniqueName);
//
//                // List<Dimension> dimensions = new ArrayList<Dimension>();
//                Map<Dimension, MolapFilterInfo> pred = new HashMap<Dimension, MolapFilterInfo>();
//                //
//                extractTypes(properties, locCube, factTable, level);
//                
//                //
//                for(Map.Entry<String, MolapFilterInfo> entry : columnContraints.entrySet())
//                {
//                    MolapFilterInfo p = entry.getValue();
//                    if(p.getExcludedMembers().size() > 0 || p.getIncludedMembers().size() > 0)
//                    {
//                        String colName = entry.getKey();
//
//                        if(colName.contains("#"))
//                        {
//                            String[] columnDimeHierNames = colName.split("#");
//                            String dimHierName = columnDimeHierNames[1];
//                            colName = columnDimeHierNames[0];
//                            String[] dimHIerSplitted = dimHierName.split(",");
//                            pred.put(locCube.getDimensionByUniqueDimensionAndHierName(columnDimeHierNames[0], factTable,
//                                    dimHIerSplitted[0], dimHIerSplitted[1]), entry.getValue());
//                        }
//                        else
//                        {
//                            pred.put(locCube.getDimensionByUniqueName(entry.getKey(), factTable), entry.getValue());
//                        }
////                        pred.put(locCube.getDimensionByUniqueName(entry.getKey(), factTable), p);
//                    }
//                }
//                
//                //
//                iter = new MolapResultHolder(types);
//
//                List<Dimension> dimTables = locCube.getDimensions(factTable);
//                //
//                MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimTables, dataSource);
//
//                delegateToExecutor(locCube, iter, factTable, dimensionsList.toArray(d),
//                        new ArrayList<Measure>(), pred, properties, executor,true,-1,false,true,new ArrayList<Measure>(),msrFilterConstraints,readingMembers,false,false,false);
//            }
//            else
//            {
//                List<Integer> dims = new ArrayList<Integer>();
//                // List<Dimension> dimNames = new ArrayList<Dimension>();
//                boolean isAll = false;
//                int depth = 0;
//                MolapMetadata metadata = MolapMetadata.getInstance();
//                for(int i = 0;i < levels.length;i++)
//                {
//                    RolapLevel level3 = (RolapLevel)levels[i];
//                    if(!level3.isAll())
//                    {
//                        
//                        depth++;
//                        locCube = metadata.getCube(cubeUniqueName);
//                        int keyOrdinal = isAll ? i - 1 : i;
//                        Dimension dimension = new Dimension(((MondrianDef.Column)level3.getKeyExp()).name,
//                                keyOrdinal, level3.getName(),locCube);
//                        dimension.setLevelType(level3.getLevelType());
//                        //Coverity fix, check null else by default 32
//                        Dimension dimesnsion = locCube.getDimensionByLevelName(level3.getDimension().getName(),level3.getHierarchy().getSubName(),level3.getName());
//                        if(dimesnsion == null)
//                        {
//                            dimesnsion = locCube.getDimensionByUniqueName(((MondrianDef.Column)level3.getKeyExp()).table+'_'+((MondrianDef.Column)level3.getKeyExp()).name, locCube.getFactTableName());
//                        }
//                        if( null != dimesnsion)
//                        {
//                            dimension=dimesnsion.getDimCopy();
//                            dimension.setOrdinal(keyOrdinal);
//                        }
//                        dimension.setTableName(((MondrianDef.Column)level3.getKeyExp()).table);
//                        dimension.setPropertyCount(level3.getProperties().length);
//                        dimension.setDataType(MolapMetadata.makeSQLDataTye(level3.getDatatype()));
//                        dimension.setOrdinalCol(hasOrdinalColumn(level3));
//                        dimensionsList.add(dimension);
//                        for(int j = 0;j < level.length;j++)
//                        {
//                            if(level[j].getName().equals(level3.getName()))
//                            {
//                                if(isAll)
//                                {
//                                    dims.add(i - 1);
//
//                                }
//                                else
//                                {
//                                    dims.add(i);
//                                }
//                                
//                                types.add(dimension.getDataType());
//                                
//                                if(dimension.hasOrdinalCol())
//                                {
//                                    types.add(SqlStatement.Type.INT);
//                                }
//
//                                // Add types for level properties also
//                                for(RolapProperty property : level3.getProperties())
//                                {
//
//                                    types.add(MolapMetadata.makeSQLDataTye(property.getType()));
//                                }
//
//                            }
//
//                        }
//
//                        // types.add(dimension.getDataType());
//
//                    }
//                    else
//                    {
//                        isAll = true;
//                    }
//                }
//
//                
//                MolapExecutor executor = MolapExecutorFactory.getMolapExecutor(dimensionsList, dataSource);
//                int[] dimArray = getintArray(dims);
//
//                iter = new MolapResultHolder(types);
//                if(depth > 1)
//                {
//
//                    executor.executeHierarichies(hName, dimArray, dimensionsList,
//                            getConstrMap(columnContraints, levels, dimensionsList), iter);
//                }
//                else
//                {
//                    executor.executeDimension(hName, dimensionsList.get(0), dimArray,
//                            getConstrMap(columnContraints, levels, dimensionsList), iter);
//                }
//            }
//
//            accessors.clear();
//            for(Type type : guessTypes())
//            {
//                accessors.add(createAccs(accessors.size(), type));
//            }
//
//        }
//        catch (MemoryLimitExceededException e) 
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//            throw e;
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,e,e.getMessage());
//        }
//
//    }
//
//    private void extractTypes(boolean properties, Cube locCube, String factTable, RolapLevel... level)
//    {// CHECKSTYLE:OFF 
//        /*
//         * Approval ID: V3R8C10_112
//         * http://blrsvn01-rd:6801/svn/CRDU_NSE_UniBI_DOCS_SVN/NSE
//         * V300R008/NSE V300R008C10/03.SW Folder/Approval
//         * Record/Checkstyle_Approval.xlsx
//         */
//
//        for(int i = 0;i < level.length;i++)
//        {
//            RolapLevel level3 = (RolapLevel)level[i];
//            if(!level3.isAll())
//            {
//                Dimension dimension = locCube.getDimensionByUniqueName(((MondrianDef.Column)level3.getKeyExp()).table
//                        + '_' + ((MondrianDef.Column)level3.getKeyExp()).name, factTable);
//                /**
//                 * Fortify Fix: NULL_RETURNS
//                 */
//                if(null != dimension)
//                {
//                    dimensionsList.add(dimension);
//                    types.add(dimension.getDataType());
//                    //
//                    if(properties)
//                    {
//                        //
//                        if(dimension.hasOrdinalCol())
//                        {
//                            types.add(SqlStatement.Type.INT);
//                        }
//
//                        // Add types for level properties also
//                        for(SqlStatement.Type type : dimension.getPropertyTypes())
//                        {
//                            types.add(type);
//                        }
//                    }
//                }
//            }
//        }
//    }// CHECKSTYLE:ON
//
//    private int[] getintArray(List<Integer> list)
//    {
//        int[] a = new int[list.size()];
//        for(int i = 0;i < a.length;i++)
//        {
//            a[i] = list.get(i);
//        }
//        return a;
//    }
//
//    private Map<Dimension, MolapFilterInfo> getConstrMap(Map<String, MolapFilterInfo> cons, Level[] levels,
//            List<Dimension> dimList)
//    {
//
//        boolean isAll = false;
//        Map<Dimension, MolapFilterInfo> consWithIds = new HashMap<Dimension, MolapFilterInfo>(cons.size());
//        //
//        for(Map.Entry<String, MolapFilterInfo> entry : cons.entrySet())
//        {
//            String column = entry.getKey();
//            for(int j = 0;j < levels.length;j++)
//            {
//                RolapLevel level2 = (RolapLevel)levels[j];
//                //
//                if(!level2.isAll())
//                {
//                    if((((MondrianDef.Column)level2.getKeyExp()).table+'_'+((MondrianDef.Column)level2.getKeyExp()).name).equals(column))
//                    {
//                        if(isAll)
//                        {
//                            //
//                            consWithIds.put(getDimension(column, dimList), entry.getValue());
//                        }
//                        else
//                        {
//                            consWithIds.put(getDimension(column, dimList), entry.getValue());
//                        }
//                    }
//                }
//                else
//                {
//                    isAll = true;
//                }
//
//            }
//        }
//        return consWithIds;
//    }
//
//    private Dimension getDimension(String col, List<Dimension> dimList)
//    {
//        for(Dimension dimension : dimList)
//        {
//            if((dimension.getTableName()+'_'+dimension.getColName()).equals(col))
//            {
//                return dimension;
//            }
//        }
//        return null;
//    }
//
//    private Accessor createAccs(int column, Type type)
//    {
//        final int columnPlusOne = column + 1;
//        //
//        switch(type)
//        {
//        case OBJECT:
//            return new Accessor()
//            {
//                public Object get() throws SQLException
//                {
//                    return iter.getObject(columnPlusOne);
//                }
//            };
//            //
//        case STRING:
//            return new Accessor()
//            {
//                public Object get() throws SQLException
//                {
//                    return iter.getObject(columnPlusOne);
//                }
//            };
//            //
//        case INT:
//            return new Accessor()
//            {
//                public Object get() throws SQLException
//                {
//                    final int val = iter.getInt(columnPlusOne);
//                    if(val == 0 && iter.wasNull())
//                    {
//                        return null;
//                    }
//                    return val;
//                }
//            };
//            //
//        case LONG:
//            return new Accessor()
//            {
//                public Object get() throws SQLException
//                {
//                    final long val = iter.getLong(columnPlusOne);
//                    if(val == 0 && iter.wasNull())
//                    {
//                        return null;
//                    }
//                    return val;
//                }
//            };
//            //
//        case DOUBLE:
//            return new Accessor()
//            {
//                public Object get() throws SQLException
//                {
//                    final double val = iter.getDouble(columnPlusOne);
//                    if(Double.compare(val, 0) == 0 && iter.wasNull())
//                    {
//                        return null;
//                    }
//                    return val;
//                }
//            };
//        default:
//            throw Util.unexpected(type);
//        }
//    }
//
//    public MolapResultHolder getIterator()
//    {
//        return iter;
//    }
//
//    public void updateIterator(MolapResultHolder iterator)
//    {
//        this.iter = iterator;
//    }
//
//    @Override
//    public List<Type> guessTypes() throws SQLException
//    {
//        return types;
//    }
//
//    @Override
//    public List<Accessor> getAccessors() throws SQLException
//    {
//        return accessors;
//    }
//
//    @Override
//    public void close()
//    {
//        // TODO Auto-generated method stub
//        // super.close();
//    }
//
//    public List<Dimension> getDimensionsList()
//    {
//        return dimensionsList;
//    }
//
//    public List<Dimension> getSortedDimensionsList()
//    {
//        ArrayList<Dimension> sortedDimensions = new ArrayList<MolapMetadata.Dimension>(dimensionsList.size());
//        for(Dimension dimension : dimensionsList)
//        {
//            sortedDimensions.add(dimension);
//        }
//        Collections.sort(sortedDimensions, new DimensionComparatorSchemaBased());
//        return sortedDimensions;
//    }
//
//    /**
//     * 
//     */
//    private static boolean assignOrderKeys = MondrianProperties.instance().CompareSiblingsByOrderKey.get();
//    /**
//     * Check whether to consider Ordinal column separately if it is configured.
//     */
//    public boolean hasOrdinalColumn(RolapLevel level)
//    {
//        return (assignOrderKeys && (!level.getOrdinalExp().equals(level.getKeyExp())));
//    }
//    /**
//     *  This method added just to delegate to the correct method based on measure filter and TopN
//     * @throws ExecutionException 
//     * @throws Exception 
//     * @throws KeyGenException 
//     * @throws IOException 
//     */
//    private void delegateToExecutor(Cube cube, MolapResultHolder hIterator, String factTable, Dimension[] dims, List<Measure> msrs,
//            Map<Dimension, MolapFilterInfo> constraints, boolean properties, MolapExecutor molapExecutor,
//            boolean isFilterInHierGroups,int rowLimit,boolean onlyNameColumnReq,boolean isAggTable,List<Measure> calcMsrs,
//            Map<Measure, MeasureFilterModel[]> msrFilters,boolean readingMembers,boolean inludeTupleFilters,boolean segmentQuery, boolean isCompoundPredicate) throws Exception
//    {
//        
//        Query query= (Query)RolapConnection.THREAD_LOCAL.get().get("QUERY_OBJ");
//        MolapQueryExecutorModel executorModel = new MolapQueryExecutorModel();
//        executorModel.setCube(cube);
//        executorModel.sethIterator(hIterator);
//        executorModel.setFactTable(factTable);
//        executorModel.setDims(dims);
//        executorModel.setMsrs(msrs);
//        executorModel.setConstraints(constraints);
//        executorModel.setProperties(properties);
//        executorModel.setFilterInHierGroups(isFilterInHierGroups);
//        executorModel.setRowLimit(rowLimit);
//        executorModel.setOnlyNameColumnReq(onlyNameColumnReq);
//        executorModel.setAggTable(isAggTable);
//        executorModel.setRangeFilter(FilterFunDef.RANGE_THREAD_LOCAL.get());
//        executorModel.setCalcMeasures(calcMsrs);
//        executorModel.setComputePredicatePresent(isCompoundPredicate);
//        MeasureFilterModel[][] measureFilterArrayInMDXQueryOrder = getMeasureFilterArrayInMDXQueryOrder(msrFilters, msrs,calcMsrs,executorModel);
//        executorModel.setConstraintsAfterTopN(new HashMap<MolapMetadata.Dimension, MolapFilterInfo>());
//        TopNModel topNModel = null;
//        TopNModel tempTopNModel = null;
//        if(measureFilterArrayInMDXQueryOrder!= null)
//        {
//            GroupMeasureFilterModel measureFilterModel = new GroupMeasureFilterModel(measureFilterArrayInMDXQueryOrder, MeasureFilterGroupType.OR);
//            List<GroupMeasureFilterModel> filterModels = new ArrayList<GroupMeasureFilterModel>();
//            filterModels.add(measureFilterModel);
//            executorModel.setMsrFilterModels(filterModels);
//        }
//        FilterFunDef.RANGE_THREAD_LOCAL.set(false);
//        MolapQuery molapQuery = null;
//        if(query != null)
//        {
//            molapQuery = (MolapQuery)query.getAttribute("MOLAP_QUERY");
//        }
//        if(enableTopNopt && molapQuery != null)
//        {
//            String cubeName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.CUBE_NAME);
//            String schemaName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.SCHEMA_NAME);
//            MolapQueryModel model = MolapQueryParseUtil.parseMolapQuery(molapQuery, schemaName, cubeName);
//            List<Dimension> fillWithParents = null;
//            if(model.isPaginationRequired())
//            {
//                if(segmentQuery)
//                {
//                    validateDimsComingFromSegmentQuery(dims,model.getQueryDims());
//                }
//               fillWithParents = fillWithParents(model.getQueryDims(),cube.getDimensions(factTable),dims,false);
//            }
//            else
//            {
//                 fillWithParents = fillWithParents(model.getQueryDims(),cube.getDimensions(factTable),dims,true);
//            }
//           // fillWithParents = fillWithParents(model.getQueryDimsRows(),cube.getDimensions(factTable),dims,true);
//            List<Dimension> fillWithParentsWithOutQueryDimsRows = fillWithParents(model.getQueryDimsRows(),cube.getDimensions(factTable),dims,false);
//            List<Dimension> fillWithParentsWithOutQueryDimsCols = fillWithParents(model.getQueryDimsCols(),cube.getDimensions(factTable),dims,false);
//            List<Dimension> fillWithParentsWithOutQueryDims = fillWithParents(model.getQueryDims(),cube.getDimensions(factTable),dims,false);
//            executorModel.setActualQueryDims(fillWithParents.toArray(new Dimension[fillWithParents.size()]));
//            executorModel.setActualDimsRows(fillWithParentsWithOutQueryDimsRows.toArray(new Dimension[fillWithParentsWithOutQueryDimsRows.size()]));
//            executorModel.setActualDimsCols(fillWithParentsWithOutQueryDimsCols.toArray(new Dimension[fillWithParentsWithOutQueryDimsCols.size()]));
//            executorModel.setAnalyzerDims(fillWithParentsWithOutQueryDims.toArray(new Dimension[fillWithParentsWithOutQueryDims.size()]));
//            executorModel.setPaginationRequired(model.isPaginationRequired());
//            String property = cube.getAutoAggregateType();
//            boolean isAutoAgg=false;
//            if(MolapCommonConstants.MOLAP_AUTO_TYPE_VALUE.equalsIgnoreCase(property)
//                    || MolapCommonConstants.MOLAP_MANUAL_TYPE_VALUE.equalsIgnoreCase(property))
//            {
//                isAutoAgg = true;
//            }
//            executorModel.setActualMsrs(getUpdatedMeasureFor(model.getMsrs(), isAggTable,isAutoAgg));
//            if(!model.isAnalyzer())
//            {
//               model.setConstraintsAfterTopN(addConstraintstoExecutorModel(model.getConstraintsAfterTopN()));
//            }
//            executorModel.setUseNonVisualTotal(model.isGrandTotalForAllRows());
//            executorModel.setConstraintsAfterTopN(model.getConstraintsAfterTopN());
////            executorModel.setUseNonVisualTotal(model.isUseNonVisualTotal());
//            //TODO : Need to handle aggregation tables on Pagination.
//            if(model.isPaginationRequired() && executorModel.isCountFunction())
//            {
//                if(!factTable.equals(cube.getFactTableName()))
//                {
//                    factTable = cube.getFactTableName();
//                    executorModel.setFactTable(factTable);
//                    dims = updateDimensions(dims, cube, factTable);
//                    msrs = updateMeasures(msrs.toArray(new Measure[msrs.size()]), cube, factTable);
//                    constraints = updateConstraints(constraints, cube, factTable);
//                    ((InMemoryQueryExecutor)molapExecutor).setDimTables(cube.getDimensions(factTable));
//                    executorModel.setDims(dims);
//                    executorModel.setMsrs(msrs);
//                    executorModel.setConstraints(constraints);
//                    executorModel.setAggTable(false);
//                    executorModel.setActualQueryDims(updateDimensions(executorModel.getActualQueryDims(), cube, factTable));
//                    executorModel.setActualMsrs(updateMeasures(model.getMsrs().toArray(new Measure[model.getMsrs().size()]), cube, factTable));
//                }
//            }
//            executorModel.setRowRange(model.getRowRange());
//            executorModel.setIsSliceFilterPresent(model.isSliceFilterPresent());
//            executorModel.setQueryId(model.getQueryId());
//            List<Measure> allCalMeasure = new ArrayList<MolapMetadata.Measure>();
//            fillCalcMeasures(model, allCalMeasure, query,model.getMsrs(),cube,factTable,executorModel,isAggTable,isAutoAgg);
//            executorModel.setActualCalMeasure(allCalMeasure);
//            fillCalcMeasures(model, calcMsrs, query,msrs,cube,factTable,executorModel,isAggTable,isAutoAgg);
//            boolean allDimsPresentInMolapQuery = isAllDimsPresentInMolapQuery(executorModel.isPaginationRequired()?fillWithParents.toArray(new Dimension[fillWithParents.size()]):dims, model.getQueryDims());
//            if(!readingMembers && allDimsPresentInMolapQuery && !(executorModel.getActualQueryDims().length != executorModel.getDims().length))
//            {
//                topNModel = model.getTopNModel();
//            }
//            if((model.getMsrFilter() != null || topNModel != null) && !readingMembers && allDimsPresentInMolapQuery && !model.isGrandTotalForAllRows())
//            {
//                if(topNModel != null)
//                {
//                    topNModel.setBreakHierarchy(((MolapQueryImpl)molapQuery).isBreakHierarchyTopN());
//                }
//                    
//                if(!model.pushTopN())
//                {
//                processTopN(executorModel.getActualQueryDims(), msrs, executorModel, model, topNModel,calcMsrs,executorModel.isPaginationRequired());
//            }
//            }
//            if(model.getMsrFilter() != null && !model.isGrandTotalForAllRows() && !readingMembers && !model.isSubTotal())
//            {
//                updateExecutorsforMsrs(executorModel.getActualQueryDims(), msrs, executorModel, model, calcMsrs,executorModel.isPaginationRequired());
//            }
//            
//            byte[] sortOrder= null;
//            
//            if(!executorModel.isPaginationRequired())
//            {
//                sortOrder = compareDimsAndGetTheSortOrder(dims, model.getQueryDims(), model.getDimSortTypes());
//            }
//            else
//            {
//                sortOrder= compareDimsAndGetTheSortOrderForPagination(executorModel.getActualQueryDims(),  model.getDimSortTypes());
//            }
//            
//            executorModel.setSortOrder(sortOrder);
//            //Add all the filters from model object to executor model incase of pagination.
//            if(executorModel.isPaginationRequired())
//            {
//                addConstraintstoExecutorModel(model.getConstraints(), constraints,cube,isFilterInHierGroups,factTable);
//            }
//            boolean optimizeCons = Boolean.parseBoolean(MolapProperties.getInstance().getProperty("molap.optimize.constraints","true"));
//            //Remove the unnecessary measures which are not there in model.It will be helpful for cache rollup optimization.
//            
//            if(optimizeCons && !readingMembers && !model.isRelativefilter() && !executorModel.isComputePredicatePresent())
//            {
//               if(!(segmentQuery && executorModel.isPaginationRequired()))
//                {
//                    removeUnnecessaryConstraints(constraints, model.getConstraints(), model.isGrandTotalForAllRows(),
//                            isFilterInHierGroups, executorModel, factTable, model.isSubTotal());
//               }
//                constraints = executorModel.getConstraints();
//                
//            }
//            executorModel.setSegmentCallWithFilterPresent(!executorModel.isPaginationRequired() && segmentQuery
//                    && !model.isSubTotal() && !model.isGrandTotalForAllRows() && constraints.size() > 0
//                    && cube.isFullyDenormalized() ? true : false);
//            if(executorModel.isCountFunction() && executorModel.isPaginationRequired())
//            {
//                //Incase of count function, if only count is selected by user then add the first measure as mondrian sends like that in the following
//                //queries
//                if(msrs.size() == 1)
//                {
//                    Measure countMsr = msrs.get(0);
//                    msrs.set(0,cube.getMeasures(factTable).get(0));
//                    msrs.add(countMsr);
//                }
//            }
//            tempTopNModel = getTopNModel(executorModel, topNModel, molapQuery, model);
//        }
//        //Log the query string
////        logQueryString(factTable, dims, msrs, constraints, properties, calcMsrs, msrFilters);
//        
//        // Added Function for Fixing sourcemonitor.
//        
//        skipOverloadAndStartExecution(hIterator, molapExecutor, executorModel, tempTopNModel);
//    }
//
//   
//
//    private TopNModel getTopNModel(MolapQueryExecutorModel executorModel, TopNModel topNModel, MolapQuery molapQuery,
//            MolapQueryModel model)
//    {
//        TopNModel tempTopNModel = topNModel;
//        if(executorModel.isPaginationRequired() && null==tempTopNModel)
//        {
//            TopNModel tempModel=model.getTopNModel();
//            if(tempModel != null)
//            {
//                tempModel.setBreakHierarchy(((MolapQueryImpl)molapQuery).isBreakHierarchyTopN());
//                tempTopNModel=tempModel;
//            }
//        }
//        return tempTopNModel;
//    }
//    
//
//private void validateDimsComingFromSegmentQuery(Dimension[] dims, List<Dimension> queryDims) 
//    {
//       boolean isConsistent=true;
//       for(Dimension dim:dims)
//       {
//           if(!QueryExecutorUtil.containWithSameColumnName(dim,queryDims))
//           {
//               
//               isConsistent=false;
//               break;
//   
//           }
//           
//       }
//       
//       if(!isConsistent)
//       {
//           dims=getDimFromQueryDims(queryDims,dims);
//           if(dims.length>queryDims.size())
//           {
//               addExtraDimToQueryDims(queryDims,dims);
//           }
//           
//       }
//    }
////   private void  validateQueryDimsAndDimsComingFromSegmentQuery(Dimension[] dims, List<Dimension> queryDims) 
////   {
////      boolean isConsistent=true; 
////      for(Dimension dim:dims)
////      {
////          if(!QueryExecutorUtil.containWithSameColumnName(dim,queryDims))
////          {
////              isConsistent=false;
////              break;
////          }
////      }
////      if(!isConsistent)
////      {
////          if(dims.length>queryDims.size())
////          {
////              addExtraDimToQueryDims(queryDims,dims);
////          }
////      }
////   }
//    private void addExtraDimToQueryDims(List<Dimension> queryDims, Dimension[] dims)
//    {
//        List<Dimension> listOfExtraDim = new ArrayList<Dimension>(15);
//        for(Dimension dim : dims)
//        {
//            if(!QueryExecutorUtil.containWithSameColumnName(dim, queryDims))
//            {
//                listOfExtraDim.add(dim);
//            }
//        }
//        queryDims.addAll(listOfExtraDim);
//        
//    }
//
//    private Dimension[] getDimFromQueryDims(List<Dimension> queryDims, Dimension[] dims)
//    {
//        for(Dimension queryDim : queryDims)
//        {
//            for(int i = 0;i < dims.length;i++)
//            {
//                if(dims[i].getDimName().equals(queryDim.getDimName())
//                        && dims[i].getColName().equals(queryDim.getColName()))
//
//                {
//                    dims[i] = queryDim;
//                    break;
//                }
//
//            }
//        }
//        
//        return dims;
//
//    }
//
//    /**
//     * 
//     * @param hIterator
//     * @param molapExecutor
//     * @param executorModel
//     * @param topNModel
//     * @throws InterruptedException
//     * @throws IOException
//     * @throws KeyGenException
//     * 
//     */
//    private void skipOverloadAndStartExecution(MolapResultHolder hIterator, MolapExecutor molapExecutor,
//            MolapQueryExecutorModel executorModel, TopNModel topNModel) throws InterruptedException, IOException,
//            KeyGenException
//    {
//        boolean skipOverload = Boolean.parseBoolean(MolapProperties.getInstance().getProperty("molap.skip.overload.control","false"));
//        if(!skipOverload)
//        {
//            MolapQueryExecutorTask executorTask = new MolapQueryExecutorTask(molapExecutor, executorModel);
//            
//            //If TopN, measure filter is present, call the specific method
//            try
//            {
//                MolapQueryExecutorHelper.getInstance().executeQueryTask(executorTask);
//            }
//            catch(ExecutionException e)
//            {
//                if(e.getCause() instanceof ResourceLimitExceededException)
//                {
//                    throw (ResourceLimitExceededException)e.getCause();
//                }
//            }
//            catch(LoadControlException e)
//            {
//                throw new MemoryLimitExceededException(e.getMessage());
//            }
//        }
//        else
//        {
//            molapExecutor.execute(executorModel);
//        }
//        if(topNModel != null && topNModel.isBreakHierarchy())
//        {
//            if(hIterator.getTotalRowCount()<topNModel.getCount())
//            {
//                RolapConnection.THREAD_LOCAL.get().put(RolapConnection.QUERY_TOTAL_ROW_COUNT,hIterator.getTotalRowCount());
//            }
//            else
//            {
//                RolapConnection.THREAD_LOCAL.get().put(RolapConnection.QUERY_TOTAL_ROW_COUNT,topNModel.getCount());
//            }
//        }
//        else
//        {
//            RolapConnection.THREAD_LOCAL.get().put(RolapConnection.QUERY_TOTAL_ROW_COUNT,hIterator.getTotalRowCount());
//        }
//    }
//    
//    
//    
//    private void addConstraintstoExecutorModel(Map<Dimension, MolapFilterInfo> source,Map<Dimension, MolapFilterInfo> target,Cube cube,boolean isFilterInHierGroups,String factTableName)
//    {
////        if(isFilterInHierGroups)
////        {
//////            source = getUpdateConstarintsForHierGroups(source, cube,true,factTableName);
////        }
//        for(Entry<Dimension, MolapFilterInfo> entry : source.entrySet())
//        {
//            if(target.get(entry.getKey()) == null)
//            {
//                if(isFilterInHierGroups)
//                {
//                    target.put(entry.getKey(), entry.getValue());
//                }
//                else
//                {
//                    target.put(entry.getKey(), encloseBracketsToConstarints(entry.getValue()));
//                }
//              
//            }
//        }
//    }
//    
//    
////    private Map<Dimension, MolapFilterInfo> getUpdateConstarintsForHierGroups(Map<Dimension, MolapFilterInfo> source,Cube cube,boolean appendBrackets)
////    {
////        Map<Dimension, MolapFilterInfo> temp = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>();
////        Map<String,List<MolapFilterInfo>> heirAndFilterInfoMap = new HashMap<String, List<MolapFilterInfo>>();
////        Map<String,Dimension> heirAndDimensionMap = new HashMap<String, Dimension>();
////        for(Dimension dimension :  source.keySet())
////        {
////            if(null==dimension)
////            {
////                continue;
////            }
////            String hierName = dimension.getDimName()+'_'+dimension.getHierName();
////            if(heirAndFilterInfoMap.get(hierName) != null)
////            {
////                continue;
////            }
////            List<Dimension> hierarchiesMapping = cube.getHierarchiesMapping(hierName);
////            List<MolapFilterInfo> molapFilterInfoList= new ArrayList<MolapFilterInfo>();
////            Dimension lastFilterLevelDimension = null;
////            for(Dimension entry : hierarchiesMapping)
////            {
////                MolapFilterInfo molapFilterInfo = source.get(entry);
////                if(null!=molapFilterInfo)
////                {
////                    lastFilterLevelDimension=entry;
////                    molapFilterInfoList.add(molapFilterInfo);
////                }
////            }
////            heirAndFilterInfoMap.put(hierName, molapFilterInfoList);
////            heirAndDimensionMap.put(hierName, lastFilterLevelDimension);
////        }
////        for(Entry<String, List<MolapFilterInfo>> entry :heirAndFilterInfoMap.entrySet())
////        {
////            temp.put(cube.getDimensionByLevelName(heirAndDimensionMap.get(entry.getKey()).getDimName(),
////                    heirAndDimensionMap.get(entry.getKey()).getHierName(), heirAndDimensionMap.get(entry.getKey())
////                            .getName()), getFilterInfo(entry.getValue(), appendBrackets));
////        }
////        return temp;
////       
////    }
//    
//    /*private Map<Dimension, MolapFilterInfo> getUpdateConstarintsForHierGroups(Map<Dimension, MolapFilterInfo> source,Cube cube,boolean appendBrackets, String factTableName)
//    {
//        Map<Dimension, MolapFilterInfo> temp = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>();
//        Map<String,List<MolapFilterInfo>> heirAndFilterInfoMap = new HashMap<String, List<MolapFilterInfo>>();
//        Map<String,Dimension> heirAndDimensionMap = new HashMap<String, Dimension>();
//        for(Dimension dimension :  source.keySet())
//        {
//            if(null==dimension)
//            {
//                continue;
//            }
//            String hierName = dimension.getDimName()+'_'+dimension.getHierName();
//            if(heirAndFilterInfoMap.get(hierName) != null)
//            {
//                continue;
//            }
//            List<Dimension> hierarchiesMapping = cube.getHierarchiesMapping(hierName);
//            List<MolapFilterInfo> molapFilterInfoList= new ArrayList<MolapFilterInfo>();
//            Dimension lastFilterLevelDimension = null;
//            for(Dimension entry : hierarchiesMapping)
//            {
//                MolapFilterInfo molapFilterInfo = source.get(entry);
//
//                if(null!=molapFilterInfo && !molapFilterInfo.isRestrictedMember())
//                {
//                    lastFilterLevelDimension=entry;
//                    molapFilterInfoList.add(molapFilterInfo);
//                }
//                else if(null!=molapFilterInfo && molapFilterInfo.isRestrictedMember())
//                {
//                    temp.put(entry, molapFilterInfo);
//                    continue;
//                }
//            }
//            heirAndFilterInfoMap.put(hierName, molapFilterInfoList);
//            heirAndDimensionMap.put(hierName, lastFilterLevelDimension);
//        }
//        for(Entry<String, List<MolapFilterInfo>> entry :heirAndFilterInfoMap.entrySet())
//        {
//            if(null!=heirAndDimensionMap.get(entry.getKey()))
//            {
//            Dimension dimensionByLevelName = cube.getDimensionByLevelName(heirAndDimensionMap.get(entry.getKey()).getDimName(),
//                        heirAndDimensionMap.get(entry.getKey()).getHierName(), heirAndDimensionMap.get(entry.getKey())
//                                .getName(),factTableName);
//            temp.put(dimensionByLevelName, getFilterInfo(entry.getValue(), appendBrackets, dimensionByLevelName));
//            }
//        }
//        return temp;
//       
//    }
//*/
//    /**
//     * @param dimension 
//     * @param entry
//     */
//   /* private MolapFilterInfo getFilterInfo(List<MolapFilterInfo> filterInfoList,boolean appendBrackets, Dimension dimension)
//    {
//        MolapFilterInfo newFilterInfo = new MolapFilterInfo();
//        
//        for(MolapFilterInfo filterInfo : filterInfoList)
//        {
//            if(filterInfo instanceof ContentMatchFilterInfo)
//            {
//                newFilterInfo = new ContentMatchFilterInfo();
//                break;
//            }
//        }
//        
//        int sizeInclude = getMaxSize(filterInfoList, true);
//        int sizeExclude = getMaxSize(filterInfoList,false);
//        
//        loop1:for(int i =0;i<sizeInclude;i++)
//        {
//            StringBuilder builder = new StringBuilder();
//            int size = filterInfoList.size();
//            for(int j = 0;j < size;j++)
//            {
//                MolapFilterInfo molapFilterInfoLocal = filterInfoList.get(j);
//                List<String> includedMembers = molapFilterInfoLocal.getIncludedMembers();
////                if(includedMembers.size()<i)
////                {
////                    break loop1;
////                }
//                if(i < includedMembers.size())
//                {
//                    if(appendBrackets)
//                    {
//                        builder.append('['+includedMembers.get(i)+']');
//                    }
//                    else
//                    {
//                        builder.append(includedMembers.get(i));
//                    }
//                }
//                if(j < size-1)
//                {
//                    builder.append('.');
//                }
//            }
//            String incString = builder.toString();
//            while(incString.charAt(incString.length() - 1) == '.')
//            {
//                incString = incString.substring(0, incString.length()-1);
//            }
//            newFilterInfo.getIncludedMembers().add(incString);
//        }
//        
//        loop2:for(int i =0;i<sizeExclude;i++)
//        {
//            StringBuilder builder = new StringBuilder();
//            int size = filterInfoList.size();
//            for(int j = 0;j < size;j++)
//            {
//                List<String> excludedMembers = filterInfoList.get(j).getExcludedMembers();
////                if(excludedMembers.size()<i)
////                {
////                    break loop2;
////                }
//                if(i < excludedMembers.size())
//                {
//                    if(appendBrackets)
//                    {
//                        builder.append('['+excludedMembers.get(i)+']');
//                    }
//                    else
//                    {
//                        builder.append(excludedMembers.get(i));
//                    }
//                }
//                if(j < size-1)
//                {
//                    builder.append('.');
//                }
//            }
//            String incString = builder.toString();
//            while(incString.charAt(incString.length() - 1) == '.')
//            {
//                incString = incString.substring(0, incString.length()-1);
//            }
//            newFilterInfo.getExcludedMembers().add(incString);
//        }
//
//        for(MolapFilterInfo filterInfo : filterInfoList)
//        {
//            if(filterInfo instanceof ContentMatchFilterInfo)
//            {
//                List<Dimension> hierarchiesMapping = dimension.getCube().getHierarchiesMapping(dimension.getDimName()+'_'+dimension.getHierName());
//                int indexOfDim = hierarchiesMapping.indexOf(dimension);
//                
//                ContentMatchFilterInfo info = new ContentMatchFilterInfo();
//                info.setIncludedContentMatchMembers(((ContentMatchFilterInfo)filterInfo).getIncludedContentMatchMembers());
//                info.setExcludedContentMatchMembers(((ContentMatchFilterInfo)filterInfo).getExcludedContentMatchMembers());
//                ((ContentMatchFilterInfo)newFilterInfo).getDimFilterMap().put(indexOfDim, info);
//            }
//        }
//        
//        return newFilterInfo;
//    }*/
//    
//    /**
//     * Get the max size of constraints.
//     * @param filterInfoList
//     * @param include
//     * @return
//     */
//   /* private int getMaxSize(List<MolapFilterInfo> filterInfoList,boolean include)
//    {
//        int maxSize = 0;
//        for(MolapFilterInfo filterInfo : filterInfoList)
//        {
//            int size = 0;
//            if(include)
//            {
//                size = filterInfo.getIncludedMembers().size();
//            }
//            else
//            {
//                size = filterInfo.getExcludedMembers().size();
//            }
//            if(size > maxSize)
//            {
//                maxSize = size;
//            }
//        }
//        return maxSize;
//    }*/
//    
//    private Map<Dimension, MolapFilterInfo> addConstraintstoExecutorModel(Map<Dimension, MolapFilterInfo> source)
//    {
//        Map<Dimension, MolapFilterInfo> map = new LinkedHashMap<MolapMetadata.Dimension, MolapFilterInfo>();
//        for(Entry<Dimension, MolapFilterInfo> entry : source.entrySet())
//        {
//            map.put(entry.getKey(), encloseBracketsToConstarints(entry.getValue()));
//        }
//        return map;
//    }
//    
//    private MolapFilterInfo encloseBracketsToConstarints(MolapFilterInfo filterInfo)
//    {
//        MolapFilterInfo molapFilterInfo = null;
//        if(filterInfo instanceof ContentMatchFilterInfo)
//        {
//            molapFilterInfo = new ContentMatchFilterInfo();
//            ((ContentMatchFilterInfo)molapFilterInfo).setIncludedContentMatchMembers(((ContentMatchFilterInfo)filterInfo).getIncludedContentMatchMembers());
//            ((ContentMatchFilterInfo)molapFilterInfo).setExcludedContentMatchMembers(((ContentMatchFilterInfo)filterInfo).getExcludedContentMatchMembers());
//        }
//        else
//        {
//            molapFilterInfo = new MolapFilterInfo();
//        }
//        if(filterInfo.getIncludedMembers() != null && filterInfo.getIncludedMembers().size() > 0)
//        {
//            for(String filter : filterInfo.getIncludedMembers())
//            {
//                if(filter.charAt(0) != '[' && filter.charAt(filter.length()-1) != ']')
//                {
//                    molapFilterInfo.getIncludedMembers().add('['+filter+']');
//                }
//                else
//                {
//                    molapFilterInfo.getIncludedMembers().add(filter);
//                }
//            }
//        }
//        
//        if(filterInfo.getExcludedMembers() != null && filterInfo.getExcludedMembers().size() > 0)
//        {
//            for(String filter : filterInfo.getExcludedMembers())
//            {
//                if(filter.charAt(0) != '[' && filter.charAt(filter.length()-1) != ']')
//                {
//                   molapFilterInfo.getExcludedMembers().add('['+filter+']');
//                }
//                else
//                {
//                    molapFilterInfo.getExcludedMembers().add(filter);
//                }
//            }
//        }
//        return molapFilterInfo;
//    }
//    
//    
//    private boolean isAllDimsPresentInMolapQuery( Dimension[] dims,  List<Dimension> molapDims)
//    {
//        for(int i = 0;i < molapDims.size();i++)
//        {
//            if(!QueryExecutorUtil.contain(molapDims.get(i), dims))
//            {
//                return false;
//            }
//            
//        }
//        return true;
//    }
//    
//
//    
//    private Dimension[] updateDimensions(Dimension[] dimensions,Cube cube, String factTable)
//    {
//        Dimension[] updatedDims = new Dimension[dimensions.length];
//        List<Dimension> dimensionsFact = cube.getDimensions(factTable);
//        
//        for(int i = 0;i < dimensions.length;i++)
//        {
//            
//            for(Dimension dimension : dimensionsFact)
//            {
//                if(dimensions[i].getName().equals(dimension.getName()) && dimensions[i].getDimName().equals(dimension.getDimName()) &&  
//                        dimensions[i].getHierName().equals(dimension.getHierName()))
//                {
//                    updatedDims[i] = dimension;
//                }
//            }
//            
//        }
//        return updatedDims;
//    }
//    
//    private List<Measure> updateMeasures(Measure[] measures,Cube cube, String factTable)
//    {
//        List<Measure> updatedMsrs = new ArrayList<Measure>();
//        List<Measure> dimensionsMsrs = cube.getMeasures(factTable);
//        
//        for(int i = 0;i < measures.length;i++)
//        {
//            
//            for(Measure measure : dimensionsMsrs)
//            {
//                if(measures[i].getName().equals(measure.getName()))
//                {
//                    updatedMsrs.add(measure);
//                }
//            }
//            
//        }
//        return updatedMsrs;
//    }
//    
//    private Map<Dimension, MolapFilterInfo> updateConstraints( Map<Dimension, MolapFilterInfo> constraints,Cube cube, String factTable)
//    {
//        Map<Dimension, MolapFilterInfo> updatedCons = new LinkedHashMap<MolapMetadata.Dimension, MolapFilterInfo>();
//        List<Dimension> dimensionsFact = cube.getDimensions(factTable);
//        
//        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
//        {
//            Dimension dim = entry.getKey();
//            for(Dimension dimension : dimensionsFact)
//            {
//                if(dimension.getName().equals(dim.getName()) && dimension.getDimName().equals(dim.getDimName()) &&  
//                        dimension.getHierName().equals(dim.getHierName()))
//                {
//                    updatedCons.put(dimension, entry.getValue());
//                    
//                }
//            }
//        }
//        return updatedCons;
//    }
//    
////    private void logQueryString(String factTable, Dimension[] dims, List<Measure> msrs,
////            Map<Dimension, MolapFilterInfo> constraints, boolean properties,List<Measure> calcMsrs,Map<Measure, MeasureFilterModel[]> msrFilters)
////    {
////        
////        StringBuffer buffer = new StringBuffer();
////        buffer.append("&&&&&&&&&&&&&&&&&&&&&&&&");
////        buffer.append("Dimensions : ");
////        for(Dimension dimension : dims)
////        {
////            buffer.append(dimension.getName());
////            buffer.append(",");
////        }
////        buffer.append(" Measures : ");
////        for(Measure msr : msrs)
////        {
////            buffer.append(msr.getName());
////            buffer.append(",");
////        }
////        
////        buffer.append(" CalCMeasures : ");
////        for(Measure msr : calcMsrs)
////        {
////            if(msr != null)
////            {
////                buffer.append(msr.getName());
////                buffer.append(",");
////            }
////        }
////        
////        buffer.append(" DimensionConstraints : ");
////        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
////        {
////            buffer.append(entry.getKey()!=null?entry.getKey().getName():entry.getKey());
////            buffer.append(" (");
////            buffer.append(" Include : ");
////            for(String filt : entry.getValue().getIncludedMembers())
////            {
////                buffer.append(filt);
////                buffer.append(",");
////            }
////            
////            buffer.append(" Exclude : ");
////            for(String filt : entry.getValue().getExcludedMembers())
////            {
////                buffer.append(filt);
////                buffer.append(",");
////            }
////            buffer.append(" )");
////        }
////        
////        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,buffer.toString());
////        
////    }
//    
//    
//    private void removeUnnecessaryConstraints(Map<Dimension, MolapFilterInfo> queryConstraints,
//            Map<Dimension, MolapFilterInfo> modelConstraints, boolean grandTotalForAllRows,
//            boolean isFilterInHierGroups, MolapQueryExecutorModel model, String factTableName, boolean isSubTotal)
//    {
////        if(queryConstraints.size() == 0)
////        {
////            return;
////        }
//        List<Dimension> extraCons = new ArrayList<Dimension>();
//        for(Entry<Dimension, MolapFilterInfo> entry : queryConstraints.entrySet())
//        {
////            if(null!=entry.getValue() && entry.getValue().isRestrictedMember())
////            {
////                continue;
////            }
//            MolapFilterInfo molapFilterInfo = modelConstraints.get(entry.getKey());
//            if(molapFilterInfo == null && entry.getKey() != null && entry.getKey().isHasAll())
//            {
////                if(!entry.getValue().isRestrictedMember())
////                {
////                extraCons.add(entry.getKey());
////                }
//            }
//        }
//        
//        if(grandTotalForAllRows || isSubTotal )
//        {
//            return;
//        }
//        
//        for(Dimension dimension : extraCons)
//        {
//            queryConstraints.remove(dimension);
//        }
//        
//        
////        //Add the constraints from model as we removed the constraints added by mondrian
//        for(Entry<Dimension, MolapFilterInfo> entry : modelConstraints.entrySet())
//        {
//            Dimension key = entry.getKey();
//            MolapFilterInfo molapFilterInfo = queryConstraints.get(key);
//            MolapFilterInfo encloseBracketsToConstarints = encloseBracketsToConstarints(entry.getValue());
//            if(molapFilterInfo == null && key != null )
//            {
//                key = key.getDimCopy();
//                queryConstraints.remove(key);
//                queryConstraints.put(key, encloseBracketsToConstarints);
//            }
//            else if(molapFilterInfo != null)
//            {
//                /**
//                 * Fortify fix: Redundant_Null_Check
//                 */
////                if(!molapFilterInfo.isRestrictedMember() && !molapFilterInfo.equals(encloseBracketsToConstarints) && null != key)
////                {
////                    key = key.getDimCopy();
////                    queryConstraints.remove(key);
////                    queryConstraints.put(key, encloseBracketsToConstarints);
////                }
//            }
//        }
//        //queryConstraints = getUpdateConstarintsForHierGroups(queryConstraints, model.getCube(),false,factTableName);
//        model.setConstraints(queryConstraints);
//        model.setFilterInHierGroups(true);
//        
//    }
//    
//
//    private void updateExecutorsforMsrs(Dimension[] dims, List<Measure> msrs,
//            MolapQueryExecutorModel executorModel, MolapQueryModel model,List<Measure> calcMsrs,boolean paginationRequired)
//    {
//        List<Measure> molapMsrs = new ArrayList<MolapMetadata.Measure>(model.getMsrs());
//        getMolapAllMsrs(model.getMsrFilter(), molapMsrs);
//        //In case of pagination just take the msrs from model as we require every thing in first time generation itself.
//        if(paginationRequired && molapMsrs.size() >= msrs.size())
//        {
//            List<Measure> unifiedMeasuresForPagination = getUnifiedMeasuresForPagination(molapMsrs, msrs);
//            msrs = unifiedMeasuresForPagination;
//        }
//        if(checkAllMsrsPresent(molapMsrs, msrs,calcMsrs))
//        {
////            updateTopNModel(topNModel, dims, msrs,calcMsrs,executorModel.getActualMsrs(),executorModel.isPaginationRequired());
//            MeasureSortModel sortModel = model.getSortModel();
//            if(sortModel != null)
//            {
//                updateMsrSortModel(msrs, sortModel,calcMsrs,executorModel.getActualMsrs(),executorModel.isPaginationRequired());
//            }
//            boolean skipFilters = false;
//            
//            if(executorModel.getActualQueryDims() != null && executorModel.getActualQueryDims().length > 0 && executorModel.getDims().length == 0 || (executorModel.getActualQueryDims().length != executorModel.getDims().length))
//            {
//                skipFilters = true;
//            }
//            if(!skipFilters)
//            {
//                List<Measure> filterMsrs = msrs;
//                if(executorModel.isPaginationRequired())
//                {
//                    if(executorModel.getActualMsrs().size() >= msrs.size())
//                    {
//                        filterMsrs = executorModel.getActualMsrs();
//                    }
//                    filterMsrs=getUnifiedMeasuresForPagination(filterMsrs,msrs);
//                }
//                setMsrFilterModels(executorModel, model.getMsrFilter(), calcMsrs, filterMsrs,false);
//                if(model.getMsrFilterAfterTopN() != null && model.getMsrFilterAfterTopN().size() > 0)
//                {
//                    setMsrFilterModels(executorModel, model.getMsrFilterAfterTopN(), calcMsrs, filterMsrs,true);
//                }
//            }
//            executorModel.setSortModel(sortModel);
//        }
//    }
//
//    private List<Measure> getUnifiedMeasuresForPagination(List<Measure> actualMsrs,List<Measure> msrs)
//    {
//        List<Measure> unifiedMsrs = new ArrayList<Measure>();
//        unifiedMsrs.addAll(msrs);
//        for(Measure msr : actualMsrs)
//        {
//            if(!unifiedMsrs.contains(msr))
//            {
//                unifiedMsrs.add(msr);
//            }
//        }
//        return unifiedMsrs;
//    }
//    /**
//     * @param executorModel
//     * @param model
//     * @param calcMsrs
//     * @param filterMsrs
//     */
//    private void setMsrFilterModels(MolapQueryExecutorModel executorModel, Map<Measure,MeasureFilterModel[]> msrFilter,
//            List<Measure> calcMsrs, List<Measure> filterMsrs,boolean isAfterTopN)
//    {
//        MeasureFilterModel[][] measureFilters = getMeasureFilterArrayInMDXQueryOrder(msrFilter, filterMsrs,calcMsrs,executorModel);
//        GroupMeasureFilterModel filterModel = new GroupMeasureFilterModel(measureFilters, MeasureFilterGroupType.AND);
//        if(executorModel.getMsrFilterModels() != null)
//        {
//            if(isAfterTopN)
//            {
//                if(null==executorModel.getMsrFilterModelsTopN())
//                {
//                    List<GroupMeasureFilterModel> listOfMsrFilterMoldel=new ArrayList<GroupMeasureFilterModel>(10);
//                    listOfMsrFilterMoldel.add(filterModel);
//                    executorModel.setMsrFilterModelsTopN(listOfMsrFilterMoldel);
//                }
//                else
//                {
//                executorModel.getMsrFilterModelsTopN().add(filterModel);
//                }
//            }
//            else
//            {
//                executorModel.getMsrFilterModels().add(filterModel);
//            }
//        }
//        else
//        {
//            List<GroupMeasureFilterModel> filterModels = new ArrayList<GroupMeasureFilterModel>();
//            filterModels.add(filterModel);
//            if(!isAfterTopN)
//            {
//                executorModel.setMsrFilterModels(filterModels);
//            }
//            else
//            {
//                executorModel.setMsrFilterModelsTopN(filterModels);
//            }
//        }
//    }
//    
//    private void processTopN(Dimension[] dims, List<Measure> msrs,
//            MolapQueryExecutorModel executorModel, MolapQueryModel model, TopNModel topNModel,List<Measure> calcMsrs,boolean paginationRequired)
//    {
//        List<Measure> molapMsrs = new ArrayList<MolapMetadata.Measure>(model.getMsrs());
//        getMolapAllMsrs(model.getMsrFilter(), molapMsrs);
//        //In case of pagination just take the msrs from model as we require every thing in first time generation itself.
//        if(paginationRequired && molapMsrs.size() > msrs.size())
//        {
//            msrs = molapMsrs;
//        }
//        if(checkAllMsrsPresent(molapMsrs, msrs,calcMsrs))
//        {
//            updateTopNModel(topNModel, dims, msrs,calcMsrs,executorModel.getActualMsrs(),executorModel.isPaginationRequired());
//            executorModel.setTopNModel(topNModel);
//        }
//    }
//    
//    private void fillCalcMeasures(MolapQueryModel model,List<Measure> calcMsrs,Query query,List<Measure> mdxMeasures,Cube cube,String tableName,MolapQueryExecutorModel executorModel, boolean isAggTable,boolean isAutoAgg)
//    {
//        List<Dimension> actualQueryDims = new ArrayList<MolapMetadata.Dimension>(Arrays.asList(executorModel.getActualQueryDims()));
//        List<CalculatedMeasure> calcMsrsFromModel = model.getCalcMsrs();
//        for(CalculatedMeasure calculatedMeasure : calcMsrsFromModel)
//        {
//            boolean found = false;
//            for(Measure measure : calcMsrs)
//            {
//                if(measure.getName().equals(calculatedMeasure.getName()))
//                {
//                    found = true;
//                    break;
//                }
//            }
//            if(!found)
//            {
//                if(calculatedMeasure.getExp() == null)
//                {
//                    calculatedMeasure = MolapCalcExpressionResolverUtil.createCalcMeasure(query, calculatedMeasure.getName());
//                    /**
//                     * Fortify Fix: NULL_RETURNS
//                     */
//                    MolapCalcFunction createCalcExpressions = null != calculatedMeasure ? MolapCalcExpressionResolverUtil.createCalcExpressions(calculatedMeasure.getExp(), cube.getMeasures(cube.getFactTableName())) : null;
//                    if(createCalcExpressions != null)
//                    {
//                        calcMsrs.add(calculatedMeasure);
//                    }
//                    else
//                    {
//                        if(null != calculatedMeasure && null != calculatedMeasure.getExp() && calculatedMeasure.getExp().toString().startsWith("Count"))
//                        {
//                            Measure measure = getCountMeasure(cube,tableName,isAggTable,isAutoAgg);
//                            
//                            boolean foundMsr = false;
//                            for(Measure measure1 : mdxMeasures)
//                            {
//                                if(measure.getName().equals(measure1.getName()))
//                                {
//                                    foundMsr = true;
//                                    break;
//                                }
//                            }
//                            if(!foundMsr)
//                            {
//                                mdxMeasures.add(measure);
//                            }
//                            executorModel.setCountFunction(true);
//                            Dimension countDimension = getCountDimension(calculatedMeasure.getExp(), cube, tableName);
//                            if(countDimension != null)
//                            {
//                                if(!QueryExecutorUtil.contain(countDimension, actualQueryDims))
//                                {
//                                   List<Dimension> allDims = cube.getDimensions(tableName);
//                                   for(Dimension dim : allDims)
//                                   {
//                                       if(dim.getHierName().equals(countDimension.getHierName()) && dim.getOrdinal() <= countDimension.getOrdinal())
//                                       {
//                                           if(!QueryExecutorUtil.contain(dim, actualQueryDims))
//                                           {
//                                               actualQueryDims.add(dim);
//                                           }
//                                       }
//                                   }
//                                }
//                            }
//                        }
//                    }
//                }
//                else
//                {
//                    calcMsrs.add(calculatedMeasure);
//                }
//                
//                if(null != calculatedMeasure)
//                {
//                    MolapCalcExpressionResolverUtil.getMeasuresFromCalcMeasures(mdxMeasures,
//                            calculatedMeasure.getExp(), cube);
//                }
//                
//            }
//        }
//        
//        executorModel.setActualQueryDims(actualQueryDims.toArray(new Dimension[actualQueryDims.size()]));
//        
//    }
//    
//    private Measure getCountMeasure(Cube cube, String actualTableName,boolean isAggTable, boolean isAutoAgg)
//    {
//        List<Measure> measures = cube.getMeasures(actualTableName);
//        
//        for(Measure measure : measures)
//        {
//            if(measure.getAggName() != null && measure.getAggName().equals(MolapCommonConstants.COUNT))
//            {
//                return measure;
//            }
//        }
//        
//        if(isAggTable && isAutoAgg)
//        {
//            List<Measure> measures1= cube.getMeasures(actualTableName);
//            int size = measures1.size();
//            for(int i = 0;i < size;i++)
//            {
//                if(!measures1.get(i).getAggName().equals(MolapCommonConstants.DISTINCT_COUNT)
//                        && !measures1.get(i).getAggName().equals(MolapCommonConstants.CUSTOM))
//                {
//                    Measure measure = measures1.get(i);
//                    measure = measure.getCopy();
//                    measure.setAggName(MolapCommonConstants.COUNT);
//                    measure.setName("CALC"+measure.getName()+measure.getAggName());
//                    return measure;
//                }
//            }
//        }
//        
//        Measure measure = cube.getMeasures(actualTableName).get(0);
//        measure = measure.getCopy();
//        measure.setAggName(MolapCommonConstants.COUNT);
//        measure.setName("CALC"+measure.getName()+measure.getAggName());
//        return measure;
//    }
//    
//    private Dimension getCountDimension(Exp exp,Cube cube,String tableName)
//    {
//        if(exp instanceof ResolvedFunCall)
//        {
//            ResolvedFunCall fun = (ResolvedFunCall)exp;
//            RolapCubeLevel cubeLevel = null;
//            if(((ResolvedFunCall)((ResolvedFunCall)fun.getArgs()[0]).getArgs()[0]).getArgs()[1] instanceof LevelExpr)
//            {
//                cubeLevel = (RolapCubeLevel)((LevelExpr)((ResolvedFunCall)((ResolvedFunCall)fun.getArgs()[0]).getArgs()[0]).getArgs()[1]).getLevel();
//            }
//            else 
//            {
//                cubeLevel = (RolapCubeLevel)((LevelExpr)((ResolvedFunCall)((ResolvedFunCall)((ResolvedFunCall)fun.getArgs()[0]).getArgs()[0]).getArgs()[0]).getArgs()[1]).getLevel();
//            }
//               
//            Dimension dimensionByLevelName = cube.getDimensionByLevelName(cubeLevel.getDimension().getName(), cubeLevel.getHierarchy().getSubName(), cubeLevel.getName(), tableName);
//            return dimensionByLevelName;
//        }
//        
//        return null;
//    }
//    
//    private List<Dimension> fillWithParents(List<Dimension> actual,List<Dimension> allDims,Dimension[] queryDims,boolean isQueryDimsAddReq)
//    {
//        boolean isActual = false;
//        if(queryDims.length > 0)
//        {
//            isActual = queryDims[0].isActualCol();
//        }
//        List<Dimension> filledDims = new ArrayList<Dimension>();
//        for(Dimension dimension : actual)
//        {
//            
//            for(Dimension allDim : allDims)
//            {
//                if(allDim.getHierName().equals(dimension.getHierName()) && allDim.getDimName().equals(dimension.getDimName()))
//                {
//                    if(allDim.getOrdinal() <= dimension.getOrdinal())
//                    {
//                        if(!QueryExecutorUtil.contain(allDim,filledDims))
//                        {
//                            Dimension dimCopy = allDim.getDimCopy();
//                            dimCopy.setActualCol(isActual);
//                            filledDims.add(dimCopy);
//                        }
//                        if(allDim.getHierName().equals(dimension.getHierName())
//                                && allDim.getDimName().equals(dimension.getDimName())
//                                && allDim.getName().equals(dimension.getName()))
//                        {
//                            break;
//                        }
//                    }
//                }
//            }
//            
//        }
//        
//        // Added method to refactor the sourcemonitor issue. 
//        fillParentIfQueryDimAddRequired(allDims, queryDims, isQueryDimsAddReq, isActual, filledDims);
//        
//        return filledDims;
//        
//    }
//
//    /**
//     * 
//     * @param allDims
//     * @param queryDims
//     * @param isQueryDimsAddReq
//     * @param isActual
//     * @param filledDims
//     * 
//     */
//    private void fillParentIfQueryDimAddRequired(List<Dimension> allDims, Dimension[] queryDims,
//            boolean isQueryDimsAddReq, boolean isActual, List<Dimension> filledDims)
//    {
//        if(isQueryDimsAddReq)
//        {
//            for(Dimension dimension : queryDims)
//            {
//                
//                for(Dimension allDim : allDims)
//                {
//                    if(allDim.getHierName().equals(dimension.getHierName()) && allDim.getDimName().equals(dimension.getDimName()))
//                    {
//                        if(allDim.getOrdinal() <= dimension.getOrdinal())
//                        {
//                            if(!QueryExecutorUtil.contain(allDim,filledDims))
//                            {
//                                Dimension dimCopy = allDim.getDimCopy();
//                                dimCopy.setActualCol(isActual);
//                                filledDims.add(dimCopy);
//                            }
//                            if(allDim.getHierName().equals(dimension.getHierName())
//                                    && allDim.getDimName().equals(dimension.getDimName())
//                                    && allDim.getName().equals(dimension.getName()))
//                            {
//                                break;
//                            }
//                        }
//                    }
//                }
//                
//            }
//        }
//    }
//    
//    /**
//     * 
//     * @param dims
//     * @param queryDims
//     * @param dimSortTypes
//     * @return
//     * 
//     */
//    private byte[] compareDimsAndGetTheSortOrder(Dimension[] dims, List<Dimension> queryDims, byte[] dimSortTypes)
//    {
//        byte [] sortOrder=new byte[dims.length];
//
//        for(int i = 0;i < dims.length;i++)
//        {
//            for(int j = 0;j < queryDims.size();j++)
//            {
//                if(dims[i].getName().equals(queryDims.get(j).getName())
//                        && dims[i].getDimName().equals(queryDims.get(j).getDimName())
//                        && dims[i].getHierName().equals(queryDims.get(j).getHierName()))
//                {
//                    // CHECKSTYLE:OFF Approval No:Approval-265
//                    sortOrder[i] = dimSortTypes[j];
//                    // CHECKSTYLE:ON
//                }
//            }
//        }
//        return sortOrder;
//    }
//
//    /**
//     * 
//     * @param dims
//     * @param queryDims
//     * @param dimSortTypes
//     * @return
//     * 
//     */
//    private byte[] compareDimsAndGetTheSortOrderForPagination(Dimension[] queryDims, byte[] dimSortTypes)
//    {
//        int size=queryDims.length;
//        if(size > dimSortTypes.length)
//        {
//            size = dimSortTypes.length;
//        }
//        byte [] sortOrder=new byte[queryDims.length];
//        System.arraycopy(dimSortTypes, 0, sortOrder, 0, size);
//        return sortOrder;
//    }
//    
//    
//    private void updateTopNModel(TopNModel model,Dimension[] dims,List<Measure> msrs,List<Measure> calcMsrs,List<Measure> actualMsrs,boolean paginationRequired)
//    {
//        if(model == null)
//        {
//            return;
//        }
//        Measure msr = model.getMeasure();
//        Dimension dim = model.getDimension();
//        if(paginationRequired && actualMsrs.size() > msrs.size())
//        {
//            msrs = actualMsrs;
//        }
//        int msrIndex = -1;
//        int i = 0;
//        for(Measure m : msrs)
//        {
//            if(msr.getName().equals(m.getName()))
//            {
//                msrIndex = i;
//                break;
//            }
//            i++;
//        }
//        i = 0;
//        if(msrIndex == -1)
//        {
//            for(Measure m : calcMsrs)
//            {
//                if(msr.getName().equals(m.getName()))
//                {
//                    msrIndex = i+msrs.size();
//                    break;
//                }
//                i++;
//            }
//        }
//        int dimIndex = getDimIndex(dims, dim);
//        model.setDimIndex(dimIndex);
//        if(msrIndex >= 0)
//        {
//            model.setMsrIndex(msrIndex);
//        }
//        else
//        {
//            if(msr instanceof CalculatedMeasure)
//            {
//                calcMsrs.add(msr);
//                model.setMsrIndex(msrs.size()+calcMsrs.size()-1);
//            }
//            else
//            {
//                msrs.add(msr);
//                model.setMsrIndex(msrs.size()-1);
//            }
//        }
//    }
//
//    /**
//     * @param dims
//     * @param dim
//     * @param dimIndex
//     * @return
//     */
//    public int getDimIndex(Dimension[] dims, Dimension dim)
//    {
//        int i = 0;
//        int dimIndex = -1;
//        for(Dimension d : dims)
//        {
//            if(dim.getDimName().equals(d.getDimName()) && dim.getHierName().equals(d.getHierName())
//                    && dim.getName().equals(d.getName()))
//            {
//                dimIndex = i;
//                break;
//            }
//            i++;
//        }
//        return dimIndex;
//    }
//    
//    private void updateMsrSortModel(List<Measure> msrs,MeasureSortModel sortModel,List<Measure> calcMsrs,List<Measure> actualMsrs,boolean paginationRequired)
//    {
//        int msrIndex = -1;
//        Measure msr = sortModel.getMeasure();
//        
//        if(paginationRequired && actualMsrs.size() >= msrs.size())
//        {
//            List<Measure> unifiedMeasuresForPagination = getUnifiedMeasuresForPagination(actualMsrs, msrs);
//            msrs = unifiedMeasuresForPagination;
//        }
//        int i = 0;
//        for(Measure m : msrs)
//        {
//            if(msr.getName().equals(m.getName()))
//            {
//                msrIndex = i;
//                break;
//            }
//            i++;
//        }
//        i = 0;
//        if(msrIndex == -1)
//        {
//            for(Measure m : calcMsrs)
//            {
//                if(msr.getName().equals(m.getName()))
//                {
//                    msrIndex = i+msrs.size();
//                    break;
//                }
//                i++;
//            }
//        }
//        sortModel.setMeasureIndex(msrIndex);
//    }
//    
//    private boolean checkAllMsrsPresent(List<Measure> molapMsrs,List<Measure> mdxMsrs,List<Measure> calcMsrs)
//    {
//        for(Measure molapMsr : molapMsrs)
//        {
//            boolean found = false;
//            for(Measure mdxMsr : mdxMsrs)
//            {
//                if(molapMsr.getName().equals(mdxMsr.getName()))
//                {
//                    found = true;
//                    break;
//                }
//            }
//            for(Measure calcMsr : calcMsrs)
//            {
//                if(molapMsr.getName().equals(calcMsr.getName()))
//                {
//                    found = true;
//                    break;
//                }
//            }
//            if(!found)
//            {
//                return false;
//            }
//        }
//        return true;
//        
//    }
//    
//    private MeasureFilterModel[][] getMeasureFilterArrayInMDXQueryOrder(Map<Measure,MeasureFilterModel[]> msrFilterMap, List<Measure> mdxMsrs,List<Measure> calcMsrs,MolapQueryExecutorModel executorModel)
//    {
//        if(msrFilterMap == null)
//        {
//            return null;
//        }
//        Dimension dimension = null;
//        AxisType axisType = null; 
//        MeasureFilterModel[][] filterModels = new MeasureFilterModel[mdxMsrs.size()+calcMsrs.size()][];
//        int i=0;
//        for(Measure measure : mdxMsrs)
//        {
//            for(Entry<Measure,MeasureFilterModel[]> entry : msrFilterMap.entrySet())
//            {
//                
//                Measure key = entry.getKey();
//                if(measure.getName().equals(key.getName()))
//                {
//                    filterModels[i] = entry.getValue();
//                    if(filterModels[i] != null && filterModels[i].length > 0)
//                    {
//                        if(filterModels[i][0].getDimension() != null)
//                        {
//                            dimension = filterModels[i][0].getDimension();
//                            axisType = filterModels[i][0].getAxisType();
//                        }
//                    }
//                    break;
//                }
//            }
//            
//            i++;
//        }
//        i = mdxMsrs.size();
//        for(Measure measure : calcMsrs)
//        {
//           
//            for(Entry<Measure,MeasureFilterModel[]> entry : msrFilterMap.entrySet())
//            {
//                
//                Measure key = entry.getKey();
//                if(measure.getName().equals(key.getName()))
//                {
//                    filterModels[i] = entry.getValue();
//                    if(filterModels[i] != null && filterModels[i].length > 0)
//                    {
//                        if(measure instanceof CalculatedMeasure)
//                        {
//                            CalculatedMeasure calculatedMeasure = (CalculatedMeasure)measure;
//                            for(int j = 0;j < filterModels[i].length;j++)
//                            {
//                                filterModels[i][j].setExp(calculatedMeasure.getExp());
//                            }
//                        }
//                        if(filterModels[i][0].getDimension() != null)
//                        {
//                            dimension = filterModels[i][0].getDimension();
//                            axisType = filterModels[i][0].getAxisType();
//                        }
//                    }
//                    break;
//                }
//            }
//            
//            i++;
//        }
//        
//        if(dimension != null)
//        {
//            MeasureFilterProcessorModel filterProcessorModel = new MeasureFilterProcessorModel();
//            filterProcessorModel.setDimension(dimension);
//            filterProcessorModel.setDimIndex(getDimIndex(executorModel.getActualQueryDims(), dimension));
//            filterProcessorModel.setAxisType(axisType);
//            executorModel.setMeasureFilterProcessorModel(filterProcessorModel);
//        }
//        
//        return filterModels;
//    }
//    
////    private void mergeMeasureFilters(MeasureFilterModel[][] molapMsrs,MeasureFilterModel[][] molapDirectApiMsrs)
////    {
////        if(molapMsrs == null)
////        {
////            return;
////        }
////        for(int i = 0;i < molapMsrs.length;i++)
////        {
////            if(molapMsrs[i] != null && molapMsrs[i].length > 0)
////            {
////                if(molapDirectApiMsrs[i] == null)
////                {
////                    molapDirectApiMsrs[i] = molapMsrs[i];
////                }
////                else
////                {
////                    MeasureFilterModel[] filterModels = new MeasureFilterModel[molapMsrs[i].length+molapDirectApiMsrs[i].length];
////                    System.arraycopy(molapDirectApiMsrs, 0, filterModels, 0, molapDirectApiMsrs.length);
////                    System.arraycopy(molapMsrs, 0, filterModels, molapDirectApiMsrs.length, molapMsrs.length);
////                    molapDirectApiMsrs[i] = filterModels;
////                }
////            }
////        }
////    }
//
//    /**
//     * @param msrFilterMap
//     * @param molapMsrs
//     */
//    private void getMolapAllMsrs(Map<Measure, MeasureFilterModel[]> msrFilterMap, List<Measure> molapMsrs)
//    {
//        for(Entry<Measure,MeasureFilterModel[]> entry : msrFilterMap.entrySet())
//        {
//            Measure key = entry.getKey();
//            boolean found = false;
//            for(Measure measure : molapMsrs)
//            {
//                if(measure.getName().equals(key.getName()))
//                {
//                    found = true;
//                    break;
//                }
//            }
//            
//            if(!found)
//            {
//                molapMsrs.add(key);
//            }
//            
//        }
//    }
}