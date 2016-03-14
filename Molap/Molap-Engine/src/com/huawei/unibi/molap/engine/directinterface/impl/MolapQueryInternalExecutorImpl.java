/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOfKZqZspKynm9RSRPgEXh4h+BWgR7cLBIQGCznqByh8754IDB8xbxylNC/tDOQ1WGaSl
vFV6zcSU14ZgQCdCHxOE4uEgXqMcdL90TIyfJ3OecTCunKBxg1RjeEYHtEK5kw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.directinterface.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
//import com.huawei.unibi.loadcontrol.exception.LoadControlException;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.filters.measurefilter.GroupMeasureFilterModel;
import com.huawei.unibi.molap.engine.filters.measurefilter.GroupMeasureFilterModel.MeasureFilterGroupType;
import com.huawei.unibi.molap.engine.filters.measurefilter.MeasureFilterModel;
import com.huawei.unibi.molap.engine.holders.MolapResultHolder;
import com.huawei.unibi.molap.engine.schema.metadata.MeasureFilterProcessorModel;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
//import mondrian.rolap.RolapConnection;
//import mondrian.rolap.RolapUtil;
//import mondrian.rolap.SqlStatement;
import com.huawei.unibi.molap.olap.SqlStatement;
import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.impl.MolapQueryImpl;
import com.huawei.unibi.molap.query.result.MolapResultStreamHolder;
import com.huawei.unibi.molap.query.result.impl.MolapResultStreamImpl;
import com.huawei.unibi.molap.queryexecutor.MolapQueryInternalExecutor;

/**
 * It is the executor service implementation class in server
 * @author R00900208
 *
 */
public class MolapQueryInternalExecutorImpl implements MolapQueryInternalExecutor
{
    
    /**
     * 
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapQueryInternalExecutorImpl.class.getName());
    
    /**
     * It executes the molap query and returns the result.
     */
    @Override
    public MolapResultStreamHolder execute(MolapQuery molapQuery,String schemaName,String cubeName)
    {
//        RolapUtil.getCube(schemaName, cubeName);
        MolapQueryImpl queryImpl = (MolapQueryImpl) molapQuery;
        MolapResultHolder hIterator = null;
        MolapQueryModel model = null;
        try
        {
//            RolapConnection.THREAD_LOCAL.get().put(RolapConnection.SCHEMA_NAME, schemaName);
//            RolapConnection.THREAD_LOCAL.get().put(RolapConnection.CUBE_NAME, cubeName);
            
            model = MolapQueryParseUtil.parseMolapQuery(molapQuery, schemaName, cubeName);
            Cube cube = model.getCube();
            model.setExactLevelsMatch(queryImpl.isExactLevelsMatch());
            
            hIterator = new MolapResultHolder(getDataTypes(model.getQueryDims()));
            
//            InMemoryQueryExecutor executor = new InMemoryQueryExecutor(cube.getDimensions(model.getFactTableName()),true,schemaName, cubeName);
            
            List<Measure> msrs = new ArrayList<MolapMetadata.Measure>(model.getMsrs());
            
            MeasureFilterModel[][] measureFilterArray = getMeasureFilterArray(model.getMsrFilter(), msrs);
            
            List<GroupMeasureFilterModel> filterModels = null;
            GroupMeasureFilterModel groupMeasureFilterModel = new GroupMeasureFilterModel(measureFilterArray,
                    MeasureFilterGroupType.AND);
            filterModels = new ArrayList<GroupMeasureFilterModel>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            filterModels.add(groupMeasureFilterModel);

            MolapQueryExecutorModel executorModel = new MolapQueryExecutorModel();
            executorModel.setCube(cube);
            executorModel.sethIterator(hIterator);
            executorModel.setFactTable(model.getFactTableName());
            executorModel.setDims(model.getQueryDims().toArray(new Dimension[model.getQueryDims().size()]));
            executorModel.setMsrs(msrs);
            executorModel.setConstraints(model.getConstraints());
            executorModel.setProperties(queryImpl.isShowLevelProperties());
            executorModel.setMsrFilterModels(filterModels);
            executorModel.setSortOrder(model.getDimSortTypes());
//            executorModel.setTopNModel(model.getTopNModel());
            executorModel.setSortModel(model.getSortModel());
            executorModel.setFilterInHierGroups(model.isExactLevelsMatch());
            executorModel.setAggTable(!(model.getFactTableName().equals(cube.getFactTableName())));
            executorModel.setActualQueryDims(model.getQueryDims().toArray(new Dimension[model.getQueryDims().size()]));
            executorModel.setActualDimsRows(model.getQueryDimsRows().toArray(new Dimension[model.getQueryDimsRows().size()]));
            executorModel.setActualDimsCols(model.getQueryDimsCols().toArray(new Dimension[model.getQueryDimsCols().size()]));
            executorModel.setCalcMeasures(new ArrayList<Measure>(model.getCalcMsrs()));
            executorModel.setAnalyzerDims(model.getQueryDims().toArray(new Dimension[model.getQueryDims().size()]));
            executorModel.setConstraintsAfterTopN(model.getConstraintsAfterTopN());
            boolean isBreak=false;
            for(int i = 0;i < measureFilterArray.length;i++)
            {
                if(isBreak)
                {
                    break;
                }
                if(null != measureFilterArray[i])
                {
                    for(int j = 0;j < measureFilterArray[i].length;j++)
                    {
                        if(null != measureFilterArray[i][j] && null != measureFilterArray[i][j].getDimension())
                        {
                            MeasureFilterProcessorModel filterProcessorModel = new MeasureFilterProcessorModel();
                            filterProcessorModel.setDimension(measureFilterArray[i][j].getDimension());
                            filterProcessorModel.setDimIndex(getDimIndex(executorModel.getDims(),
                                    measureFilterArray[i][j].getDimension()));
                            filterProcessorModel.setAxisType(measureFilterArray[i][j].getAxisType());
//                            executorModel.setMeasureFilterProcessorModel(filterProcessorModel);
                            isBreak = true;
                            break;
                        }
                    }

                }
            }
            Thread.currentThread().setName("DIRECT_API");
//            QueryMapper.queryStart(schemaName+'_'+cubeName, threadId, threadId);
//            MolapQueryExecutorTask executorTask = new MolapQueryExecutorTask(executor, executorModel);
//            MolapQueryExecutorHelper.getInstance().executeQueryTask(executorTask);
            
        }
//        catch(LoadControlException e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,"Query execution failed ");
//            throw new RuntimeException(e.getMessage());
//        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,"Query execution failed ");
            throw new RuntimeException(e.getMessage());
        }


        MolapResultStreamImpl streamImpl = MolapResultConverter.convert(hIterator, model.getQueryDimsRows(),
                model.getQueryDimsCols(), model.getMsrs(), queryImpl.isShowLevelProperties());
        
        MolapResultStreamHolder holder = new MolapResultStreamHolder();
        holder.setResultStream(streamImpl);
        holder.setUuid(UUID.randomUUID());
        return holder;
    }
    /**
     * @param dims
     * @param dim
     * @param dimIndex
     * @return
     */
    public int getDimIndex(Dimension[] dims, Dimension dim)
    {
        int i = 0;
        int dimIndex = -1;
        for(Dimension d : dims)
        {
            if(dim.getDimName().equals(d.getDimName()) && dim.getHierName().equals(d.getHierName())
                    && dim.getName().equals(d.getName()))
            {
                dimIndex = i;
                break;
            }
            i++;
        }
        return dimIndex;
    }
    

    /**
     * It fetches the next remained data as per the UUID.
     */
    @Override
    public MolapResultStreamHolder getNext(UUID uuid)
    {
        //TODO : Need to be implemented.
        return null;
    }
    
    
    
    /**
     * Get the measure filter array for each measure asked.
     * @param msrFilterMap
     * @param msrs
     * @return
     */
    private MeasureFilterModel[][] getMeasureFilterArray(Map<Measure,MeasureFilterModel[]> msrFilterMap, List<Measure> msrs)
    {
        
        for(Entry<Measure,MeasureFilterModel[]> entry : msrFilterMap.entrySet())
        {
            Measure key = entry.getKey();
            boolean found = false;
            for(Measure measure : msrs)
            {
                if(measure.getName().equals(key.getName()))
                {
                    found = true;
                    break;
                }
            }
            //If measure is existed only on slice then add it to the columns at the end.We can eliminate the measure while returning the result.
            if(!found)
            {
                msrs.add(key);
            }
            
        }
        
        //Get the measure filters for eaxh measure requested.
        MeasureFilterModel[][] filterModels = new MeasureFilterModel[msrs.size()][];
        int i=0;
        for(Measure measure : msrs)
        {
            for(Entry<Measure,MeasureFilterModel[]> entry : msrFilterMap.entrySet())
            {
                
                Measure key = entry.getKey();
                if(measure.getName().equals(key.getName()))
                {
                    filterModels[i] = entry.getValue();
                    break;
                }
            }
            
            i++;
        }
        return filterModels;
        
    }
    
    /**
     * Get all the data types of dimensions requested.
     * @param queryDims
     * @return
     */
    private List<SqlStatement.Type> getDataTypes(List<MolapMetadata.Dimension> queryDims)
    {
        List<SqlStatement.Type> dataTypes = new ArrayList<SqlStatement.Type>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        
        for(Dimension dim : queryDims)
        {
            dataTypes.add(dim.getDataType());
        }
        
        return dataTypes;
    }
    
 
 
    
}
