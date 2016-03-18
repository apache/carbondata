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
package com.huawei.datasight.molap.spark.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.sql.SparkUnknownExpression;
import org.apache.spark.sql.cubemodel.Partitioner;

import com.google.gson.Gson;
import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.datasight.molap.partition.api.Partition;
import com.huawei.datasight.molap.partition.api.impl.DefaultLoadBalancer;
import com.huawei.datasight.molap.partition.api.impl.PartitionMultiFileImpl;
import com.huawei.datasight.molap.partition.api.impl.QueryPartitionHelper;
import com.huawei.datasight.molap.query.MolapQueryPlan;
import com.huawei.datasight.molap.query.metadata.MolapColumn;
import com.huawei.datasight.molap.query.metadata.MolapDimension;
import com.huawei.datasight.molap.query.metadata.MolapLikeFilter;
import com.huawei.datasight.molap.query.metadata.MolapMeasure;
import com.huawei.datasight.molap.query.metadata.MolapQueryExpression;
import com.huawei.datasight.molap.query.metadata.SortOrderType;
import com.huawei.datasight.molap.spark.splits.TableSplit;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperations;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperationsImpl;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.aggregator.CustomMeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.CustomMolapAggregateExpression;
import com.huawei.unibi.molap.engine.aggregator.dimension.DimensionAggregatorInfo;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore;
import com.huawei.unibi.molap.engine.directinterface.impl.MeasureSortModel;
import com.huawei.unibi.molap.engine.directinterface.impl.MolapQueryParseUtil;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.QueryExecutor;
import com.huawei.unibi.molap.engine.executer.impl.QueryExecutorImpl;
import com.huawei.unibi.molap.engine.expression.ColumnExpression;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.conditional.ConditionalExpression;
import com.huawei.unibi.molap.engine.filters.likefilters.FilterLikeExpressionIntf;
import com.huawei.unibi.molap.engine.filters.metadata.ContentMatchFilterInfo;
import com.huawei.unibi.molap.engine.holders.MolapResultHolder;
//import com.huawei.unibi.molap.engine.mondrian.extensions.MolapDataSourceFactory;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
//import mondrian.olap.Connection;
//import mondrian.olap.DriverManager;
//import mondrian.olap.MondrianDef.CubeDimension;
//import mondrian.olap.MondrianDef.Schema;
//import mondrian.olap.MondrianDef.Table;
//import mondrian.olap.Util;
//import mondrian.olap.Util.PropertyList;
//import mondrian.rolap.RolapConnectionProperties;
//import mondrian.rolap.SqlStatement.Type;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.olap.MolapDef.Table;
import com.huawei.unibi.molap.olap.SqlStatement.Type;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * This utilty parses the Molap query plan to actual query model object.
 * @author R00900208
 *
 */
public final class MolapQueryUtil 
{
	
	private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapQueryUtil.class.getName());
	
//	/**
//	 * It parses the Molap query plan to executor model. The executor model is the class which have actual information
//	 * of query.
//	 * @param logicalPlan
//	 * @param schemaPath
//	 * @return
//	 * @throws IOException 
//	 */
//	public static MolapQueryExecutorModel parseQuery(MolapQueryPlan logicalPlan,String schemaPath) throws IOException
//	{
//		
//		Cube cube = loadSchema(schemaPath,logicalPlan.getSchemaName(),logicalPlan.getCubeName());
//		 MolapQueryExecutorModel executorModel = createModel(logicalPlan, cube);
//        executorModel.setMsrConstraints(measureFilterArray);
//        executorModel.setSortOrder(model.getDimSortTypes());
//        executorModel.setTopNModel(model.getTopNModel());
//        executorModel.setSortModel(model.getSortModel());
//        executorModel.setFilterInHierGroups(model.isExactLevelsMatch());
//        executorModel.setAggTable(!(model.getFactTableName().equals(factTableName)));
//        executorModel.setQueryID(model.getQueryID());
//        executorModel.setRowLimit(rowLimit);
//		
//		
//		
//		
//		
//		return executorModel;
//	}
	
	private MolapQueryUtil()
	{
		
	}

	/**
	 * API will provide the slices
	 * @param executerModel
	 * @param basePath
	 * @param partitionID
	 * @return
	 */
	public static List<String> getSliceLoads(MolapQueryExecutorModel executerModel,String basePath,String partitionID)
	{

		List<String> listOfLoadPaths=new ArrayList<String>(20);
		if (null != executerModel) {
			List<String> listOfLoad = executerModel.getListValidSliceNumbers();

			if (null != listOfLoad) {
				for (String name : listOfLoad) {
					String loadPath = MolapCommonConstants.LOAD_FOLDER + name;
					listOfLoadPaths.add(loadPath);
				}
			}
		}
		
		return listOfLoadPaths;
		
	}
	/**
	 * This API will update the query executer model with valid sclice folder numbers based on its
	 * load status present in the load metadata.
	 * @param executerModel
	 * @param dataPath
	 * @return
	 */
	public static MolapQueryExecutorModel updateMolapExecuterModelWithLoadMetadata(
			MolapQueryExecutorModel executerModel) {

		List<String> listOfValidSlices = new ArrayList<String>(10);
		String dataPath = executerModel.getCube().getMetaDataFilepath() + File.separator + MolapCommonConstants.LOADMETADATA_FILENAME + MolapCommonConstants.MOLAP_METADATA_EXTENSION;
		DataInputStream dataInputStream = null;
		Gson gsonObjectToRead = new Gson();
		AtomicFileOperations fileOperation = new AtomicFileOperationsImpl(dataPath, FileFactory.getFileType(dataPath));
		try {
			if (FileFactory.isFileExist(dataPath,
					FileFactory.getFileType(dataPath))) {

			    dataInputStream = fileOperation.openForRead();
			    
			/*	dataInputStream = FileFactory.getDataInputStream(dataPath,
						FileFactory.getFileType(dataPath));*/

				BufferedReader buffReader = new BufferedReader(
						new InputStreamReader(dataInputStream, "UTF-8"));
				
				LoadMetadataDetails[] loadFolderDetailsArray = gsonObjectToRead.fromJson(buffReader,
						LoadMetadataDetails[].class);
				List<String> listOfValidUpdatedSlices=new ArrayList<String>(10);
				//just directly iterate Array
				List<LoadMetadataDetails> loadFolderDetails=Arrays.asList(loadFolderDetailsArray);
				
					for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails) {
						if (MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
								.equalsIgnoreCase(loadMetadataDetails
										.getLoadStatus()) || MolapCommonConstants.MARKED_FOR_UPDATE
										.equalsIgnoreCase(loadMetadataDetails
												.getLoadStatus()) || MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
												.equalsIgnoreCase(loadMetadataDetails
														.getLoadStatus())) {
						    // check for merged loads.
						    if(null != loadMetadataDetails.getMergedLoadName()){
	                            
	                            if(!listOfValidSlices.contains(loadMetadataDetails.getMergedLoadName() ))
	                            {
	                                listOfValidSlices.add(loadMetadataDetails
	                                    .getMergedLoadName());
	                            }
	                            // if merged load is updated then put it in updated list
	                            if(MolapCommonConstants.MARKED_FOR_UPDATE
                                        .equalsIgnoreCase(loadMetadataDetails
                                                .getLoadStatus()))
	                            {
	                                listOfValidUpdatedSlices.add(loadMetadataDetails
	                                        .getMergedLoadName());
	                            }
	                            continue;
	                        }
						    
							if(MolapCommonConstants.MARKED_FOR_UPDATE
										.equalsIgnoreCase(loadMetadataDetails
												.getLoadStatus())){
								
								listOfValidUpdatedSlices.add(loadMetadataDetails
										.getLoadName());
							}
							listOfValidSlices.add(loadMetadataDetails
									.getLoadName());

						}
					}
					executerModel.setListValidSliceNumbers(listOfValidSlices);
					executerModel.setListValidUpdatedSlice(listOfValidUpdatedSlices);
					executerModel.setLoadMetadataDetails(loadFolderDetailsArray);
//				}

			} 

		} catch (IOException e)
		{
			LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "IO Exception @: " + e.getMessage());
		}

		finally {
			try {

				if (null != dataInputStream) {
					dataInputStream.close();
				}
			} catch (Exception e) {
				LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "IO Exception @: " + e.getMessage());
			}

		}
		return executerModel;
	}
	
    public static MolapQueryExecutorModel createModel(
            MolapQueryPlan logicalPlan, Schema schema, Cube cube,
            String storeLocation, int partitionCount) throws IOException 
	{
		MolapQueryExecutorModel executorModel = new MolapQueryExecutorModel();
		executorModel.setSparkExecution(true);
		//TODO : Need to find out right table as per the dims and msrs requested.
		
		String factTableName = cube.getFactTableName();
		executorModel.setCube(cube);
        executorModel.sethIterator(new MolapResultHolder(new ArrayList<Type>(MolapCommonConstants.CONSTANT_SIZE_TEN)));
        
        fillExecutorModel(logicalPlan, cube,schema, executorModel, factTableName);
        List<Dimension> dims = new ArrayList<Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        //since a new executorModel instance has been created the same has to be updated with
        //high cardinality property.
        
        dims.addAll(Arrays.asList(executorModel.getDims()));
//        fillDimensionAggregator(logicalPlan, executorModel, cube.getDimensions(factTableName));
//        dims.addAll(executorModel.getConstraints().keySet());
        //TODO : Make this property configurable
//        MondrianProperties.instance().setProperty("mondrian.rolap.aggregates.Use", "true");
        String suitableTableName = factTableName;
        if(!logicalPlan.isDetailQuery()
                && logicalPlan.getExpressions().isEmpty()
                && Boolean.parseBoolean(MolapProperties.getInstance()
                        .getProperty("spark.molap.use.agg", "true")))
        {
        	if(null == schema)
        	{
        		suitableTableName = MolapQueryParseUtil.getSuitableTable(cube, dims, executorModel.getMsrs());
        	}
        	else
        	{
                suitableTableName = MolapQueryParseUtil.getSuitableTable(
                        logicalPlan.getDimAggregatorInfos(), schema, cube,
                        dims, executorModel, storeLocation, partitionCount);
        	}
        }
//        if(!suitableTableName.equals(factTableName)
//                && isDimAggInfoValidForAggTable(logicalPlan, schema, cube,
//                        suitableTableName, cube.getDimensions(factTableName)))
        if(!suitableTableName.equals(factTableName))
        {
            fillExecutorModel(logicalPlan, cube,schema, executorModel,
                    suitableTableName);
            executorModel.setAggTable(true);
            fillDimensionAggregator(logicalPlan, schema, cube, executorModel);
        }
        else
        {
            fillDimensionAggregator(logicalPlan, schema, cube, executorModel,
                    cube.getDimensions(factTableName));
        }
//        fillDimensionAggregator(logicalPlan, schema, cube, executorModel,
//                cube.getDimensions(suitableTableName));
        executorModel.setActualDimsRows(executorModel.getDims());
        executorModel.setActualDimsCols(new Dimension[0]);
        executorModel.setCalcMeasures(new ArrayList<Measure>(MolapCommonConstants.CONSTANT_SIZE_TEN));
        executorModel.setAnalyzerDims(executorModel.getDims());
        executorModel.setConstraintsAfterTopN(new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE));
        executorModel.setLimit(logicalPlan.getLimit());
        executorModel.setDetailQuery(logicalPlan.isDetailQuery());
        executorModel.setQueryId(logicalPlan.getQueryId());
        executorModel.setOutLocation(logicalPlan.getOutLocationPath());
        return executorModel;
	}

	private static void fillExecutorModel(MolapQueryPlan logicalPlan,
			Cube cube, Schema schema, MolapQueryExecutorModel executorModel,
			String factTableName) 
	{
		executorModel.setFactTable(factTableName);
        List<Dimension> dimensions = cube.getDimensions(factTableName);
		executorModel.setDims(getDimensions(logicalPlan.getDimensions(), dimensions));
		updateDimensionWithHighCardinalityVal(schema,executorModel);
		fillSortInfoInModel(executorModel,logicalPlan.getSortedDimemsions(),dimensions);
        List<Measure> measures = cube.getMeasures(factTableName);
		executorModel.setMsrs(getMeasures(logicalPlan.getMeasures(), measures,executorModel.isDetailQuery(),executorModel));
		if(null!=logicalPlan.getFilterExpression())
		{
			traverseAndSetDimensionOrMsrTypeForColumnExpressions(logicalPlan.getFilterExpression(),dimensions,measures);
			executorModel.setFilterExpression(logicalPlan.getFilterExpression());
					executorModel.setConstraints(getLikeConstaints(logicalPlan.getDimensionLikeFilters(), dimensions));
		}
		List<MolapQueryExpression> expressions = logicalPlan.getExpressions();
		for(MolapQueryExpression anExpression: expressions)
		{
		    if(anExpression.getUsageType()== MolapQueryExpression.UsageType.EXPRESSION)
		    {
		        CustomMolapAggregateExpression  molapExpr = new CustomMolapAggregateExpression();
		        molapExpr.setAggregator((CustomMeasureAggregator)anExpression.getAggregator());
		        molapExpr.setExpression(anExpression.getExpression());
		        molapExpr.setName(anExpression.getExpression());
		        molapExpr.setQueryOrder(anExpression.getQueryOrder());
		        List<Dimension> columns = new ArrayList<>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
		        for(MolapColumn column : anExpression.getColumns())
		        {
		            if(column instanceof MolapDimension)
		            {
		                columns.add(MolapQueryParseUtil.findDimension(dimensions, ((MolapDimension)column).getDimensionUniqueName()));
		            }
		            else
		            {
		                columns.add(findMeasure(measures, executorModel.isDetailQuery(), ((MolapMeasure)column)));
		            }
		        }
		        molapExpr.setReferredColumns(columns);
		        executorModel.addExpression(molapExpr);
		    }
		}
		executorModel.setCountStarQuery(logicalPlan.isCountStartQuery());
	}
	
    /**
     * 
     * @param executorModel
     * @param partitionColumns
     * 
     */
    public static void setPartitionColumn(
            MolapQueryExecutorModel executorModel, String[] partitionColumns)
    {
        List<String> partitionList = Arrays.asList(partitionColumns);
        executorModel.setPartitionColumns(partitionList);
    }
	
	private static void fillSortInfoInModel(
			MolapQueryExecutorModel executorModel, List<MolapDimension> sortedDims, List<Dimension> dimensions)
    {
		if(null!=sortedDims)
		{
			byte[] sortOrderByteArray=new byte[sortedDims.size()];
	    	int i=0;
	    	for(MolapDimension mdim:sortedDims)
	    	{
	    		sortOrderByteArray[i++]=(byte) mdim.getSortOrderType().ordinal();
	    		//sortOrderByteArray[i++]=(byte) 1;
	    	}
	    	executorModel.setSortOrder(sortOrderByteArray);
	    	executorModel.setSortedDimensions(getDimensions(sortedDims, dimensions));
		}
		else
		{
			executorModel.setSortOrder(new byte[0]);
	    	executorModel.setSortedDimensions(new Dimension[0]);
		}
    	
	}

    /**
     * 
     * @param logicalPlan
     * @param executorModel
     * @param dimensions
     * 
     */
    private static void fillDimensionAggregator(MolapQueryPlan logicalPlan,
            Schema schema, Cube cube, MolapQueryExecutorModel executorModel)
    {
        Map<String, DimensionAggregatorInfo> dimAggregatorInfos = logicalPlan
                .getDimAggregatorInfos();
        String dimColumnName = null;
        List<Measure> measure = executorModel.getMsrs();
        List<DimensionAggregatorInfo> dimensionAggregatorInfos = new ArrayList<DimensionAggregatorInfo>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        DimensionAggregatorInfo value = null;
        Measure aggMsr = null;
        List<Measure> measures = cube.getMeasures(executorModel.getFactTable());
        for(Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos
                .entrySet())
        {
            dimColumnName = entry.getKey();
            value = entry.getValue();
            List<Integer> orderList = value.getOrderList();
            List<String> aggList = value.getAggList();
            for(int i = 0;i < aggList.size();i++)
            {
                aggMsr = getMeasure(measures, dimColumnName, aggList.get(i));
                if(aggMsr != null)
                {
                    aggMsr.setQueryOrder(orderList.get(i));
                    if(MolapCommonConstants.DISTINCT_COUNT.equals(aggList
                            .get(i)))
                    {
                        aggMsr.setDistinctQuery(true);
                    }
                    measure.add(aggMsr);
                }
            }
        }
        executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
    }
    
    /**
     * 
     * @param measures
     * @param msrName
     * @param aggName
     * @return
     * 
     */
    private static Measure getMeasure(List<Measure> measures, String msrName,
            String aggName)
    {
        for(Measure measure : measures)
        {
            if(measure.getName().equals(msrName)
                    && measure.getAggName().equals(aggName))
            {
                return measure;
            }
        }
        return null;
    }
    
    /**
     * 
     * @param logicalPlan
     * @param executorModel
     * @param dimensions
     * 
     */
    private static void fillDimensionAggregator(MolapQueryPlan logicalPlan,
            Schema schema, Cube cube, MolapQueryExecutorModel executorModel,
            List<Dimension> dimensions)
    {
        Map<String, DimensionAggregatorInfo> dimAggregatorInfos = logicalPlan
                .getDimAggregatorInfos();
        String dimColumnName = null;
        List<DimensionAggregatorInfo> dimensionAggregatorInfos = new ArrayList<DimensionAggregatorInfo>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        DimensionAggregatorInfo value = null;
        Dimension dimension = null;
        for(Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos
                .entrySet())
        {
            dimColumnName = entry.getKey();
            value = entry.getValue();
            dimension = MolapQueryParseUtil.findDimension(dimensions, dimColumnName);
            value.setDim(dimension);
            dimensionAggregatorInfos.add(value);
        }
        executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
    }
    
//    private static boolean isDimAggInfoValidForAggTable(
//            MolapQueryPlan logicalPlan, Schema schema, Cube cube,
//            String tableName, List<Dimension> dimensions)
//    {
//        Map<String, DimensionAggregatorInfo> dimAggregatorInfos = logicalPlan
//                .getDimAggregatorInfos();
//        String dimColumnName = null;
//        DimensionAggregatorInfo value = null;
//        Dimension dimension = null;
//        for(Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos
//                .entrySet())
//        {
//            dimColumnName = entry.getKey();
//            value = entry.getValue();
//            dimension = findDimension(dimensions, dimColumnName);
//            List<String> aggList = value.getAggList();
//            for(int i = 0;i < aggList.size();i++)
//            {
//                if(!MolapQueryParseUtil.isDimensionMeasureInAggTable(schema,
//                        cube, dimension, aggList.get(i), tableName))
//                {
//                    return false;
//                }
//            }
//        }
//        return true;
//    }
    
	private static void traverseAndSetDimensionOrMsrTypeForColumnExpressions(
			Expression filterExpression, List<Dimension> dimensions,
			List<Measure> measures) {
		if (null != filterExpression) {
			if (null != filterExpression.getChildren()
					&& filterExpression.getChildren().size() == 0) {
				if (filterExpression instanceof ConditionalExpression) {
					List<ColumnExpression> listOfCol = ((ConditionalExpression) filterExpression)
							.getColumnList();
					for (ColumnExpression expression : listOfCol) 
					{
						setDimAndMsrColumnNode(dimensions, measures,
								(ColumnExpression) expression);
					}

				}
			}
			for (Expression expression : filterExpression.getChildren()) {

				if (expression instanceof ColumnExpression) {
					setDimAndMsrColumnNode(dimensions, measures,
							(ColumnExpression) expression);
				} else if (expression instanceof SparkUnknownExpression) {
					SparkUnknownExpression exp = ((SparkUnknownExpression) expression);
					List<ColumnExpression> listOfColExpression = exp
							.getAllColumnList();
					for (ColumnExpression col : listOfColExpression) {
						setDimAndMsrColumnNode(dimensions, measures, col);
					}
				} else {
					traverseAndSetDimensionOrMsrTypeForColumnExpressions(
							expression, dimensions, measures);
				}
			}
		}

	}
	private static void setDimAndMsrColumnNode(List<Dimension> dimensions,
			List<Measure> measures, ColumnExpression col) {
		Dimension dim;
		Measure msr;
		String columnName;
		columnName = col.getColumnName();
		dim = MolapQueryParseUtil.findDimension(dimensions, columnName);
		col.setDim(dim);
		col.setDimension(true);
		if (null == dim) {
			msr = getMolapMetadataMeasure(columnName, measures);
			col.setDim(msr);
			col.setDimension(false);
        }
	}
	private static Map<Dimension, MolapFilterInfo> getLikeConstaints(
			Map<MolapDimension, List<MolapLikeFilter>> dimensionLikeFilters,
			List<Dimension> dimensions) {
		Map<Dimension, MolapFilterInfo> cons = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
		for(Entry<MolapDimension, List<MolapLikeFilter>> entry : dimensionLikeFilters.entrySet())
		{
			Dimension findDim = MolapQueryParseUtil.findDimension(dimensions, entry.getKey().getDimensionUniqueName());
			MolapFilterInfo createFilterInfo = createLikeFilterInfo(entry.getValue());
			cons.put(findDim, createFilterInfo);
		}
		return cons;
	}

	public static Cube loadSchema(String schemaPath,String schemaName,String cubeName) {
		Schema schema = MolapSchemaParser.loadXML(schemaPath);
		
		MolapMetadata.getInstance().loadSchema(schema);
		Cube cube = null;
		if(schemaName != null && cubeName != null)
		{
			cube = MolapMetadata.getInstance().getCube(schemaName+'_'+cubeName);
		}
		return cube;
	}
	
	public static Cube loadSchema(Schema schema,String schemaName,String cubeName) {
	    
	    MolapMetadata.getInstance().loadSchema(schema);
	    Cube cube = null;
	    if(schemaName != null && cubeName != null)
	    {
	        cube = MolapMetadata.getInstance().getCube(schemaName+'_'+cubeName);
	    }
	    return cube;
	}
	
	public static int getDimensionIndex(Dimension dimension, Dimension[] dimensions)
	{
		int idx = -1;
		
		if(dimension != null)
		{
			for (int i = 0; i < dimensions.length; i++) 
			{
				if(dimensions[i].equals(dimension))
				{
					idx = i;
				}
			}
		}
		return idx;
	}
	
	/**
	 * Get the best suited dimensions from metadata.
	 * @param molapDims
	 * @param dimensions
	 * @return
	 */
	private static Dimension[] getDimensions(List<MolapDimension> molapDims,List<Dimension> dimensions)
	{
		Dimension[] dims = new Dimension[molapDims.size()];
		
		int i = 0;
		for(MolapDimension molapDim : molapDims)
		{
			Dimension findDim = MolapQueryParseUtil.findDimension(dimensions, molapDim.getDimensionUniqueName());
			if(findDim != null)
			{
				findDim.setQueryForDistinctCount(molapDim.isDistinctCountQuery());
				findDim.setQueryOrder(molapDim.getQueryOrder());
				dims[i++] = findDim;
			}
		}
		return dims;
		
	}

//	/**
//	 * Find the dimension from metadata by using unique name. As of now we are taking level name as unique name.
//	 * But user needs to give one unique name for each level,that level he needs to mention in query.
//	 * @param dimensions
//	 * @param molapDim
//	 * @return
//	 */
//	private static Dimension findDimension(List<Dimension> dimensions,String molapDim) 
//	{
//		Dimension findDim = null;
//		for(Dimension dimension : dimensions)
//		{
//			//Its just a temp work around to use level name as unique name. we need to provide a way to configure unique name 
//			//to user in schema.
//			if(dimension.getName().equalsIgnoreCase(molapDim))
//			{
//				findDim = dimension.getDimCopy();
//				findDim.setActualCol(true);
//				break;
//			}
//		}
//		return findDim;
//	}
	
	/**
	 * Find the dimension from metadata by using unique name. As of now we are taking level name as unique name.
	 * But user needs to give one unique name for each level,that level he needs to mention in query.
	 * @param dimensions
	 * @param molapDim
	 * @return
	 * @throws Exception 
	 */
	public static MolapDimension getMolapDimension(List<Dimension> dimensions,String molapDim)
	{
		for(Dimension dimension : dimensions)
		{
			//Its just a temp work around to use level name as unique name. we need to provide a way to configure unique name 
			//to user in schema.
			if(dimension.getName().equalsIgnoreCase(molapDim))
			{
				return new MolapDimension(molapDim);
			}
		}
		return null;
	}
	
	/**
	 * This method returns dimension ordinal for given dimensions name
	 */
	public static int[] getDimensionOrdinal(List<Dimension> dimensions,String[] dimensionNames)
	{
		int[] dimOrdinals = new int[dimensionNames.length];
		int index = 0;
		for (String dimensionName : dimensionNames)
		{
			for (Dimension dimension : dimensions)
			{
				if (dimension.getName().equals(dimensionName))
				{
					dimOrdinals[index++] = dimension.getOrdinal();
					break;
				}
			}
		}

		Arrays.sort(dimOrdinals);
		return dimOrdinals;
	}
	
	/**
	 * Create the molap measures from the requested query model.
	 * @param molapMsrs
	 * @param measures
	 * @param isDetailQuery
	 * @return
	 */
	private static List<Measure> getMeasures(List<MolapMeasure> molapMsrs,List<Measure> measures,boolean isDetailQuery,MolapQueryExecutorModel executorModel)
	{
		List<Measure> reqMsrs = new ArrayList<Measure>(MolapCommonConstants.CONSTANT_SIZE_TEN);
		
		for(MolapMeasure molapMsr : molapMsrs)
		{
			Measure findMeasure = findMeasure(measures, isDetailQuery, molapMsr);
			if(null!=findMeasure){
			findMeasure.setDistinctQuery(molapMsr.isQueryDistinctCount());
			findMeasure.setQueryOrder(molapMsr.getQueryOrder());
			}
			reqMsrs.add(findMeasure);
			if(molapMsr.getSortOrderType() != SortOrderType.NONE) {
				MeasureSortModel sortModel = new MeasureSortModel(findMeasure, molapMsr.getSortOrderType()==SortOrderType.DSC?1:0);
				executorModel.setSortModel(sortModel);
			}
			
		}
		
		return reqMsrs;
	}

	private static Measure findMeasure(List<Measure> measures,
			boolean isDetailQuery, MolapMeasure molapMsr) {
		String aggName = null;
		String name = molapMsr.getMeasure();
		if(!isDetailQuery)
		{
			//we assume the format is like sum(colName). need to handle in proper way.
			int indexOf = name.indexOf("(");
			if(indexOf > 0)
			{
				aggName = name.substring(0, indexOf).toLowerCase(Locale.getDefault());
				name = name.substring(indexOf+1, name.length()-1);
			}
		}
		if(name.equals("*"))
		{
			Measure measure = measures.get(0);
			measure = measure.getCopy();
			measure.setAggName(molapMsr.getAggregatorType() != null?molapMsr.getAggregatorType().getValue().toLowerCase(Locale.getDefault()):aggName);
			return measure;
		}
		for(Measure measure : measures)
		{
			if(measure.getName().equalsIgnoreCase(name))
			{
				measure = measure.getCopy();
				measure.setAggName(molapMsr.getAggregatorType() != null?molapMsr.getAggregatorType().getValue().toLowerCase():measure.getAggName());
				return measure;
			}
		}
		return null;
	}
	
	
	public static MolapMeasure getMolapMeasure(String name,List<Measure> measures)
	{
		
		    //dcd fix
			//String aggName = null;
			String msrName = name;
			//we assume the format is like sum(colName). need to handle in proper way.
			int indexOf = name.indexOf("(");
			if(indexOf > 0)
			{
				//dcd fix
				//aggName = name.substring(0, indexOf).toLowerCase();
				msrName = name.substring(indexOf+1, name.length()-1);
			}
			if(msrName.equals("*"))
			{
				return new MolapMeasure(name);
			}
			for(Measure measure : measures)
			{
				if(measure.getName().equalsIgnoreCase(msrName))
				{
					return new MolapMeasure(name);
				}
			}
		
		return null;
	}
	
	public static Measure getMolapMetadataMeasure(String name,List<Measure> measures)
	{
			for(Measure measure : measures)
			{
				if(measure.getName().equalsIgnoreCase(name))
				{
					return measure;
				}
			}
		return null;
	}
	/**
	 * whether it is a detail query or agg query. If any measures has agg present like sum(col1) then it becomes agg query.
	 * @param molapMsrs
	 * @return
	 */
	/*private static boolean isDetailQuery(List<MolapMeasure> molapMsrs)
	{
		for(MolapMeasure molapMsr : molapMsrs)
		{
			String name = molapMsr.getMeasure();
			//we are just checking for ( present or not but needs to handle properly.
			int indexOf = name.indexOf("(");
			if(indexOf < 0)
			{
				return true;
			}
		}
		return false;
	}*/
	
	/**
	 * Get the constraints from query model
	 * @param queryCons
	 * @param dimensions
	 * @return
	 */
	/*private static Map<Dimension, MolapFilterInfo> getConstaints(Map<MolapDimension, MolapDimensionFilter> queryCons,List<Dimension> dimensions, Map<Dimension, MolapFilterInfo> constraints)
	{
		if(null==constraints)
		{
			constraints = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>();
		}
		
		for(Entry<MolapDimension, MolapDimensionFilter> entry : queryCons.entrySet())
		{
			Dimension findDim = findDimension(dimensions, entry.getKey().getDimensionUniqueName());
			MolapFilterInfo createFilterInfo = createFilterInfo(entry.getValue());
			constraints.put(findDim, createFilterInfo);
		}
		
		return constraints;
	}*/
	
	/**
	 * Create the filter model from the molap query
	 * @param dimensionFilter
	 * @return
	 */
//	private static MolapFilterInfo createFilterInfo(MolapDimensionFilter dimensionFilter)
//	{
//		MolapFilterInfo filterInfo = null;
//		
//		filterInfo = getMolapFilterInfoBasedOnLikeExpressionType(dimensionFilter);
//		if (null != filterInfo) {
//			return filterInfo;
//		}
//		filterInfo = new MolapFilterInfo();
//		filterInfo.addAllIncludedMembers(addBrackets(dimensionFilter
//				.getIncludeFilters()));
//		filterInfo.addAllExcludedMembers(addBrackets(dimensionFilter
//				.getExcludeFilters()));
//		return filterInfo;
//		}
	
	private static MolapFilterInfo createLikeFilterInfo(List<MolapLikeFilter> listOfFilter)
		{
		MolapFilterInfo filterInfo = null;
		filterInfo=getMolapFilterInfoBasedOnLikeExpressionTypeList(listOfFilter);
//		if(null!=filterInfo)
//		{
//			return filterInfo;
//		}
		return filterInfo;
	}
//	private static MolapFilterInfo getMolapFilterInfoBasedOnLikeExpressionType(
//			MolapDimensionFilter dimensionFilter) {
////		filterInfo.getIncludedMembersRanges().addAll(dimensionFilter.getIncludeRangeFilters());
////		filterInfo.getExcludedMembersRanges().addAll(dimensionFilter.getExcludeRangeFilters());
//		if (dimensionFilter instanceof MolapLikeFilter
//				&& null != ((MolapLikeFilter) dimensionFilter)
//						.getLikeFilterExpression())
//		{
//			ContentMatchFilterInfo filterInfo = new ContentMatchFilterInfo();
//			return filterInfo;
//		}
//		return null;
//	}
	private static MolapFilterInfo getMolapFilterInfoBasedOnLikeExpressionTypeList(
			List<MolapLikeFilter> listOfFilter) {
		List<FilterLikeExpressionIntf> listOfFilterLikeExpressionIntf = new ArrayList<FilterLikeExpressionIntf>(MolapCommonConstants.CONSTANT_SIZE_TEN);
		ContentMatchFilterInfo filterInfo = new ContentMatchFilterInfo();
		for (MolapLikeFilter molapLikeFilter : listOfFilter) {
			listOfFilterLikeExpressionIntf.add(molapLikeFilter
					.getLikeFilterExpression());
		}
		filterInfo.setLikeFilterExpression(listOfFilterLikeExpressionIntf);
		return filterInfo;
	}
	
	/*private static List<String> addBrackets(List<String> filters)
	{
		List<String> updated = new ArrayList<String>();
		for(String filter : filters)
		{
			updated.add('['+filter+']');
		}
		return updated;
	}*/
	
	
	
	/*private static Map<Measure, MeasureFilterModel[]> convertMsrFilters(Map<MolapMeasure, List<MolapMeasureFilter>> measureFilters,List<Measure> msrs,boolean isDetailQuery,MolapQueryExecutorModel executorModel,List<Dimension> dimensions)
	{
		Map<Measure, MeasureFilterModel[]> converted = new HashMap<MolapMetadata.Measure, MeasureFilterModel[]>();
		for(Entry<MolapMeasure, List<MolapMeasureFilter>> entry : measureFilters.entrySet())
		{
			Measure findMeasure = findMeasure(msrs, isDetailQuery, entry.getKey());
			List<MeasureFilterModel> filterModels = new ArrayList<MeasureFilterModel>();
			
			for (int i = 0; i < entry.getValue().size(); i++) 
			{
				MolapMeasureFilter molapMeasureFilter = entry.getValue().get(i);
				MolapDimension dimension = molapMeasureFilter.getDimension();
				if(dimension != null)
				{
					Dimension findDimension = findDimension(dimensions, dimension.getDimensionUniqueName());
					executorModel.setMsrFilterDimension(findDimension);
				}
				if(molapMeasureFilter.getFilterType().equals(MolapMeasureFilterType.BETWEEN))
				{
					filterModels.add(new MeasureFilterModel(molapMeasureFilter.getOperandOne(), MeasureFilterType.GREATER_THAN));
					filterModels.add(new MeasureFilterModel(molapMeasureFilter.getOperandTwo(), MeasureFilterType.LESS_THAN));
				}
				else
				{
					filterModels.add(new MeasureFilterModel(molapMeasureFilter.getOperandOne(), MeasureFilterType.valueOf(molapMeasureFilter.getFilterType().name())));
				}
				
			}
			converted.put(findMeasure, filterModels.toArray(new MeasureFilterModel[filterModels.size()]));
		}
		
		if(executorModel.getMsrFilterDimension() == null && executorModel.getDims().length > 0)
		{
			executorModel.setMsrFilterDimension(executorModel.getDims()[executorModel.getDims().length-1]);
		}
		
		return converted;
	}*/
	
	/**
     * Get the measure filter array for each measure asked.
     * @param msrFilterMap
     * @param msrs
     * @return
     */
    /*private static MeasureFilterModel[][] getMeasureFilterArray(Map<Measure, MeasureFilterModel[]> msrFilterMap, List<Measure> msrs)
    {
        for(Entry<Measure, MeasureFilterModel[]> entry : msrFilterMap.entrySet())
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
        boolean msrFilterExist = false;
        int i=0;
        for(Measure measure : msrs)
        {
            for(Entry<Measure,MeasureFilterModel[]> entry : msrFilterMap.entrySet())
            {
                
                Measure key = entry.getKey();
                if(measure.getName().equals(key.getName()))
                {
                    filterModels[i] = entry.getValue();
                    msrFilterExist = true;
                    break;
                }
            }
            
            i++;
        }
        if(!msrFilterExist)
        {
        	return null;
        }
        return filterModels;
        
    }*/
    
//    /**
//     * Its just a temp method.
//     * @return
//     */
//	private static HbaseDataSourceImpl getDataSource()
//	{
//        //TODO change the hard coded values for hbase configuration for demo .
//        Util.PropertyList connectInfo = new Util.PropertyList();
//        connectInfo.put(RolapConnectionProperties.Jdbc.name(), "jdbc:hbase://master:2181");
//        connectInfo.put(RolapConnectionProperties.JdbcDrivers.name(), "com.huawei.unibi.molap.engine.datasource.HbaseDataSourceImpl");
//        HbaseDataSourceImpl hbaseDataSourceImpl = new HbaseDataSourceImpl(connectInfo);
//		return hbaseDataSourceImpl;
//	}
//	
	/**
	 * It creates the one split for each region server. 
	 * @param factTable
	 * @param aggModel
	 * @return
	 * @throws IOException
	 */
	public static synchronized TableSplit[] getTableSplits(String schemaName, String cubeName,MolapQueryPlan queryPlan, Partitioner partitioner) throws IOException
    {
 
    	//Just create splits depends on locations of region servers 
    	List<Partition> allPartitions = null; 
    	if(queryPlan == null)
    	{
    	    allPartitions = QueryPartitionHelper.getInstance().getAllPartitions(schemaName, cubeName, partitioner);
    	}
    	else
    	{
    	    allPartitions = QueryPartitionHelper.getInstance().getPartitionsForQuery(queryPlan, partitioner);
    	}
    	TableSplit[] splits = new TableSplit[allPartitions.size()];
    	for(int i = 0;i < splits.length;i++)
        {
            splits[i] = new TableSplit();
            List<String> locations = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            Partition partition = allPartitions.get(i);
            String location = QueryPartitionHelper.getInstance().getLocation(partition, schemaName, cubeName, partitioner);
            locations.add(location);
            splits[i].setPartition(partition);
            splits[i].setLocations(locations);
        }
    	
		return splits;
    }
	
	/**
	 * It creates the one split for each region server. 
	 * @param factTable
	 * @param aggModel
	 * @return
	 * @throws Exception 
	 */
	public static TableSplit[] getTableSplitsForDirectLoad(String sourcePath,  String [] nodeList, int partitionCount) throws Exception
    {
 
    	//Just create splits depends on locations of region servers 
    	FileType fileType = FileFactory.getFileType(sourcePath);
    	DefaultLoadBalancer loadBalancer = null;
    	List<Partition> allPartitions=getAllFilesForDataLoad(sourcePath, fileType,partitionCount);
		loadBalancer = new DefaultLoadBalancer(Arrays.asList(nodeList), allPartitions);
    	TableSplit[] tblSplits = new TableSplit[allPartitions.size()]; 
    	for(int i = 0;i < tblSplits.length;i++)
        {
            tblSplits[i] = new TableSplit();
            List<String> locations = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
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
	 * @param factTable
	 * @param aggModel
	 * @return
	 * @throws Exception 
	 */
	public static TableSplit[] getPartitionSplits(String sourcePath,  String [] nodeList, int partitionCount) throws Exception
    {
 
    	//Just create splits depends on locations of region servers 
    	FileType fileType = FileFactory.getFileType(sourcePath);
    	DefaultLoadBalancer loadBalancer = null;
    	List<Partition> allPartitions=getAllPartitions(sourcePath, fileType,partitionCount);
		loadBalancer = new DefaultLoadBalancer(Arrays.asList(nodeList), allPartitions);
    	TableSplit[] splits = new TableSplit[allPartitions.size()];
    	for(int i = 0;i < splits.length;i++)
        {
            splits[i] = new TableSplit();
            List<String> locations = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            Partition partition = allPartitions.get(i);
            String location = loadBalancer.getNodeForPartitions(partition);
            locations.add(location);
            splits[i].setPartition(partition);
            splits[i].setLocations(locations);
        }
		return splits;
    }
	
	public static void getAllFiles(String sourcePath, List<String> partitionsFiles, FileType fileType) throws Exception
	{
        
        if(!FileFactory.isFileExist(sourcePath, fileType, false))
        {
            throw new Exception("Source file doesn't exist at path: " + sourcePath);
        }
        
        MolapFile file = FileFactory.getMolapFile(sourcePath, fileType); 
        if(file.isDirectory())
        {
        	MolapFile[] fileNames = file.listFiles(new MolapFileFilter()
            {
                @Override
                public boolean accept(MolapFile pathname)
                {
                    return true;
                }
            });
        	for (int i = 0; i < fileNames.length; i++) {
        		getAllFiles(fileNames[i].getPath(),partitionsFiles, fileType);
			}
        }
        else
        {
            partitionsFiles.add(file.getPath());
        }
	}
	
	private static List<Partition> getAllFilesForDataLoad(String sourcePath,FileType fileType, int partitionCount) throws Exception
	{
	    List<String> files = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
	    getAllFiles(sourcePath, files, fileType);
	    List<Partition> partitionList = new ArrayList<Partition>(MolapCommonConstants.CONSTANT_SIZE_TEN);
	    Map<Integer,List<String>> partitionFiles = new HashMap<Integer, List<String>>();
	    
	    for(int i = 0;i<partitionCount;i++)
	    {
	    	partitionFiles.put(i, new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN));
	    	partitionList.add(new PartitionMultiFileImpl(i+"", partitionFiles.get(i)));
	    }
	    for(int i = 0;i < files.size();i++)
        {
	    	partitionFiles.get(i%partitionCount).add(files.get(i));
        }
	    return partitionList;
	}
	
	private static List<Partition> getAllPartitions(String sourcePath,FileType fileType, int partitionCount) throws Exception
	{
	    List<String> files = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
	    getAllFiles(sourcePath, files, fileType);
	    int[] numberOfFilesPerPartition = getNumberOfFilesPerPartition(files.size(), partitionCount);
	    int startIndex=0;
	    int endIndex=0;
	    List<Partition> partitionList = new ArrayList<Partition>(MolapCommonConstants.CONSTANT_SIZE_TEN);
	    if(numberOfFilesPerPartition!=null){
	    for(int i = 0;i < numberOfFilesPerPartition.length;i++)
        {
	        List<String> partitionFiles = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
	        endIndex+=numberOfFilesPerPartition[i];
            for(int j = startIndex;j < endIndex;j++)
            {
                partitionFiles.add(files.get(j));
            }
            startIndex+=numberOfFilesPerPartition[i];
	        partitionList.add(new PartitionMultiFileImpl(i+"", partitionFiles));
        }
	    }
	    return partitionList;
	}
	
	   private static int[] getNumberOfFilesPerPartition(int numberOfFiles, int partitionCount)
	    {
	       int div = numberOfFiles / partitionCount;
	       int mod = numberOfFiles % partitionCount;
	       int[] numberOfNodeToScan = null;
	        if(div > 0)
	        {
	            numberOfNodeToScan = new int[partitionCount];
	            Arrays.fill(numberOfNodeToScan, div);
	        }
	        else if(mod > 0)
	        {
	            numberOfNodeToScan = new int[mod];
	        }
	        for(int i = 0;i < mod;i++)
	        {
	            numberOfNodeToScan[i] = numberOfNodeToScan[i] + 1;
	        }
	        return numberOfNodeToScan;
	    }
    
//    public static void createDataSource(String path, String partitionID)
//    {
//        PropertyList makeConnectString = makeConnectString(path);
//        if(partitionID!=null)
//        {
//            Schema schema = updateSchemaWithPartition(path, partitionID);
//            
//            String partitioBasedSchema = schema.toXML();
//            
//            makeConnectString.put(RolapConnectionProperties.CatalogContent.name(), partitioBasedSchema);
//        }
//        
//        DataSource molapDataSource = new MolapDataSourceFactory().getMolapDataSource(makeConnectString);
//		Connection connection = DriverManager.getConnection(makeConnectString, null,molapDataSource);
//    }
    
//    public static void createDataSource(String path, String partitionID,List<String> listOfLoadFolders)
//    {
//        PropertyList makeConnectString = makeConnectString(path);
//        if(partitionID!=null)
//        {
//            Schema schema = updateSchemaWithPartition(path, partitionID);
//            
//            String partitioBasedSchema = schema.toXML();
//            
//            makeConnectString.put(RolapConnectionProperties.CatalogContent.name(), partitioBasedSchema);
//        }
//        
//        DataSource molapDataSource = new MolapDataSourceFactory().getMolapDataSource(makeConnectString);
//		Connection connection = DriverManager.getConnection(makeConnectString, null,molapDataSource);
		
//		 Schema schema = MolapSchemaParser.loadXML(path);
//		 
//		 createDataSource(schema, partitionID,listOfLoadFolders,null,null);
//    }
    
//    public static void createDataSource(Schema schema, String partitionID)
//    {
//        PropertyList makeConnectString = makeConnectString(null);
//        if(partitionID!=null)
//        {
//            schema = updateSchemaWithPartition(schema, partitionID);
//            
//            String partitioBasedSchema = schema.toXML();
//            
//            makeConnectString.put(RolapConnectionProperties.CatalogContent.name(), partitioBasedSchema);
//        }
//        
//        DataSource molapDataSource = new MolapDataSourceFactory().getMolapDataSource(makeConnectString);
//        Connection connection = DriverManager.getConnection(makeConnectString, null,molapDataSource);
//    }
    
    public static void createDataSource(int currentRestructNumber, Schema schema, Cube cube, String partitionID,List<String> sliceLoadPaths,List<String> sliceUpdatedLoadPaths,String factTableName, long cubeCreationTime)
    {
//        PropertyList makeConnectString = makeConnectString(null);
//        if(partitionID!=null)
//        {
//            schema = updateSchemaWithPartition(schema, partitionID);
//            
//            String partitioBasedSchema = schema.toXML();
//            
//            makeConnectString.put(RolapConnectionProperties.CatalogContent.name(), partitioBasedSchema);
//        }
//        
//        DataSource molapDataSource = new MolapDataSourceFactory().getMolapDataSource(makeConnectString);
//        Connection connection = DriverManager.getConnection(makeConnectString, null,molapDataSource);
        String basePath = MolapUtil.getCarbonStorePath(schema.name, schema.cubes[0].name)/*MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)*/;
    	InMemoryCubeStore.getInstance().loadCube(schema, cube, partitionID,sliceLoadPaths,sliceUpdatedLoadPaths,factTableName,basePath, currentRestructNumber, cubeCreationTime);
    }   
    
    public static void createDataSource(int currentRestructNumber, Schema schema, Cube cube, String partitionID,
            List<String> sliceLoadPaths, List<String> sliceUpdatedLoadPaths,
            String factTableName, String basePath, long cubeCreationTime)
    {
        InMemoryCubeStore.getInstance().loadCube(schema, cube,
                partitionID, sliceLoadPaths, sliceUpdatedLoadPaths,
                factTableName, basePath, currentRestructNumber, cubeCreationTime);
    }
    
    
    
    

	public static Schema updateSchemaWithPartition(Schema schema,
			String partitionID) {
		
		String originalSchemaName = schema.name;
		String originalCubeName = schema.cubes[0].name;
		schema.name = originalSchemaName + '_' + partitionID;
		schema.cubes[0].name = originalCubeName + '_' + partitionID;
		return schema;
	}
	
	public static Schema updateSchemaWithPartition(String path,
	        String partitionID) {
	    Schema schema = MolapSchemaParser.loadXML(path);
	    
	    String originalSchemaName = schema.name;
	    String originalCubeName = schema.cubes[0].name;
	    schema.name = originalSchemaName + '_' + partitionID;
	    schema.cubes[0].name = originalCubeName + '_' + partitionID;
	    return schema;
	}
    
    
	public static boolean isQuickFilter(MolapQueryExecutorModel molapQueryModel) {
		return ("true".equals(MolapProperties.getInstance().getProperty(
				MolapCommonConstants.CARBON_ENABLE_QUICK_FILTER))
				&& null == molapQueryModel.getFilterExpression()
				&& molapQueryModel.getDims().length == 1
				&& molapQueryModel.getMsrs().size() == 0
				&& molapQueryModel.getDimensionAggInfo().size() == 0
				&& molapQueryModel.getExpressions().size() == 0 && !molapQueryModel
					.isDetailQuery());
	}
    
	public static String[] getAllColumns(Schema schema)
	{
	    String cubeUniqueName = schema.name+'_'+schema.cubes[0].name;
	    MolapMetadata.getInstance().removeCube(cubeUniqueName);
		MolapMetadata.getInstance().loadSchema(schema);
        Cube cube = MolapMetadata.getInstance().getCube(cubeUniqueName);
		Set<String> metaTableColumns = cube.getMetaTableColumns(((Table)schema.cubes[0].fact).name);
		return metaTableColumns.toArray(new String[metaTableColumns.size()]);
	}
    
//    public static MolapExecutor getMolapExecutor(Cube cube)
//    {
//    	 InMemoryQueryExecutor executor = new InMemoryQueryExecutor(cube.getDimensions(cube.getFactTableName()),cube.getSchemaName(), cube.getOnlyCubeName());
//    	 return executor;
//    }

//    public static MolapExecutor getMolapExecutor(Cube cube, String factTable)
//    {
//    	 InMemoryQueryExecutor executor = new InMemoryQueryExecutor(cube.getDimensions(factTable),cube.getSchemaName(), cube.getOnlyCubeName());
//    	 return executor;
//    }
    
    public static QueryExecutor getQueryExecuter(Cube cube, String factTable)
    {
    	QueryExecutor executer = new QueryExecutorImpl(cube.getDimensions(factTable),cube.getSchemaName(), cube.getOnlyCubeName());
    	return executer;
    }
    
    /**
     * 
     * @param schema
     * @param queryModel
     */
	public  static void updateDimensionWithHighCardinalityVal(
			MolapDef.Schema schema, MolapQueryExecutorModel queryModel) {

		CubeDimension[] cubeDimensions = schema.cubes[0].dimensions;
		Dimension[] metadataDimensions = queryModel.getDims();

		for (Dimension metadataDimension : metadataDimensions) {
			for (CubeDimension cubeDimension : cubeDimensions) {
				if (metadataDimension.getName().equals(cubeDimension.name)
						&& cubeDimension.highCardinality) {
					metadataDimension
							.setHighCardinalityDims(cubeDimension.highCardinality);
					break;
				}

			}
		}

	}
    
    /**
     * 
     * @param queryModel
     * @param listLoadFolders
     * @param cubeUniqueName
     * @return
     * @throws Exception
     * 
     */
    public static List<String> validateAndLoadRequiredSlicesInMemory(
            MolapQueryExecutorModel queryModel, List<String> listLoadFolders,
            String cubeUniqueName) throws RuntimeException
    {
        Set<String> columns = getColumnList(queryModel, cubeUniqueName);
        return loadRequiredLevels(listLoadFolders, cubeUniqueName, columns);
    }

    /**
     * 
     * @param listLoadFolders
     * @param cubeUniqueName
     * @param columns
     * @return
     * @throws Exception
     * 
     */
    public static List<String> loadRequiredLevels(List<String> listLoadFolders,
            String cubeUniqueName, Set<String> columns) throws RuntimeException
    {
        List<String> levelCacheKeys = InMemoryCubeStore.getInstance()
                .loadRequiredLevels(cubeUniqueName, columns, listLoadFolders);
        return levelCacheKeys;
    }
    
    /**
     * This method will return the name of the all the columns reference in the
     * query
     * 
     * @param queryModel
     * @return
     * 
     */
    public static Set<String> getColumnList(MolapQueryExecutorModel queryModel, String cubeUniqueName)
    {
        Set<String> queryColumns = new HashSet<String>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        Expression filterExpression = queryModel.getFilterExpression();
        Cube cube = queryModel.getCube();
        List<Dimension> dimensions = cube
                .getDimensions(cube.getFactTableName());
        LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "@@@@Dimension size :: " + dimensions.size());
        if(dimensions.isEmpty())
        {
        	cube = MolapMetadata.getInstance().getCube(cubeUniqueName);
        	dimensions = cube
                    .getDimensions(cube.getFactTableName());
        }
        List<Measure> measures = cube.getMeasures(cube.getFactTableName());
        traverseAndPopulateColumnList(filterExpression, dimensions, measures,
                queryColumns);
        Dimension[] dims = queryModel.getDims();
        for(int i = 0;i < dims.length;i++)
        {
            queryColumns.add(dims[i].getColName());
        }
        dims = queryModel.getSortedDimensions();
        for(int i = 0;i < dims.length;i++)
        {
            queryColumns.add(dims[i].getColName());
        }
        List<DimensionAggregatorInfo> dimensionAggInfo = queryModel
                .getDimensionAggInfo();
        for(DimensionAggregatorInfo dimAggInfo : dimensionAggInfo)
        {
            Dimension dim = MolapQueryParseUtil.findDimension(dimensions,
                    dimAggInfo.getColumnName());
            if(null != dim)
            {
                queryColumns.add(dim.getColName());
            }
        }
        addCustomExpressionColumnsToColumnList(queryModel.getExpressions(),
                queryColumns, dimensions);
        return queryColumns;
    }

    /**
     * 
     * @param expressions
     * @param queryColumns
     * @param dimensions
     * 
     */
    private static void addCustomExpressionColumnsToColumnList(
            List<CustomMolapAggregateExpression> expressions,
            Set<String> queryColumns, List<Dimension> dimensions)
    {
        for(CustomMolapAggregateExpression expression : expressions)
        {
            List<Dimension> referredColumns = expression.getReferredColumns();
            for(Dimension refColumn : referredColumns)
            {
                Dimension dim = MolapQueryParseUtil.findDimension(dimensions,
                        refColumn.getColName());
                if(null != dim)
                {
                    queryColumns.add(dim.getColName());
                }
            }
        }
    }

    /**
     * This method will traverse and populate the column list
     * 
     * @param filterExpression
     * @param dimensions
     * @param measures
     * @param columns
     * 
     */
    private static void traverseAndPopulateColumnList(
            Expression filterExpression, List<Dimension> dimensions,
            List<Measure> measures, Set<String> columns)
    {
        if(null != filterExpression)
        {
            if(null != filterExpression.getChildren()
                    && filterExpression.getChildren().size() == 0)
            {
                if(filterExpression instanceof ConditionalExpression)
                {
                    List<ColumnExpression> listOfCol = ((ConditionalExpression)filterExpression)
                            .getColumnList();
                    for(ColumnExpression expression : listOfCol)
                    {
                        columns.add(expression.getColumnName());
                    }
                }
            }
            for(Expression expression : filterExpression.getChildren())
            {
                if(expression instanceof ColumnExpression)
                {
                    columns.add(((ColumnExpression)expression).getColumnName());
                }
                else if(expression instanceof SparkUnknownExpression)
                {
                    SparkUnknownExpression exp = ((SparkUnknownExpression)expression);
                    List<ColumnExpression> listOfColExpression = exp
                            .getAllColumnList();
                    for(ColumnExpression col : listOfColExpression)
                    {
                        columns.add(col.getColumnName());
                    }
                }
                else
                {
                	traverseAndPopulateColumnList(
                            expression, dimensions, measures, columns);
                }
            }
        }
    }
    
    /**
     * 
     * @param details
     * @return
     * 
     */
    public static List<String> getListOfSlices(LoadMetadataDetails[] details)
    {
        List<String> slices = new ArrayList<>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        if(null != details)
        {
	        for(LoadMetadataDetails oneLoad : details)
	        {
	            if(!MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(oneLoad
	                    .getLoadStatus()))
	            {
	                String loadName = MolapCommonConstants.LOAD_FOLDER
	                        + oneLoad.getLoadName();
	                slices.add(loadName);
	            }
	        }
        }
        return slices;
    }
    
    /**
     * 
     * @param listOfLoadFolders
     * @param columns
     * @param schemaName
     * @param cubeName
     * @param partitionCount
     * 
     */
    public static void clearLevelLRUCacheForDroppedColumns(
            List<String> listOfLoadFolders, List<String> columns,
            String schemaName, String cubeName, int partitionCount)
    {
        MolapQueryParseUtil.removeDroppedColumnsFromLevelLRUCache(
                listOfLoadFolders, columns, schemaName, cubeName,
                partitionCount);
    }
	
	/**
	 * whether the start key is present in the list of keys.
	 * @param startKeys
	 * @param key
	 * @return
	 */
//	private static boolean isPresent(List<byte[]> startKeys,byte[] key)
//	{
//		boolean present = false;
//		for(byte[] stKey : startKeys)
//		{
//			if(Arrays.equals(stKey, key))
//			{
//				present = true;
//				break;
//			}
//		}
//		
//		return present;
//	}
	
//    public static Util.PropertyList makeConnectString(String path) 
//    {
//        String connectString = null;
//
//        Util.PropertyList connectProperties;
//        if (connectString == null || connectString.equals("")) 
//        {
//            // create new and add provider
//            connectProperties = new Util.PropertyList();
//            connectProperties.put(
//                RolapConnectionProperties.Provider.name(),
//                "mondrian");
//        } else {
//            // load with existing connect string
//            connectProperties = Util.parseConnectString(connectString);
//        }
//
//        // override jdbc urlmondrian.jdbcURL
//        String jdbcURL = MolapProperties.getInstance().getProperty("mondrian.jdbcURL","molap://sourceDB=rdbmscon;mode=file");//"jdbc:hsqldb:hsql://localhost:9001/sampledata";
//
//
//        if (jdbcURL != null) {
//            // add jdbc url to connect string
//            connectProperties.put(
//                RolapConnectionProperties.Jdbc.name(),
//                jdbcURL);
//        }
//
//        // override jdbc drivers
//        String jdbcDrivers = MolapProperties.getInstance().getProperty("mondrian.jdbcDrivers","com.huawei.unibi.molap.engine.datasource.MolapDataSourceImpl");//"org.hsqldb.jdbcDriver";
//
//        if (jdbcDrivers != null) {
//            // add jdbc drivers to connect string
//            connectProperties.put(
//                RolapConnectionProperties.JdbcDrivers.name(),
//                jdbcDrivers);
//        }
//
//        // override catalog url
//        String catalogURL = MolapProperties.getInstance().getProperty("mondrian.catalogURL",path);//"D:/Cloud/Pentaho/biserver-ce-3.9.0-stable/biserver-ce/pentaho-solutions/steel-wheels/analysis/steelwheels.mondrian.xml";
//
//        if (catalogURL != null) {
//            // add catalog url to connect string
//            connectProperties.put(
//                RolapConnectionProperties.Catalog.name(),
//                catalogURL);
//        }
//
//        // override JDBC user
//        String jdbcUser = "pentaho_user";
//
//
//        if (jdbcUser != null) {
//            // add user to connect string
//            connectProperties.put(
//                RolapConnectionProperties.JdbcUser.name(),
//                jdbcUser);
//        }
//
//        // override JDBC password
//        String jdbcPassword = "password";
//
//        if (jdbcPassword != null) {
//            // add password to connect string
//            connectProperties.put(
//                RolapConnectionProperties.JdbcPassword.name(),
//                jdbcPassword);
//        }
//		return connectProperties;
//
//    }
	


//    /**
//     * Sample query plan for testing
//     * @return
//     */
//    public static MolapQueryPlan getSampleMolapQueryPlan()
//    {
//    	MolapQueryPlan plan = new MolapQueryPlan("DETAIL_UFDR_Streaming", "DETAIL_UFDR_Streaming");
//    	String[] dims = new String[]{"msisdn","begin_time","prot_category","prot_type","get_streaming_flag","disconnection_flag","intbuffer_full_flag","failure_code","rat","mcc","mnc","lac","ci","sac","eci","rac"};
//    	for (int i = 0; i < dims.length; i++) 
//    	{
//    		plan.addDimension(new MolapDimension(dims[i]));
//		}
//    	String[] measures = new String[]{"network_ul_traffic", "network_dl_traffic","l4_dw_throughput", "l4_dw_throughput", "l4_ul_packets"};
////    	String[] measures = new String[]{"sum(network_ul_traffic)", "sum(network_dl_traffic)","sum(l4_dw_throughput)", "sum(l4_dw_throughput)", "sum(l4_ul_packets)",};
//    	for (int i = 0; i < measures.length; i++) 
//    	{
//    		plan.addMeasure(new MolapMeasure(measures[i]));
//		}
//    	
//    	MolapDimensionFilter time_filter = new MolapDimensionFilter();
//    	time_filter.addIncludeRangeFilter(new long[]{1370028539,1370528739});
//    	plan.setDimensionFilter(new MolapDimension("begin_time"), time_filter);
//    	
//       	return plan;
//    }

    
   /* public static void main(String[] args)
    {
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","D:/f/SVN/TRP/2014/SparkOLAPRavi/steelwheels.molap.xml","hdfs://master:54310/opt/ravi/store");
//		exec.init();
//		System.out.println( exec.execute("SELECT Territory, Quantity FROM  SteelWheelsSales ORDER BY Territory ASC", null, null));
		
//		System.out.println(exec.execute("select City,Quantity from SteelWheelsSales"));
	}*/

}
