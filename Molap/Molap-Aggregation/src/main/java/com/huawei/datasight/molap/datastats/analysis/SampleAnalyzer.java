package com.huawei.datasight.molap.datastats.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.datastats.LoadSampler;
import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.util.DataStatsUtil;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.dimension.DimensionAggregatorInfo;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.QueryExecutor;
import com.huawei.unibi.molap.engine.expression.ColumnExpression;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.LiteralExpression;
import com.huawei.unibi.molap.engine.expression.conditional.InExpression;
import com.huawei.unibi.molap.engine.expression.conditional.ListExpression;
import com.huawei.unibi.molap.engine.holders.MolapResultHolder;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.olap.SqlStatement.Type;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * this class will read some sample data from store and analyze how its spread
 * on complete store
 * 
 * @author A00902717
 *
 */
public class SampleAnalyzer
{

	private static final LogService LOGGER = LogServiceFactory
			.getLogService(SampleAnalyzer.class.getName());
	private LoadSampler loadSampler;
	private Cube cube;
	
	
	private String cubeUniqueName;
	
	private Level[] result;

	public SampleAnalyzer(LoadSampler loadSampler,Level[] result)
	{

		this.loadSampler = loadSampler;
		this.cube = loadSampler.getMetaCube();
		this.result=result;
		this.cubeUniqueName=loadSampler.getCubeUniqueName();

	}

	public void execute(List<Level> dimOrdCard, String partitionId) throws AggSuggestException
	{
		int totalDimensions=loadSampler.getDimensions().size();
		
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Processing cardinality:" + dimOrdCard);
		for (int i = 0; i < dimOrdCard.size(); i++)
		{

			Level masterDim = dimOrdCard.get(i);
			//this array will have distinct relation of master dimension w.r.t to other dimension
			
			// setting default value to 1. We have considered totalDimensions because there is ignore_cardinality which is 
			//not part of dimOrdCard and hence indexoutbound exception.
			//TO-DO explain this scenarion in more detail
			Map<Integer,Integer> distinctOfMasterDim=new HashMap<Integer,Integer>(totalDimensions);
			
			for (int j = 0; j < i; j++)
			{
				// Step 1: we need to find distinct data of dimCO in earlier
				// calculated dimensions
				Level dim = dimOrdCard.get(j);
				// get distinctdata mapping for jth dimension ordinal
				Level dimDistinctData =null;
				for(Level level:result)
				{
					if(level.getOrdinal()==dim
						.getOrdinal())
					{
						dimDistinctData=level;
						break;
					}
				}
				// this will give distinct data of dim w.r.t dimCO
				int distinct=0;
				//findbug fix
				if(null!=dimDistinctData&&null!=dimDistinctData.getOtherDimesnionDistinctData())
				{
					Integer distinctInt = dimDistinctData.getOtherDimesnionDistinctData().get(masterDim.getOrdinal());
					if(null!=distinctInt)
					{
						distinct=distinctInt;
					}
				}
				/**
				 * formulae is (earlier computed cardinality)*distinct/current
				 * cardinality
				 */
				// this will calculate distinct data of dimCO w.r.t dim
				int res = (dim.getCardinality() * distinct)
						/ masterDim.getCardinality();
				distinctOfMasterDim.put(dim.getOrdinal(),res);
				
			}
			LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "From 0 to "
					+ i + " is calculated data");
			// find out distinct data for remaining dimensions
			ArrayList<Level> leftOut = new ArrayList<Level>(dimOrdCard.size());
			//It is i+1 because we are calculating distinct data for  ith ordinal
			for (int k = i; k < dimOrdCard.size(); k++)
			{
				leftOut.add(dimOrdCard.get(k));

			}
			if (leftOut.size() > 1)
			{
				//read sample data from fact and query all record for given sample on BPlus tree
				queryData(masterDim, leftOut,distinctOfMasterDim);
				
			}
			//setting result
			masterDim.setOtherDimesnionDistinctData(distinctOfMasterDim);
			//logResult(masterDim, distinctOfMasterDim);
			
		}
	}

	private void queryData(Level master,
			ArrayList<Level> slaves, Map<Integer,Integer> distinctOfMasterDim) throws AggSuggestException
	{
		List<Dimension> allDimensions = loadSampler.getDimensions();
		Dimension dimension=null;
		for(Dimension dim:allDimensions)
		{
			if(dim.getOrdinal()==master.getOrdinal())
			{
				dimension=dim;
				break;
			}
		}
		//findbug fix
		if(null==dimension)
		{
			throw new AggSuggestException("dimension:"+master.getName()+", is not available in visible dimension.");
		}
		// Sample data
		List<String> realDatas = loadSampler.getSampleData(dimension,
				cubeUniqueName);
		
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Load size for dimension[" + dimension.getColName() + "]:"
						+ realDatas.size());
		long queryExecutionStart = System.currentTimeMillis();
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Started with sample data:" + realDatas);
		//query on bplus tree
		querySampleDataOnBTree(dimension,
				realDatas, slaves,distinctOfMasterDim);

		long queryExecutionEnd = System.currentTimeMillis();
		long queryExecutionTimeTaken = queryExecutionEnd - queryExecutionStart;
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Finished with sample data,time taken[ms]:"
						+ queryExecutionTimeTaken);

		
	}

	/**
	 * 
	 * @param master
	 *            : master dimension
	 * @param tableName
	 * @param data
	 *            : member value of given dimension
	 * @param distinctOfMasterDim 
	 * @param highCardinalities
	 * @return dimension's ordinal and its distinct value for given member value
	 * @throws AggSuggestException 
	 */
	public void querySampleDataOnBTree(
			Dimension master, List<String> data,
			ArrayList<Level> slaves, Map<Integer,Integer> distinctOfMasterDim) throws AggSuggestException
	{

		ResultAnalyzer resultAnalyzer = new ResultAnalyzer(master);

		
		try
		{
			Set<String> columnsForQuery=new HashSet<String>(10);
			// Create QueryExecution model
			MolapQueryExecutorModel queryExecutionModel=createQueryExecutorModel(master, data, slaves,columnsForQuery);
			List<String> levelCacheKeys=null;
			if (InMemoryCubeStore.getInstance().isLevelCacheEnabled()) 
			{
				levelCacheKeys = DataStatsUtil.validateAndLoadRequiredSlicesInMemory(loadSampler.getAllLoads(), cubeUniqueName, columnsForQuery);
	        }
			cube = MolapMetadata.getInstance().getCube(cubeUniqueName);
			queryExecutionModel.setCube(cube);
			QueryExecutor queryExecutor= DataStatsUtil.getQueryExecuter(queryExecutionModel.getCube(),
					queryExecutionModel.getFactTable());
			
			// Execute the query
			MolapIterator<RowResult> rowIterator = queryExecutor
					.execute(queryExecutionModel);

			//delegate result analysiss
			//analyzed result will be set in distinctOfMasterDim
			resultAnalyzer.analyze(rowIterator, slaves,distinctOfMasterDim);
			if(null!=levelCacheKeys)
			{
				for(String levelCacheKey:levelCacheKeys)
				{
					InMemoryCubeStore.getInstance().updateLevelAccessCountInLRUCache(levelCacheKey);
				}
			}

		}
		catch (Exception e)
		{
			throw new AggSuggestException(e.getMessage(), e);
		}

	}

	
	/**
	 * Creating query model to execute on BPlus tree
	 * @param master
	 * @param datas
	 * @param slaves
	 * @param columnsForQuery 
	 * @return
	 */
	public MolapQueryExecutorModel createQueryExecutorModel(Dimension master,
			List<String> datas, ArrayList<Level> slaves, Set<String> columnsForQuery)
	{
		MolapQueryExecutorModel executorModel = new MolapQueryExecutorModel();
		executorModel.setSparkExecution(true);
		String factTableName = cube.getFactTableName();
		executorModel.setCube(cube);
        executorModel.sethIterator(new MolapResultHolder(new ArrayList<Type>(1)));
        executorModel.setFactTable(factTableName);
        
        List<Dimension> allDimensions = loadSampler.getDimensions();
        Dimension[] dimensions=new Dimension[slaves.size()];
        int index=0;
        for(Level slave:slaves)
        {
        	for(Dimension dimension:allDimensions)
        	{
        		if(dimension.getOrdinal()==slave.getOrdinal())
        		{
        			dimensions[index]=dimension;
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
     		((ColumnExpression)left).setDim(master);
    		((ColumnExpression)left).setDimension(true);
    		
    		
    		
     		// creating all data as LiteralExpression and adding to list
     		List<Expression> exprs = new ArrayList<Expression>(datas.size());
     		for (String data : datas)
     		{
     			Expression right = new LiteralExpression(data,
     					DataStatsUtil.getDataType(master.getDataType()));
     			exprs.add(right);
     		}

     		// Creating listexpression so that it can be used in InExpression
     		Expression listExpr = new ListExpression(exprs);
     		InExpression inExpr = new InExpression(left, listExpr);

     		// EqualToExpression equalExpr = new EqualToExpression(left, right);
           executorModel.setFilterExpression(inExpr);
			
           
           List<DimensionAggregatorInfo> dimensionAggregatorInfos= new ArrayList<DimensionAggregatorInfo>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
           
           executorModel.setConstraints(new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(1));
           executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
           executorModel.setActualDimsRows(executorModel.getDims());
           executorModel.setActualDimsCols(new Dimension[0]);
           executorModel.setCalcMeasures(new ArrayList<Measure>(1));
           executorModel.setAnalyzerDims(executorModel.getDims());
           executorModel.setConstraintsAfterTopN(new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(1));
           executorModel.setLimit(-1);
           executorModel.setDetailQuery(false);
           executorModel.setQueryId(System.nanoTime() + "");
           executorModel.setOutLocation(MolapProperties.getInstance()
   				.getProperty(MolapCommonConstants.STORE_LOCATION_HDFS));
           return executorModel;
	}
	

}
