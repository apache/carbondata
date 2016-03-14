package com.huawei.datasight.common.util;

import java.util.ArrayList;
import java.util.List;

import com.huawei.datasight.molap.query.MolapQueryPlan;
import com.huawei.datasight.molap.query.metadata.MolapDimension;
import com.huawei.datasight.molap.query.metadata.MolapMeasure;
import com.huawei.datasight.molap.query.metadata.MolapMeasure.AggregatorType;
import com.huawei.datasight.molap.query.metadata.SortOrderType;
import com.huawei.unibi.molap.engine.expression.ColumnExpression;
import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.LiteralExpression;
import com.huawei.unibi.molap.engine.expression.conditional.InExpression;
import com.huawei.unibi.molap.engine.expression.conditional.ListExpression;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class QueryPlanUtils {
	
	private static String schemaName;
	private static String cubeName;
	
	public static void setSchemaCube(String schema, String cube)
	{
		schemaName = schema;
		cubeName = cube;
	}
	public static MolapQueryPlan createQueryPlan()
	{
		MolapQueryPlan plan = new MolapQueryPlan(schemaName, cubeName);
		//Set Query ID
        plan.setQueryId(System.nanoTime()+"");
        return plan;       
	}

	public static void addDimension(MolapQueryPlan plan, String dimensionName, int queryOrder) {
		//Add Dimension
		MolapDimension dim = new MolapDimension(dimensionName);
		dim.setQueryOrder(queryOrder);
		plan.addDimension(dim);
	}
	public static void addDimensionWithSort(MolapQueryPlan plan, String dimensionName, SortOrderType sortOrderType, int queryOrder) {
		//Add Dimension with Asc Sort
		MolapDimension dim = new MolapDimension(dimensionName);
		dim.setQueryOrder(queryOrder);
		dim.setSortOrderType(sortOrderType);
		plan.addDimension(dim);
	}
	public static void addMeasure(MolapQueryPlan plan, String measureName, int queryOrder) {
		//Add Measure
		MolapMeasure msr = new MolapMeasure(measureName);
		msr.setQueryOrder(queryOrder);
		plan.addMeasure(msr);
	}
	public static void addMeasureWithAgg(MolapQueryPlan plan, String measureName, AggregatorType aggregatorType, int queryOrder) {
		//Add Count Measure
		MolapMeasure msr1 = new MolapMeasure(measureName);
		msr1.setQueryOrder(queryOrder);
		msr1.setAggregatorType(aggregatorType);
		plan.addMeasure(msr1);
	}
	public static void addAggOnDimension(MolapQueryPlan plan, String measureName, String aggregatorType, int queryOrder) {
		//Add Dimension Aggregate
		plan.addAggDimAggInfo(measureName, aggregatorType, queryOrder);
	}
	public static void setQueryLimit(MolapQueryPlan plan, int limit) {
		//Set Query Limit
		plan.setLimit(limit);
	}
	public static void isDetailedQuery(MolapQueryPlan plan, boolean isDetailQuery) {
		//Set Detailed Query
		plan.setDetailQuery(isDetailQuery);
	}
	public static void addInFilter(MolapQueryPlan plan, String columnName, List<Object> values, Dimension currDim, DataType dataType) {
		//Filter
		//Left Column
		Expression left = new ColumnExpression(columnName,dataType);
		((ColumnExpression)left).setDim(currDim);
		((ColumnExpression)left).setDimension(true);
		
		List<Expression> exprs = new ArrayList<Expression>(values.size());
		for(Object obj : values)
		{
			// Right values
			Expression right = new LiteralExpression(obj,dataType);
			exprs.add(right);
		}
		Expression listExpr = new ListExpression(exprs);
		//Creating In Filter
		InExpression inExpr = new InExpression(left, listExpr);
		// EqualToExpression equalExpr = new EqualToExpression(left, right);
		plan.setFilterExpression(inExpr);
	}
	
}
