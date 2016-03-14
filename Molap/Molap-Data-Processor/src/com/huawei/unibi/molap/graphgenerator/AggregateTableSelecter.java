package com.huawei.unibi.molap.graphgenerator;

import java.util.ArrayList;
import java.util.List;

//import mondrian.olap.MondrianDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.schema.metadata.AggregateTable;

public abstract class AggregateTableSelecter {

	protected List<AggregateTable> aggregateTables;
//	protected boolean isAutoAggRequest;
	protected List<AggregateTable> processedAggTables = new ArrayList<AggregateTable>(
			15);
	protected String factName;
	//AggregateTableDerivative aggregateTableDerivative;
	//AggregateTableDerivative aggregateTableDerivativeForGraphGeneration;
	AggregateTableDerivativeComposite root;

	AggregateTableSelecter(List<AggregateTable> aggregateTables) {
		this.aggregateTables = aggregateTables;

	}

//	/**
//	 * Selecting the aggregate table whose having maximum levels
//	 * 
//	 * @param mondrianCube
//	 * @return
//	 */
//	public void selectAggTableNeedsToGenFromFact(Cube mondrianCube) {
//
//		factName = mondrianCube.fact.getAlias();
//		AggregateTable maxLevelAggTable = null;
//		int length = 0;
//		for (AggregateTable aggtable : aggregateTables) {
//			String[] aggLevels = aggtable.getAggLevels();
//			if (aggLevels.length > length) {
//				length = aggLevels.length;
//				maxLevelAggTable = aggtable;
//			}
//		}
//		processedAggTables.add(maxLevelAggTable);
//		maxLevelAggTable.setTableNameForAggregate(factName);
//		aggregateTableDerivative=new AggregateTableDerivativeComposite(factName,maxLevelAggTable);
//
//	}

	//public abstract void selectTableForAggTableAggregationProcess(AggregateTable[] aggregateTable);
	public abstract void selectTableForAggTableAggregationProcess(AggregateTable[] aggregateTable, Cube cube);
	
	public AggregateTableDerivative getAggregateTableDerivativeInstanceForAggEval() {

		return root;
	}
	

}
