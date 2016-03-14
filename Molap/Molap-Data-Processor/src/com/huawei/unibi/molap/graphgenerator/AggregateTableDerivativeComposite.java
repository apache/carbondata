package com.huawei.unibi.molap.graphgenerator;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.schema.metadata.AggregateTable;

public class AggregateTableDerivativeComposite implements
		AggregateTableDerivative {

	private String evaluatorTableName;
	private AggregateTable aggregateTable;


	private List<AggregateTableDerivative> childrens = new ArrayList<AggregateTableDerivative>(
			20);

	public AggregateTableDerivativeComposite()
	{
		
	}
	public AggregateTableDerivativeComposite(String name,AggregateTable aggregateTable) {
		this.evaluatorTableName = name;
		this.aggregateTable = aggregateTable;
	}

	@Override
	public void add(AggregateTableDerivative aggregateTableDerivative) {
		childrens.add(aggregateTableDerivative);

	}

	@Override
	public AggregateTableDerivative get(int i) {
		// TODO Auto-generated method stub
		return childrens.get(i);
	}
	public List<AggregateTableDerivative> getChildrens() {
		return childrens;
	}
	
	public int length() {
		// TODO Auto-generated method stub
		return childrens.size();
	}

	@Override
	public void remove(AggregateTableDerivative aggregateTableDerivative) {
		childrens.remove(aggregateTableDerivative);

	}

	public String getEvaluatorTableName() {
		return evaluatorTableName;
	}

	public AggregateTable getAggregateTable() {
		return aggregateTable;
	}

}
