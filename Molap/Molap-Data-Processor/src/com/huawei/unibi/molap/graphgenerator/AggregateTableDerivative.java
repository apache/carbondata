package com.huawei.unibi.molap.graphgenerator;

import java.util.List;

import com.huawei.unibi.molap.schema.metadata.AggregateTable;

public interface  AggregateTableDerivative {

	void add(AggregateTableDerivative aggregateTableDerivative);
	AggregateTableDerivative get(int i);
	void remove(AggregateTableDerivative aggregateTableDerivative);
	 int length();
	 List<AggregateTableDerivative> getChildrens();
	 AggregateTable getAggregateTable();
}
