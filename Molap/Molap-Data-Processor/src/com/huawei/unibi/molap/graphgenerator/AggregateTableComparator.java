package com.huawei.unibi.molap.graphgenerator;

import java.util.Comparator;

import com.huawei.unibi.molap.schema.metadata.AggregateTable;


public class AggregateTableComparator implements Comparator<AggregateTable> {

	@Override
	public int compare(AggregateTable o1, AggregateTable o2) {
		if(o1.getAggLevels().length>o2.getAggLevels().length)
		{
			return -1;
		}
		else if (o1.getAggLevels().length < o2.getAggLevels().length) {
	        return 1;
	    }
	    return 0;
		
	}


	

}
