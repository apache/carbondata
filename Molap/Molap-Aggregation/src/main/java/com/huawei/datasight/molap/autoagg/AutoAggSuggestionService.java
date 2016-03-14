package com.huawei.datasight.molap.autoagg;

import java.util.List;

import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.datastats.model.LoadModel;

/**
 * This class is interface to spark ddl command
 * 
 * @author A00902717
 *
 */
public interface AutoAggSuggestionService
{
	
	/**
	 * This method gives list of all dimensions can be used in Aggregate table
	 * @param schema
	 * @param cube
	 * @return
	 * @throws AggSuggestException
	 */
	List<String> getAggregateDimensions(LoadModel loadModel) throws AggSuggestException;
	
	/**
	 * this method gives all possible aggregate table script
	 * @param schema
	 * @param cube
	 * @return
	 * @throws AggSuggestException 
	 */
	List<String> getAggregateScripts(LoadModel loadModel) throws AggSuggestException;

	

}
