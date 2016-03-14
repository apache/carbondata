package com.huawei.datasight.molap.autoagg.model;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;

import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.util.AggCombinationGeneratorUtil;

/**
 * Details of single aggregate combination
 * 
 * @author p70884
 *
 */
public class AggSuggestion implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private BigInteger possibleRows = BigInteger.valueOf(1);

	private Level[] aggCombinationLevels;

	public AggSuggestion(Level[] levelDetails, Request requestType)
	{
		this.aggCombinationLevels = levelDetails;
		if (requestType == Request.DATA_STATS)
		{
			initCalculatedInfo();
		}

	}

	private void initCalculatedInfo()
	{

		this.possibleRows = AggCombinationGeneratorUtil
				.getMaxPossibleRows(aggCombinationLevels);

	}

	/************** GETTers and SETTers ***************************************/

	public Level[] getAggLevels()
	{
		return aggCombinationLevels;
	}

	public BigInteger getPossibleRows()
	{
		return possibleRows;
	}

	public String toString()
	{
		return Arrays.toString(aggCombinationLevels) + ':' + getPossibleRows();
	}

}
