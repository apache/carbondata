package com.huawei.datasight.molap.querystats;

import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.util.AggCombinationGeneratorUtil;

/**
 * This class will calculate size of table with given dimensions
 * @author A00902717
 *
 */
public class TableSizeCalculator
{
	/**
	 * distinct relationship between dimensions
	 */
	private Level[] distinctData;

	public TableSizeCalculator(Level[] distinctData)
	{
		this.distinctData=distinctData;
	}

	/**
	 * Calculate size of table having given ordinals
	 * @param dimOrdinals
	 * @return
	 */
	public long getApproximateRowSize(int[] dimOrdinals)
	{
		Level[] levels=new Level[dimOrdinals.length];
		for(int i=0;i<dimOrdinals.length;i++)
		{
			for(int j=0;j<distinctData.length;j++)
			{
				if(distinctData[j].getOrdinal()==dimOrdinals[i])
				{
					levels[i]=distinctData[j];
					break;
				}
			}
			if(levels[i]==null)
			{
				return 0;
			}
		}
		long noOfRowsScanned=AggCombinationGeneratorUtil.getMaxPossibleRows(levels).longValue();
		return noOfRowsScanned;
			
	}

	
}
