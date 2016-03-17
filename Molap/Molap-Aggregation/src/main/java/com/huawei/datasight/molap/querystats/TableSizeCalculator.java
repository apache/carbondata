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
