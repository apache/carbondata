/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.query.result.impl;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.query.metadata.MolapTuple;
import com.huawei.unibi.molap.query.result.MolapResultChunk;

/**
 * Implementation class for MolapResultChunk interface.
 * @author R00900208
 *
 */
public class MolapResultChunkImpl implements MolapResultChunk 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8170271559294139918L;
	
	/**
	 * rowTuples
	 */
	private List<MolapTuple> rowTuples = new ArrayList<MolapTuple>(10);
	
	/**
	 * data
	 */
	private Object[][] data; 

	/**
	 * See interface comments
	 */
	@Override
	public List<MolapTuple> getRowTuples() 
	{
		return rowTuples;
	}
	
	/**
	 * See interface comments
	 */
	@Override
	public Object getCell(int columnIndex, int rowIndex) 
	{
	
		return data[rowIndex][columnIndex];
	}

	/**
	 * Set the row tuples to chunk
	 * @param rowTuples
	 */
	public void setRowTuples(List<MolapTuple> rowTuples)
	{
		this.rowTuples = rowTuples;
	}

	/**
	 * Set the cell data to chunk.
	 * @param data
	 */
	public void setData(Object[][] data)
	{
		this.data = data;
	}
}
