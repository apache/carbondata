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
package com.huawei.unibi.molap.query.result;

import java.io.Serializable;
import java.util.List;

import com.huawei.unibi.molap.query.metadata.MolapTuple;

/**
 * Molap result chunk. It will be retrived from server chunk by chunk.
 * 
 * @author R00900208
 *
 */
public interface MolapResultChunk extends Serializable
{
	
	
	/**
	 * It returns the row tuples
	 * 
	 * @return List<String>
	 */
	List<MolapTuple> getRowTuples();
		 

    /**
     * Get the value for the column index
     * 
     * @param columnIndex
     *          column index
     * @return actual value
     *      
     *
     */
    Object getCell(int columnIndex,int rowIndex);

}
