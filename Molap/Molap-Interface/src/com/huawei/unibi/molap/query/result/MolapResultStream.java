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
 * MolapResultStream class
 * @author R00900208
 *
 */
public interface MolapResultStream extends Serializable
{
	
	/**
	 * It returns the column tuples
	 *  
	 * @return List<String>
	 */
	List<MolapTuple> getColumnTuples();
	
	/**
	 *  It tells whether data is remained to fetch or not.
	 * @return
	 */
	boolean hasNext();
	
	/**
	 * It returns the result chunk
	 * @return
	 */
	MolapResultChunk getResult();
}
