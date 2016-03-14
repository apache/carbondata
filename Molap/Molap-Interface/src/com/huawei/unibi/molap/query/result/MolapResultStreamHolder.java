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
import java.util.UUID;

/**
 * It is the result holder which is used internally to fetch data fro server.
 * @author R00900208
 *
 */
public class MolapResultStreamHolder implements Serializable
{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5499387522959024023L;

	/**
	 * resultStream
	 */
	private MolapResultStream resultStream;
	
	/**
	 * Unique ID
	 */
	private UUID uuid;

	/**
	 * @return the resultStream
	 */
	public MolapResultStream getResultStream() 
	{
		return resultStream;
	}

	/**
	 * @param resultStream the resultStream to set
	 */
	public void setResultStream(MolapResultStream resultStream) 
	{
		this.resultStream = resultStream;
	}

	/**
	 * @return the uuid
	 */
	public UUID getUuid() 
	{
		return uuid;
	}

	/**
	 * @param uuid the uuid to set
	 */
	public void setUuid(UUID uuid) 
	{
		this.uuid = uuid;
	}
	
	

}
