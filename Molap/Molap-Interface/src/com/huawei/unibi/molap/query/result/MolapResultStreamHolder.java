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
