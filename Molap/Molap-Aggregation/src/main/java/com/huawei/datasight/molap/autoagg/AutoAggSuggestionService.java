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
