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

package com.huawei.unibi.molap.query.result;

import java.io.Serializable;
import java.util.List;

import com.huawei.unibi.molap.query.metadata.MolapTuple;

/**
 * Molap result chunk. It will be retrived from server chunk by chunk.
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
