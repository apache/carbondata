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

package org.carbondata.query.carbon.result;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * Result interface for storing the result 
 */
public interface Result<K> {
	/**
	 * Below method will be used to 
	 * add the sccaed result
	 * @param result
	 */
    void addScannedResult(K result);

    /**
     * Returns {@code true} if the iteration has more elements.
     * @return {@code true} if the iteration has more elements
     */
    boolean hasNext();

    /**
     * Below method will return the result key 
     * @return key 
     */
    ByteArrayWrapper getKey();

    /**
     * Below code will return the result value
     * @return value
     */ 
    MeasureAggregator[] getValue();

    void merge(Result<K> otherResult);

    /**
     * Below method will be used to get the result 
     * @return
     */
    K getResult();
    
    /**
     * 
     * @return size of the result 
     */
    int size();
}
