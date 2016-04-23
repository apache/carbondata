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

package org.carbondata.query.carbon.model;

import java.io.Serializable;

import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.query.evaluators.FilterEvaluator;

/**
 * Table filter model which will be used to apply filter 
 */
public class TableBlockFilterModel implements Serializable{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * filter evaluator tree
	 */
	private FilterEvaluator filterEvaluatorTree;
	
	/**
	 * start key for the filter 
	 */
	private IndexKey startKey;
	
	/**
	 * end key for the filter
	 */
	private IndexKey endKey;

	/**
	 * @return the filterEvaluatorTree
	 */
	public FilterEvaluator getFilterEvaluatorTree() {
		return filterEvaluatorTree;
	}

	/**
	 * @param filterEvaluatorTree the filterEvaluatorTree to set
	 */
	public void setFilterEvaluatorTree(FilterEvaluator filterEvaluatorTree) {
		this.filterEvaluatorTree = filterEvaluatorTree;
	}

	/**
	 * @return the startKey
	 */
	public IndexKey getStartKey() {
		return startKey;
	}

	/**
	 * @param startKey the startKey to set
	 */
	public void setStartKey(IndexKey startKey) {
		this.startKey = startKey;
	}

	/**
	 * @return the endKey
	 */
	public IndexKey getEndKey() {
		return endKey;
	}

	/**
	 * @param endKey the endKey to set
	 */
	public void setEndKey(IndexKey endKey) {
		this.endKey = endKey;
	}
}
