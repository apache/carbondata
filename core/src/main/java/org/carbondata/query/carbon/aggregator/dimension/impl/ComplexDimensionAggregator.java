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
package org.carbondata.query.carbon.aggregator.dimension.impl;

import java.util.List;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Class to aggregation the complex type of dimension
 * @TODO need to implement this class
 */
public class ComplexDimensionAggregator implements DimensionDataAggregator {

	/**
	 * info object which store information about dimension to be aggregated
	 */
	protected DimensionAggregatorInfo dimensionAggeragtorInfo;

	/**
	 * default which was added for new dimension after restructuring for the
	 * older blocks
	 */
	protected Object defaultValue;

	/**
	 * dictinoanryInfo;
	 */
	private List<Dictionary> columnDictionary;

	/**
	 * parent block index 
	 */
	private int parentBlockIndex;

	public ComplexDimensionAggregator(
			DimensionAggregatorInfo dimensionAggeragtorInfo,
			Object defaultValue, List<Dictionary> columnDictionary,
			int parentBlockIndex) {
		this.dimensionAggeragtorInfo = dimensionAggeragtorInfo;
		this.defaultValue = defaultValue;
		this.parentBlockIndex = parentBlockIndex;
		this.columnDictionary=columnDictionary;
	}

	/**
	 * Below method will be used to aggregate the complex dimension data
	 * 
	 * @param scannedResult
	 *            scanned result
	 * @param aggeragtor
	 *            aggregator used to aggregate the data
	 */
	@Override
	public void aggregateDimensionData(AbstractScannedResult scannedResult,
			MeasureAggregator[] aggeragtor) {

	}
}
