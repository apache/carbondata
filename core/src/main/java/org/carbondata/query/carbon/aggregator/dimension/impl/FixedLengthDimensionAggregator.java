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

import java.nio.ByteBuffer;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.util.DataTypeUtil;

/**
 * Class which will be used to aggregate the fixed length dimension data
 * 
 */
public class FixedLengthDimensionAggregator implements DimensionDataAggregator {

	/**
	 * info object which store information about dimension to be aggregated
	 */
	protected DimensionAggregatorInfo dimensionAggeragtorInfo;

	/**
	 * default which was added for new dimension after restructuring for the
	 * older blocks
	 */
	protected Object defaultValue;

	private int aggregatorStartIndex;

	private ByteBuffer buffer;

	private Dictionary columnDictionary;
	
	private int blockIndex;

	public FixedLengthDimensionAggregator(
			DimensionAggregatorInfo dimensionAggeragtorInfo,
			Object defaultValue, Dictionary columnDictionary,
			int aggregatorStartIndex, int blockIndex) {
		this.dimensionAggeragtorInfo = dimensionAggeragtorInfo;
		this.defaultValue = defaultValue;
		this.aggregatorStartIndex = aggregatorStartIndex;
		this.blockIndex=blockIndex;
		buffer =ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
		this.columnDictionary=columnDictionary;
	}

	/**
	 * Below method will be used to aggregate the dimension data
	 * 
	 * @param scannedResult
	 *            scanned result
	 * @param aggeragtor
	 *            aggregator used to aggregate the data
	 */
	@Override
	public void aggregateDimensionData(AbstractScannedResult scannedResult,
			MeasureAggregator[] aggeragtor) {
		byte[] dimensionData = scannedResult
				.getDimensionKey(blockIndex);
		int surrogateKey = CarbonUtil.getSurrogateKey(dimensionData,buffer);
		if (surrogateKey == 1) {
			return;
		}
		Object dataBasedOnDataType = DataTypeUtil.getDataBasedOnDataType(
				columnDictionary.getDictionaryValueForKey(surrogateKey),
				dimensionAggeragtorInfo.getDim().getDataType());
		if (null == dataBasedOnDataType) {
			return;
		}

		for (int j = 0; j < dimensionAggeragtorInfo.getAggList().size(); j++) {
			aggeragtor[aggregatorStartIndex + j].agg(dataBasedOnDataType);
		}
	}

}
