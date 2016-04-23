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
package org.carbondata.query.carbon.aggregator.impl;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.DataAggregator;
import org.carbondata.query.carbon.aggregator.ScannedResultAggregator;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.impl.ListBasedResult;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * It is not a aggregator it is just a scanned result holder.
 * 
 * @TODO change it to some other name
 *
 */
public class ListBasedResultAggregator implements ScannedResultAggregator {

	private static final LogService LOGGER = LogServiceFactory
			.getLogService(ListBasedResultAggregator.class.getName());
	/**
	 * data aggregator
	 */
	private DataAggregator dataAggregator;

	/**
	 * to keep a track of number of row processed to handle limit push down in
	 * case of detail query scenario
	 */
	private int rowCounter;

	/**
	 * number of records asked in limit query if -1 then its either is
	 * detail+order by query or detail query
	 */
	private int limit;

	/**
	 * dimension values list
	 */
	private List<ListBasedResultWrapper> listBasedResult;

	/**
	 * aggreagtor used
	 * 
	 * @TODO no need to create this
	 * @TODO need to handle in some other way
	 */
	private MeasureAggregator[] blockAggregator;

	/**
	 * restructuring info
	 */
	private KeyStructureInfo restructureInfos;

	/**
	 * table block execution infos
	 */
	private BlockExecutionInfo tableBlockExecutionInfos;

	public ListBasedResultAggregator(
			BlockExecutionInfo tableBlockExecutionInfos,
			DataAggregator aggregator) {
		limit = tableBlockExecutionInfos.getLimit();
		this.tableBlockExecutionInfos = tableBlockExecutionInfos;
		blockAggregator = tableBlockExecutionInfos.getAggregatorInfo()
				.getMeasuresAggreagators();
		restructureInfos = tableBlockExecutionInfos.getKeyStructureInfo();
	}

	@Override
	/**
	 * This method will add a record both key and value to list object 
	 * it will keep track of how many record is processed, to handle limit scneario 
	 * @param scannedResult
	 * 			scanned result 
	 */
	public int aggregateData(AbstractScannedResult scannedResult) {
		this.listBasedResult = new ArrayList<ListBasedResultWrapper>(limit>-1?scannedResult.numberOfOutputRows():limit);
		ByteArrayWrapper wrapper = null;
		MeasureAggregator[] measureAggregator = null;
		// scan the record and add to list
		ListBasedResultWrapper resultWrapper;
		while (scannedResult.hasNext() && (limit == -1 || rowCounter < limit)) {
			wrapper = new ByteArrayWrapper();
			wrapper.setComplexTypesKeys(scannedResult.getComplexTypeKeyArray());
			wrapper.setNoDictionaryKeys(scannedResult.getNoDictionaryKeyArray());
			wrapper.setDictionaryKey(scannedResult.getDictionaryKeyArray());
			resultWrapper = new ListBasedResultWrapper();
			resultWrapper.setKey(wrapper);
			resultWrapper.setValue(getNewAggregator());
			rowCounter++;
		}
		// call data aggregator to convert measure value to some aggreagtor
		// object
		dataAggregator.aggregateData(scannedResult, measureAggregator);
		return rowCounter;
	}

	/**
	 * Below method will be used to get the new aggreagtor
	 * 
	 * @return new aggregator object
	 */
	private MeasureAggregator[] getNewAggregator() {
		MeasureAggregator[] aggregators = new MeasureAggregator[blockAggregator.length];
		for (int i = 0; i < blockAggregator.length; i++) {
			aggregators[i] = blockAggregator[i].getNew();
		}
		return aggregators;
	}

	/**
	 * Below method will used to get the result 
	 * @param aggregated result
	 */
	@Override
	public Result<List<ListBasedResultWrapper>> getAggregatedResult() {
		Result<List<ListBasedResultWrapper>> result = new ListBasedResult();
		if(!tableBlockExecutionInfos.isFixedKeyUpdateRequired())
		{
			updateKeyWithLatestBlockKeyganerator();
			result.addScannedResult(listBasedResult);
		}
		else
		{
			result.addScannedResult(listBasedResult);
		}
		return result;
	}

	/**
	 * Below method will be used to update the fixed length key with the 
	 * latest block key generator
	 * @return updated block
	 */
	private void updateKeyWithLatestBlockKeyganerator() {
		try {
			long[] data = null;
			ByteArrayWrapper key = null;
			for (int i = 0; i < listBasedResult.size(); i++) {
				// get the key
				key = listBasedResult.get(i).getKey();
				// unpack the key with table block key generator
				data = tableBlockExecutionInfos.getDataBlockKeyGenerator()
						.getKeyArray(
								key.getDictionaryKey(),
								tableBlockExecutionInfos
										.getMaskedByteForBlock());
				// packed the key with latest block key generator
				// and generate the masked key for that key
				key.setDictionaryKey(QueryUtil.getMaskedKey(restructureInfos
						.getKeyGenerator().generateKey(data), restructureInfos
						.getMaxKey(), restructureInfos.getMaskByteRanges(),
						restructureInfos.getMaskByteRanges().length));
				listBasedResult.get(i).setKey(key);
			}
		} catch (KeyGenException e) {
			LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
		}
	}

}
