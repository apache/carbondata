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
package org.carbondata.query.carbon.processor.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.comparators.ComparatorChain;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.executor.infos.SortInfo;
import org.carbondata.query.carbon.processor.AbstractScannedResultProcessor;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.comparator.FixedLengthKeyResultComparator;
import org.carbondata.query.carbon.result.comparator.VariableLengthKeyResultComparator;
import org.carbondata.query.carbon.result.iterator.MemoryBasedResultIterator;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Below class will be used to sort and merge the scanned result
 *
 */
public class SortedScannedResultMerger extends AbstractScannedResultProcessor {

	/**
	 * LOGGER
	 */
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(SortedScannedResultMerger.class.getName());

	public SortedScannedResultMerger(BlockExecutionInfo blockExecutionInfo,
			int maxNumberOfScannedresultList) {
		super(blockExecutionInfo, maxNumberOfScannedresultList);
	}

	/**
	 * Below method will be used to get the final query 
	 * return 
	 * @return iterator over result 
	 * 
	 */
	@Override
	public CarbonIterator<Result> getQueryResultIterator()
			throws QueryExecutionException {
		execService.shutdown();
		try {
			execService.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e1) {
			LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
					"Problem in thread termination" + e1.getMessage());
		}
		if (scannedResultList.size() > 0) {
			mergeScannedResults(scannedResultList);
			scannedResultList = null;
		}
		LOGGER.debug(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
				"Finished result merging from all slices");
		sortResult();
		return new MemoryBasedResultIterator(mergedScannedResult);
	}

	/**
	 * Below method will be used to sort the query result
	 * for dictionary dimension it will unpack the key array a
	 * and then it will get the sort index based on which new dictionary 
	 * key will be created 
	 * @throws QueryExecutionException
	 */
	private void sortResult() throws QueryExecutionException {
		List<ListBasedResultWrapper> result = new ArrayList<ListBasedResultWrapper>(
				mergedScannedResult.size());
		ListBasedResultWrapper wrapper = null;
		SortInfo sortInfo = blockExecutionInfo.getSortInfo();
		KeyStructureInfo keyStructureInfo = blockExecutionInfo
				.getKeyStructureInfo();
		long[] keyArray = null;
		try {
			while (mergedScannedResult.hasNext()) {
				wrapper = new ListBasedResultWrapper();
				keyArray = keyStructureInfo.getKeyGenerator().getKeyArray(
						mergedScannedResult.getKey().getDictionaryKey(),
						keyStructureInfo.getMaskedBytes());
				for (int i = 0; i < sortInfo.getSortDimension().size(); i++) {
					if (CarbonUtil.hasEncoding(
							sortInfo.getSortDimension().get(i).getEncoder(),
							Encoding.DICTIONARY)) {
						keyArray[sortInfo.getSortDimension().get(i)
								.getKeyOrdinal()] = blockExecutionInfo
								.getColumnIdToDcitionaryMapping()
								.get(sortInfo.getSortDimension().get(i)
										.getColumnId())
								.getSortedIndex(
										(int) keyArray[sortInfo
												.getSortDimension().get(i)
												.getKeyOrdinal()]);
					}
				}
				mergedScannedResult.getKey().setDictionaryKey(
						keyStructureInfo.getKeyGenerator()
								.generateKey(keyArray));
				wrapper.setKey(mergedScannedResult.getKey());
				wrapper.setValue(mergedScannedResult.getValue());
			}
		} catch (KeyGenException e) {
			throw new QueryExecutionException(e);
		}
		initialiseResult();
		Collections.sort(result, getMergerChainComparator(sortInfo));
		mergedScannedResult.addScannedResult(result);
	}


	/**
	 * Below method will be used to get the comparator for sorting the 
	 * result
	 * @param sortInfo
	 * 			sort info 
	 * @return comparator
	 */
	public static ComparatorChain getMergerChainComparator(SortInfo sortInfo) {
		List<Comparator> compratorList = new ArrayList<Comparator>(
				CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
		int length = sortInfo.getSortDimension().size();
		int noDictionaryIndex = 0;
		for (int i = 0; i < length; i++) {
			if (!CarbonUtil.hasEncoding(sortInfo.getSortDimension().get(i)
					.getEncoder(), Encoding.DICTIONARY)) {
				compratorList.add(new VariableLengthKeyResultComparator(
						sortInfo.getDimensionSortOrder()[i],
						noDictionaryIndex++, sortInfo.getSortDimension().get(i)
								.getDataType()));
			} else {
				compratorList.add(new FixedLengthKeyResultComparator(sortInfo
						.getMaskedByteRangeForSorting()[i], sortInfo
						.getDimensionSortOrder()[i], sortInfo
						.getDimensionMaskKeyForSorting()[i]));
			}
		}
		return new ComparatorChain(compratorList);
	}
}
