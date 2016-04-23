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
package org.carbondata.query.carbon.executor.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.carbon.executor.InternalQueryExecutor;
import org.carbondata.query.carbon.executor.InternalQueryExecutorFactory;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.AggregatorInfo;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.infos.SortInfo;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.executor.util.RestructureUtil;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.RowResult;
import org.carbondata.query.carbon.result.impl.ListBasedResult;
import org.carbondata.query.carbon.result.iterator.ChunkBasedResultIterator;
import org.carbondata.query.carbon.result.iterator.ChunkRowIterator;
import org.carbondata.query.carbon.result.iterator.DetailQueryResultIterator;
import org.carbondata.query.carbon.result.iterator.MemoryBasedResultIterator;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Below class will be used to execute the query
 *
 */
public class QueryExecutorImpl extends AbstractQueryExecutor {

	private static final LogService LOGGER = LogServiceFactory
			.getLogService(QueryExecutorImpl.class.getName());

	/**
	 * Method which will execute the query based on query model and return the
	 * query result iterator
	 * 
	 * @param query
	 *            model
	 */
	@Override
	public CarbonIterator<RowResult> execute(QueryModel queryModel)
			throws QueryExecutionException {

		StandardLogService.setThreadName(
				StandardLogService.getPartitionID(queryModel
						.getAbsoluteTableIdentifier()
						.getCarbonTableIdentifier().getTableName()),
				queryModel.getQueryId());
		// to initialize the query properties
		initQuery(queryModel);

		if(queryProperties.dataBlocks.size()==0)
		{
			 // if there are not block present then set empty row
			// and return 
            return new ChunkRowIterator(
                    new ChunkBasedResultIterator(new MemoryBasedResultIterator(new ListBasedResult()),
                    		queryProperties, queryModel));
		}
		InternalQueryExecutor queryExecutor = InternalQueryExecutorFactory
				.getQueryExecutor(queryModel, queryProperties.dataBlocks);
		if (queryExecutor instanceof CountStartExecutor) {
			queryExecutor.executeQuery(null, null);

		} else if (queryExecutor instanceof FunctionQueryExecutor) {
			queryProperties.isFunctionQuery = true;
			queryExecutor.executeQuery(null, null);
		}
		// logger query execution information
		// for example table name and if it is a detail query
		LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
				"Query will be executed on table: " + queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName());

		LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
				"Is detail Query: " + queryModel.isDetailQuery());
		List<BlockExecutionInfo> blockExecutionInfoList = new ArrayList<BlockExecutionInfo>();
		// fill all the block execution infos for all the blocks selected in
		// query
		// and query will be executed based on that infos
		for (int i = 0; i < queryProperties.dataBlocks.size(); i++) {
			blockExecutionInfoList.add(getBlockExecutionInfoForBlock(
					queryModel, queryProperties.dataBlocks.get(i), false));
		}
		blockExecutionInfoList.add(getBlockExecutionInfoForBlock(queryModel,
				queryProperties.dataBlocks.get(queryProperties.dataBlocks
						.size() - 1), true));
		CarbonIterator<Result> queryResultIterator = null;
		if (blockExecutionInfoList.size() > 0) {
			if (!queryModel.isDetailQuery()
					|| (queryModel.isDetailQuery()
							&& null != queryModel.getSortOrder() && queryModel
							.getSortOrder().length > 0)) {
				queryResultIterator = queryExecutor.executeQuery(
						blockExecutionInfoList, null);
			} else {
				LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
						"Memory based detail query: ");
				return new ChunkRowIterator(new DetailQueryResultIterator(
						blockExecutionInfoList, queryProperties, queryModel,
						queryExecutor));
			}
		} else {
			return new ChunkRowIterator(new ChunkBasedResultIterator(
					new MemoryBasedResultIterator(new ListBasedResult()),
					queryProperties, queryModel));
		}
		ChunkBasedResultIterator chunkBasedResultIterator = new ChunkBasedResultIterator(
				queryResultIterator, queryProperties, queryModel);
		return new ChunkRowIterator(chunkBasedResultIterator);
	}

	private BlockExecutionInfo getBlockExecutionInfoForBlock(
			QueryModel queryModel, AbstractIndex blockIndex, boolean isLastBlock)
			throws QueryExecutionException {
		BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
		SegmentProperties segmentProperties = blockIndex.getSegmentProperties();
		List<CarbonDimension> tableBlockDimensions = segmentProperties
				.getDimensions();
		KeyGenerator blockKeyGenerator = segmentProperties
				.getDimensionKeyGenerator();

		// below is to get only those dimension in query which is present in the
		// table block
		List<CarbonDimension> updatedQueryDimension = RestructureUtil
				.getUpdatedQueryDimension(queryModel.getQueryDimension(),
						tableBlockDimensions);
		// add complex dimension children
		updatedQueryDimension = RestructureUtil
				.addChildrenForComplexTypeDimension(updatedQueryDimension,
						tableBlockDimensions);
		int[] maskByteRangesForBlock = QueryUtil.getMaskedByteRange(
				updatedQueryDimension, blockKeyGenerator);
		int[] maksedByte = QueryUtil.getMaksedByte(
				blockKeyGenerator.getKeySizeInBytes(), maskByteRangesForBlock);
		blockExecutionInfo.setDataBlock(blockIndex);
		blockExecutionInfo.setDataBlockKeygenerator(blockKeyGenerator);
		// adding aggregation info for query
		blockExecutionInfo.setAggregatorInfo(getAggregatorInfoForBlock(
				queryModel, blockIndex));
		// adding custom aggregate expression of query
		blockExecutionInfo.setCustomAggregateExpressions(queryModel
				.getExpressions());

		// setting the limit
		blockExecutionInfo.setLimit(queryModel.getLimit());
		// setting whether detail query or not
		blockExecutionInfo.setDetailQuery(queryModel.isDetailQuery());
		// setting the masked byte of the block which will be
		// used to update the unpack the older block keys
		blockExecutionInfo.setMaskedByteForBlock(maksedByte);
		// total number dimension
		blockExecutionInfo.setTotalNumberDimensionBlock(segmentProperties
				.getDimensionOrdinalToBlockMapping().size());
		blockExecutionInfo.setTotalNumberOfMeasureBlock(segmentProperties
				.getMeasuresOrdinalToBlockMapping().size());
		// to check whether older block key update is required or not
		blockExecutionInfo.setFixedKeyUpdateRequired(blockKeyGenerator
				.equals(queryProperties.keyStructureInfo.getKeyGenerator()));

		// expression dimensions
		List<CarbonDimension> expressionDimensions = new ArrayList<CarbonDimension>(
				CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
		// expression measure
		List<CarbonMeasure> expressionMeasures = new ArrayList<CarbonMeasure>(
				CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
		// to get all the dimension and measure which required to get the chunk
		// indexes to be read from file
		QueryUtil.extractDimensionsAndMeasuresFromExpression(
				queryModel.getExpressions(), expressionDimensions,
				expressionMeasures);
		// setting all the dimension chunk indexes to be read from file
		blockExecutionInfo.setAllSelectedDimensionBlocksIndexes(QueryUtil
				.getDimensionsBlockIndexes(updatedQueryDimension,
						segmentProperties.getDimensionOrdinalToBlockMapping(),
						queryModel.getDimAggregationInfo(),
						expressionDimensions));
		// setting all the measure chunk indexes to be read from file
		blockExecutionInfo.setAllSelectedMeasureBlocksIndexes(QueryUtil
				.getMeasureBlockIndexes(queryModel.getQueryMeasures(),
						expressionMeasures,
						segmentProperties.getMeasuresOrdinalToBlockMapping()));
		// setting the key structure info which will be required
		// ot update the older block key with new key generator
		blockExecutionInfo
				.setKeyStructureInfo(queryProperties.keyStructureInfo);
		// settingthe size of fixed key column (dictionary column)
		blockExecutionInfo.setFixedLengthKeySize(getKeySize(
				updatedQueryDimension, segmentProperties));
		List<Integer> dictionaryColumnBlockIndex = new ArrayList<Integer>();
		List<Integer> noDictionaryColumnBlockIndex = new ArrayList<Integer>();

		// get the block index to be read from file for query dimension
		// for both dictionary columns and no dictionary columns
		QueryUtil.fillQueryDimensionsBlockIndexes(updatedQueryDimension,
				segmentProperties.getDimensionOrdinalToBlockMapping(),
				dictionaryColumnBlockIndex, noDictionaryColumnBlockIndex);
		int[] queryDictionaruColumnBlockIndexes = ArrayUtils
				.toPrimitive(dictionaryColumnBlockIndex
						.toArray(new Integer[dictionaryColumnBlockIndex.size()]));
		// need to sort the dictionary column as for all dimension
		// column key will be filled based on key order
		Arrays.sort(queryDictionaruColumnBlockIndexes);
		blockExecutionInfo
				.setDictionaryColumnBlockIndex(queryDictionaruColumnBlockIndexes);
		// setting the no dictionary column block indexes
		blockExecutionInfo.setNoDictionaryBlockIndexes(ArrayUtils
				.toPrimitive(noDictionaryColumnBlockIndex
						.toArray(new Integer[noDictionaryColumnBlockIndex
								.size()])));
		blockExecutionInfo
				.setColumnIdToDcitionaryMapping(queryProperties.columnToDictionayMapping);
		// to set the sort info for the block which will be
		// required to sort the data, this will be set only for
		// last block as it will be done after all the blocks
		// processing in done and final merger will handle sorting
		// so all the block fixed key will be updated based on new key generator
		// so no need to set for all the block execution info
		if (isLastBlock) {
			blockExecutionInfo.setSortInfo(getSortInfos(
					queryModel.getQueryDimension(), queryModel));
		}

		blockExecutionInfo.setEachColumnValueSize(segmentProperties
				.getEachDimColumnValueSize());
		try {
			// to set column group and its key structure info which will be used
			// to
			// for getting the column group column data in case of final row
			// and in case of dimension aggregation
			blockExecutionInfo.setColumnGroupToKeyStructureInfo(QueryUtil
					.getColumnGroupKeyStructureInfo(updatedQueryDimension,
							segmentProperties));
		} catch (KeyGenException e) {
			throw new QueryExecutionException(e);
		}
		return blockExecutionInfo;
	}

	/**
	 * This method will be used to get fixed key length size this will be used
	 * to create a row from column chunk
	 * 
	 * @param queryDimension
	 *            query dimension
	 * @param blockMetadataInfo
	 *            block metadata info
	 * @return key size
	 */
	private int getKeySize(List<CarbonDimension> queryDimension,
			SegmentProperties blockMetadataInfo) {
		List<Integer> fixedLengthDimensionOrdinal = new ArrayList<Integer>(
				CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
		int counter = 0;
		while (counter < queryDimension.size()) {
			if (queryDimension.get(counter).numberOfChild() > 0) {
				counter += queryDimension.get(counter).numberOfChild();
				continue;
			} else if (!CarbonUtil.hasEncoding(queryDimension.get(counter)
					.getEncoder(), Encoding.DICTIONARY)) {
				counter++;
			} else {
				fixedLengthDimensionOrdinal.add(queryDimension.get(counter)
						.getKeyOrdinal());
				counter++;
			}
		}
		int[] dictioanryColumnOrdinal = ArrayUtils
				.toPrimitive(fixedLengthDimensionOrdinal
						.toArray(new Integer[fixedLengthDimensionOrdinal.size()]));
		if (dictioanryColumnOrdinal.length > 0) {
			return blockMetadataInfo.getFixedLengthKeySplitter()
					.getKeySizeByBlock(dictioanryColumnOrdinal);
		}
		return 0;
	}

	/**
	 * Below method will be used to get the sort information which will be
	 * required during sorting
	 * 
	 * @param orderByDimension
	 *            order by dimension
	 * @param queryModel
	 *            query model
	 * @return Sort infos
	 * @throws QueryExecutionException
	 *             if problem while
	 */
	private SortInfo getSortInfos(List<CarbonDimension> orderByDimension,
			QueryModel queryModel) throws QueryExecutionException {

		// get the masked by range for order by dimension
		int[][] maskedByteRangeForSorting = QueryUtil
				.getMaskedByteRangeForSorting(orderByDimension,
						queryProperties.keyStructureInfo.getKeyGenerator(),
						queryProperties.keyStructureInfo.getMaskByteRanges());
		// get masked key for sorting
		byte[][] maksedKeyForSorting = QueryUtil.getMaksedKeyForSorting(
				orderByDimension,
				queryProperties.keyStructureInfo.getKeyGenerator(),
				maskedByteRangeForSorting,
				queryProperties.keyStructureInfo.getMaskByteRanges());
		// fill sort dimension indexes
		queryProperties.sortDimIndexes = QueryUtil.getSortDimensionIndexes(
				orderByDimension, queryModel.getQueryDimension());
		SortInfo sortInfos = new SortInfo();
		sortInfos.setDimensionMaskKeyForSorting(maksedKeyForSorting);
		sortInfos.setDimensionSortOrder(queryModel.getSortOrder());
		sortInfos.setMaskedByteRangeForSorting(maskedByteRangeForSorting);
		sortInfos.setSortDimensionIndex(queryProperties.sortDimIndexes);
		return sortInfos;
	}

	/**
	 * Below method will be used to get the aggrgator info for the query
	 * 
	 * @param queryModel
	 *            query model
	 * @param tableBlock
	 *            table block
	 * @return aggregator info
	 */
	private AggregatorInfo getAggregatorInfoForBlock(QueryModel queryModel,
			AbstractIndex tableBlock) {
		// getting the aggregate infos which will be used during aggregation
		AggregatorInfo aggregatorInfos = RestructureUtil.getAggregatorInfos(
				queryModel.getQueryMeasures(), tableBlock
						.getSegmentProperties().getMeasures());
		// setting the index of expression in measure aggregators
		aggregatorInfos
				.setExpressionAggregatorStartIndex(queryProperties.aggExpressionStartIndex);
		// setting the index of measure columns in measure aggregators
		aggregatorInfos
				.setMeasureAggregatorStartIndex(queryProperties.measureStartIndex);
		// setting the measure aggregator for all aggregation function selected
		// in query
		aggregatorInfos
				.setMeasuresAggreagators(queryProperties.measureAggregators);
		return aggregatorInfos;
	}
}
