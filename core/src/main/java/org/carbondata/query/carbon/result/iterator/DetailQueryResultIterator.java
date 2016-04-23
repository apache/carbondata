package org.carbondata.query.carbon.result.iterator;

import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.impl.btree.BtreeDataRefNodeFinder;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.carbon.executor.InternalQueryExecutor;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.executor.impl.QueryResultPreparator;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.ChunkResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class DetailQueryResultIterator implements CarbonIterator<ChunkResult> {

	/**
	 * LOGGER.
	 */
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(DetailQueryResultIterator.class.getName());

	private QueryResultPreparator queryResultPreparator;

	private List<BlockExecutionInfo> infos;

	private InternalQueryExecutor executor;

	private long numberOfCores;

	private long[] totalNumberOfNodesPerSlice;

	private long totalNumberOfNode;

	private long currentCounter;

	private long[] numberOfExecutedNodesPerSlice;

	private int[] sliceIndexToBeExecuted;

	public DetailQueryResultIterator(List<BlockExecutionInfo> infos,
			QueryExecutorProperties executerProperties, QueryModel queryModel,
			InternalQueryExecutor queryExecutor) {
		this.queryResultPreparator = new QueryResultPreparator(
				executerProperties, queryModel);
		int recordSize = 0;
		String defaultInMemoryRecordsSize = CarbonProperties.getInstance()
				.getProperty(CarbonCommonConstants.INMEMORY_REOCRD_SIZE);
		if (null != defaultInMemoryRecordsSize) {
			try {
				recordSize = Integer.parseInt(defaultInMemoryRecordsSize);
			} catch (NumberFormatException ne) {
				LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
						"Invalid inmemory records size. Using default value");
				recordSize = CarbonCommonConstants.INMEMORY_REOCRD_SIZE_DEFAULT;
			}
		}
		this.numberOfCores = recordSize
				/ Integer.parseInt(CarbonProperties.getInstance().getProperty(
						CarbonCommonConstants.LEAFNODE_SIZE,
						CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));
		if (numberOfCores == 0) {
			numberOfCores++;
		}
		executor = queryExecutor;
		this.infos = infos;
		this.sliceIndexToBeExecuted = new int[(int) numberOfCores];
		intialiseInfos();
	}

	private void intialiseInfos() {
		this.totalNumberOfNodesPerSlice = new long[infos.size()];
		this.numberOfExecutedNodesPerSlice = new long[infos.size()];
		int index = -1;
		for (BlockExecutionInfo blockInfo : infos) {
			++index;
			DataRefNodeFinder finder = new BtreeDataRefNodeFinder(
					blockInfo.getEachColumnValueSize());
			DataRefNode startDataBlock = finder.findFirstDataBlock(
					blockInfo.getFirstDataBlock(), blockInfo.getStartKey());
			DataRefNode endDataBlock = finder.findLastDataBlock(
					blockInfo.getFirstDataBlock(), blockInfo.getEndKey());

			this.totalNumberOfNodesPerSlice[index] = startDataBlock
					.nodeNumber() - endDataBlock.nodeNumber() + 1;
			totalNumberOfNode += this.totalNumberOfNodesPerSlice[index];
			blockInfo.setFirstDataBlock(startDataBlock);
			blockInfo.setNumberOfBlockToScan(1);
		}

	}

	@Override
	public boolean hasNext() {
		return currentCounter < totalNumberOfNode;
	}

	@Override
	public ChunkResult next() {
		updateSliceIndexToBeExecuted();
		CarbonIterator<Result> result = null;
		try {
			result = executor.executeQuery(infos, sliceIndexToBeExecuted);
		} catch (QueryExecutionException e) {
			LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
					e.getMessage());
		}
		for (int i = 0; i < sliceIndexToBeExecuted.length; i++) {
			if (sliceIndexToBeExecuted[i] != -1) {
				infos.get(sliceIndexToBeExecuted[i]).setFirstDataBlock(
						infos.get(sliceIndexToBeExecuted[i])
								.getFirstDataBlock().getNextDataRefNode());
			}
		}
		if (null != result) {
			Result next = result.next();
			if (next.size() > 0) {
				return queryResultPreparator.getQueryResult(next);
			} else {
				return new ChunkResult();
			}
		} else {
			return new ChunkResult();
		}
	}

	private void updateSliceIndexToBeExecuted() {
		Arrays.fill(sliceIndexToBeExecuted, -1);
		int currentSliceIndex = 0;
		int i = 0;
		for (; i < (int) numberOfCores;) {
			if (this.totalNumberOfNodesPerSlice[currentSliceIndex] > this.numberOfExecutedNodesPerSlice[currentSliceIndex]) {
				this.numberOfExecutedNodesPerSlice[currentSliceIndex]++;
				sliceIndexToBeExecuted[i] = currentSliceIndex;
				i++;
			}
			currentSliceIndex++;
			if (currentSliceIndex >= totalNumberOfNodesPerSlice.length) {
				break;
			}
		}
		currentCounter += i;
	}

}
