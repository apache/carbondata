package com.huawei.datasight.molap.datastats.load;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.huawei.datasight.molap.datastats.analysis.AggDataSuggestScanner;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.util.MolapProperties;
/**
 * Fact file reader
 * @author A00902717
 *
 */
public class FactDataReader
{
	private List<FactDataNode> factDataNodes;

	private int keySize;

	private FileHolder fileHolder;

	public FactDataReader(List<FactDataNode> factDataNodes, int keySize,
			FileHolder fileHolder)
	{
		this.factDataNodes = factDataNodes;
		this.keySize = keySize;
		this.fileHolder = fileHolder;

	}

	public HashSet<Integer> getSampleFactData(int dimension, int noOfRows)
	{
		int[] dimensions = new int[] {dimension};
		boolean[] needCompression = new boolean[dimensions.length];
		Arrays.fill(needCompression, true);

		
		String configFactSize = MolapProperties.getInstance().getProperty(
				Preference.AGG_FACT_COUNT);
		int noOfFactsToRead = factDataNodes.size();
		if (null != configFactSize
				&& Integer.parseInt(configFactSize) < noOfFactsToRead)
		{
			noOfFactsToRead = Integer.parseInt(configFactSize);
		}
		HashSet<Integer> mergedData = new HashSet<Integer>(noOfFactsToRead*noOfRows);
		for (int i = 0; i < noOfFactsToRead; i++)
		{
			AggDataSuggestScanner scanner = new AggDataSuggestScanner(keySize,
					dimensions);

			scanner.setKeyBlock(factDataNodes.get(i).getColumnData(fileHolder,
					dimensions, needCompression));

			scanner.setNumberOfRows(factDataNodes.get(i).getMaxKeys());
			mergedData.addAll(scanner.getLimitedDataBlock(noOfRows));

		}
		return mergedData;

	}
}
