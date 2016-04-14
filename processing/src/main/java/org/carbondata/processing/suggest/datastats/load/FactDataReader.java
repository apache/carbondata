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

package org.carbondata.processing.suggest.datastats.load;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.suggest.datastats.analysis.AggDataSuggestScanner;
import org.carbondata.query.querystats.Preference;

/**
 * Fact file reader
 *
 * @author A00902717
 */
public class FactDataReader {
    private List<FactDataNode> factDataNodes;

    private int keySize;

    private FileHolder fileHolder;

    public FactDataReader(List<FactDataNode> factDataNodes, int keySize, FileHolder fileHolder) {
        this.factDataNodes = factDataNodes;
        this.keySize = keySize;
        this.fileHolder = fileHolder;

    }

    public HashSet<Integer> getSampleFactData(int dimension, int noOfRows) {
        int[] dimensions = new int[] { dimension };
        boolean[] needCompression = new boolean[dimensions.length];
        Arrays.fill(needCompression, true);

        String configFactSize =
                CarbonProperties.getInstance().getProperty(Preference.AGG_FACT_COUNT);
        int noOfFactsToRead = factDataNodes.size();
        if (null != configFactSize && Integer.parseInt(configFactSize) < noOfFactsToRead) {
            noOfFactsToRead = Integer.parseInt(configFactSize);
        }
        HashSet<Integer> mergedData = new HashSet<Integer>(noOfFactsToRead * noOfRows);
        for (int i = 0; i < noOfFactsToRead; i++) {
            AggDataSuggestScanner scanner = new AggDataSuggestScanner(keySize, dimensions);

            scanner.setKeyBlock(
                    factDataNodes.get(i).getColumnData(fileHolder, dimensions, needCompression,null));

            scanner.setNumberOfRows(factDataNodes.get(i).getMaxKeys());
            mergedData.addAll(scanner.getLimitedDataBlock(noOfRows));

        }
        return mergedData;

    }

    /**
     * getSampleFactDataForNoDictionaryValKey.
     * @param ordinal
     * @param numberOfRows
     * @param noDictionaryValgateIndex
     * @param noDictionaryColIndexes
     */
	public void getSampleFactDataForNoDictionaryValKey(int dimension,
			int numberOfRows, int[] noDictionaryColIndexes, HashSet<byte[]> mergedData) {
		int[] dimensions = new int[] { dimension };
		boolean[] needCompression = new boolean[dimensions.length];
		Arrays.fill(needCompression, true);
		String configFactSize = CarbonProperties.getInstance().getProperty(
				Preference.AGG_FACT_COUNT);
		int noOfFactsToRead = factDataNodes.size();
		if (null != configFactSize
				&& Integer.parseInt(configFactSize) < noOfFactsToRead) {
			noOfFactsToRead = Integer.parseInt(configFactSize);
		}
		for (int i = 0; i < noOfFactsToRead; i++) {
			AggDataSuggestScanner scanner = new AggDataSuggestScanner(keySize,
					dimensions);
			ColumnarKeyStoreDataHolder dataHolder = factDataNodes.get(i)
					.getColumnData(fileHolder, dimension, false,
							noDictionaryColIndexes);
			scanner.getLimitedDataBlockForNoDictionaryVals(numberOfRows,
					mergedData, dataHolder);
		}

	}
}
