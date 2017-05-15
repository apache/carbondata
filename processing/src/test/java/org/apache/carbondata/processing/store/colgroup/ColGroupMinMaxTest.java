/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.processing.store.colgroup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * RowStore store min max test
 */
public class ColGroupMinMaxTest {

	/**
	 * column groups
	 */
	int[][] columnGroups;

	/**
	 * surrogate key
	 */
	int[][] data;
	/**
	 * mdkey data
	 */
	byte[][] mdkeyData;

	/**
	 * min value of surrogates
	 */
	int[] min;
	/**
	 * max value of surrogates
	 */
	int[] max;
	private ColGroupMinMax[] colGrpMinMax;
	
	private SegmentProperties segmentProperties;

	@Before
	public void setupBeforeClass() throws KeyGenException {
		int[] dimLens = new int[] { 100000, 1000, 10, 100, 100, 10000, 1000, 10,
				1000, 1 };
		columnGroups = new int[][] { { 0, 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8, 9 } };
		segmentProperties = getSegmentProperties(dimLens, columnGroups);
		initColGrpMinMax();
		Random random = new Random();
		data = new int[1000][];
		min = new int[dimLens.length];
		Arrays.fill(min, Integer.MAX_VALUE);
		max = new int[dimLens.length];
		Arrays.fill(max, Integer.MIN_VALUE);
		for (int i = 0; i < 1000; i++) {

			data[i] = new int[dimLens.length];
			for (int j = 0; j < data[i].length; j++) {
				data[i][j] = random.nextInt(dimLens[j]);
			}
			setMinData(data[i]);
			setMaxData(data[i]);
			System.out.println(Arrays.toString(data[i]));
		}
		mdkeyData = new byte[1000][];
		for (int i = 0; i < 1000; i++) {
			mdkeyData[i] = segmentProperties.getDimensionKeyGenerator().generateKey(data[i]);
			evaluateColGrpMinMax(mdkeyData[i]);
		}
	}

	private SegmentProperties getSegmentProperties(int[] dimLens, int[][] columnGroups) {
		List<ColumnSchema> columnSchemas = new ArrayList<>();
		for(int i=0;i<columnGroups.length;i++) {
			  for(int j=0;j<columnGroups[i].length;j++) {
			  	  columnSchemas.add(getDimensionColumn(i+j,i));
			  }
			
		}
		return new SegmentProperties(columnSchemas, dimLens);
	}
	private ColumnSchema getDimensionColumn(int var , int groupId) {
  ColumnSchema dimColumn = new ColumnSchema();
  dimColumn.setColumnar(false);
  dimColumn.setColumnName("IMEI"+var);
  dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
  dimColumn.setDataType(DataType.STRING);
  dimColumn.setDimensionColumn(true);
  List<Encoding> encodeList =
      new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  encodeList.add(Encoding.DICTIONARY);
  dimColumn.setEncodingList(encodeList);
  dimColumn.setColumnGroup(0);
  dimColumn.setNumberOfChild(0);
  return dimColumn;
}

	private void evaluateColGrpMinMax(byte[] mdkey) {

		for (int colGrp = 0; colGrp < segmentProperties.getColumnGroups().length; colGrp++) {
			if (segmentProperties.getColumnGroups()[colGrp].length > 0) {
				colGrpMinMax[colGrp].add(mdkey);
			}
		}
	}

	private void initColGrpMinMax() {
		int[][] colGrps = segmentProperties.getColumnGroups();
		colGrpMinMax = new ColGroupMinMax[colGrps.length];
		for (int colGrp = 0; colGrp < colGrps.length; colGrp++) {
			if (colGrps[colGrp].length > 0) {
				colGrpMinMax[colGrp] = new ColGroupMinMax(segmentProperties,
						colGrp);
			}
		}
	}

	private void setMaxData(int[] data) {
		for (int i = 0; i < max.length; i++) {
			if (max[i] < data[i]) {
				max[i] = data[i];
			}
		}

	}

	private void setMinData(int[] data) {
		for (int i = 0; i < min.length; i++) {
			if (min[i] > data[i]) {
				min[i] = data[i];
			}
		}
	}

	@Ignore
	@Test
	public void testRowStoreMinMax() throws KeyGenException {

		DataHolder[] dataHolders = getDataHolders(segmentProperties.getColumnGroupModel().getNoOfColumnStore(),
				mdkeyData.length);
		for (int i = 0; i < mdkeyData.length; i++) {
			byte[][] split = segmentProperties.getFixedLengthKeySplitter().splitKey(mdkeyData[i]);
			for (int j = 0; j < split.length; j++) {
				dataHolders[j].addData(split[j], i);
			}

		}
		ColGroupBlockStorage[] rowBlockStorage = new ColGroupBlockStorage[dataHolders.length];
		for (int i = 0; i < dataHolders.length; i++) {

			rowBlockStorage[i] = new ColGroupBlockStorage(dataHolders[i]);
		}
		int[][] columnGroup = segmentProperties.getColumnGroups();
		for (int i = 0; i < dataHolders.length; i++) {
			assertMinMax(colGrpMinMax[i].getMin(), rowBlockStorage[i].getMax(),
					columnGroup[i]);
		}

	}

	private void assertMinMax(byte[] min, byte[] max, int[] columnGroup)
			throws KeyGenException {

		int columnStartIndex = 0;
		for (int i = 0; i < columnGroup.length; i++) {
			int col = columnGroup[i];
			int[] maskByteRange = getMaskByteRange(col);
			int[] maskBytePosition = new int[segmentProperties.getDimensionKeyGenerator().getKeySizeInBytes()];
			updateMaskedKeyRanges(maskBytePosition, maskByteRange);

			byte[] columnMin = new byte[maskByteRange.length];
			System.arraycopy(min, columnStartIndex, columnMin, 0, maskByteRange.length);
			byte[] columnMax = new byte[maskByteRange.length];
			System.arraycopy(max, columnStartIndex, columnMax, 0, maskByteRange.length);

			long[] minKeyArray = segmentProperties.getDimensionKeyGenerator().getKeyArray(columnMin, maskBytePosition);
			long[] maxKeyArray = segmentProperties.getDimensionKeyGenerator().getKeyArray(columnMax, maskBytePosition);
			System.out.println("calculated:(min,max) for column " + col + ":("
					+ minKeyArray[col] + "," + maxKeyArray[col] + ")");
			System.out.println("correct:(min,max) for column " + col + ":("
					+ this.min[col] + "," + this.max[col] + ")");
			columnStartIndex += maskByteRange.length;
			Assert.assertEquals(minKeyArray[col], this.min[col]);
			Assert.assertEquals(maxKeyArray[col], this.max[col]);

		}

	}

	private DataHolder[] getDataHolders(int noOfColumn, int noOfRow) {
		DataHolder[] dataHolders = new DataHolder[noOfColumn];
		for (int colGrp = 0; colGrp < noOfColumn; colGrp++) {
			if (segmentProperties.getColumnGroupModel().isColumnar(colGrp)) {
				dataHolders[colGrp] = new ColumnDataHolder(noOfRow);
			} else {
				dataHolders[colGrp] = new ColGroupDataHolder(
						segmentProperties.getFixedLengthKeySplitter().getBlockKeySize()[colGrp], noOfRow,
						colGrpMinMax[colGrp]);
			}
		}
		return dataHolders;
	}

	private int[] getMaskByteRange(int col) {
		Set<Integer> integers = new HashSet<>();
		int[] range = segmentProperties.getDimensionKeyGenerator().getKeyByteOffsets(col);
		for (int j = range[0]; j <= range[1]; j++) {
			integers.add(j);
		}
		int[] byteIndexs = new int[integers.size()];
		int j = 0;
		for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext();) {
			Integer integer = (Integer) iterator.next();
			byteIndexs[j++] = integer.intValue();
		}
		return byteIndexs;
	}

	private void updateMaskedKeyRanges(int[] maskedKey, int[] maskedKeyRanges) {
		Arrays.fill(maskedKey, -1);
		for (int i = 0; i < maskedKeyRanges.length; i++) {
			maskedKey[maskedKeyRanges[i]] = i;
		}
	}
}
