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

package org.carbondata.processing.merger.columnar.iterator.impl;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.processing.factreader.CarbonSurrogateTupleHolder;
import org.carbondata.processing.factreader.FactReaderInfo;
import org.carbondata.processing.factreader.columnar.CarbonColumnarBlockletIterator;
import org.carbondata.processing.iterator.CarbonIterator;
import org.carbondata.processing.merger.columnar.iterator.CarbonDataIterator;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;

public class CarbonColumnarLeafTupleDataIterator
    implements CarbonDataIterator<CarbonSurrogateTupleHolder> {

  /**
   * unique value if slice
   */
  private Object[] uniqueValue;

  /**
   * hash next
   */
  private boolean hasNext;

  /**
   * blocklet iterator
   */
  private CarbonIterator<AbstractColumnarScanResult> blockletIterator;

  /**
   * measureCount
   */
  private int measureCount;

  /**
   * aggType
   */
  private char[] aggType;

  /**
   * keyValue
   */
  private AbstractColumnarScanResult keyValue;

  /**
   * tuple
   */
  private CarbonSurrogateTupleHolder currentTuple;

  /**
   * isMeasureUpdateResuired
   */
  private boolean isMeasureUpdateResuired;

  /**
   * CarbonSliceTupleIterator constructor to initialise
   *
   * @param mdkeyLength mdkey length
   */
  public CarbonColumnarLeafTupleDataIterator(String sliceLocation, CarbonFile[] factFiles,
      FactReaderInfo factItreatorInfo, int mdkeyLength) {
    this.measureCount = factItreatorInfo.getMeasureCount();
    ValueCompressionModel compressionModelObj =
        getCompressionModel(sliceLocation, factItreatorInfo.getTableName(), measureCount);
    this.uniqueValue = compressionModelObj.getUniqueValue();
    this.blockletIterator =
        new CarbonColumnarBlockletIterator(factFiles, mdkeyLength, compressionModelObj,
            factItreatorInfo);
    this.aggType = compressionModelObj.getType();
    initialise();
    this.isMeasureUpdateResuired = factItreatorInfo.isUpdateMeasureRequired();
  }

  /**
   * below method will be used to initialise
   */
  private void initialise() {
    if (this.blockletIterator.hasNext()) {
      keyValue = blockletIterator.next();
      this.hasNext = true;
    }
  }

  /**
   * This method will be used to get the compression model for slice
   *
   * @param measureCount measure count
   * @return compression model
   */
  private ValueCompressionModel getCompressionModel(String sliceLocation, String tableName,
      int measureCount) {
    ValueCompressionModel compressionModelObj = ValueCompressionUtil.getValueCompressionModel(
        sliceLocation + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
            + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT, measureCount);
    return compressionModelObj;
  }

  /**
   * below method will be used to get the measure value from measure data
   * wrapper
   *
   * @return
   */
  private Object[] getMeasure() {
    Object[] measures = new Object[measureCount];
    Object values = 0;
    for (int i = 0; i < measures.length; i++) {
      if (aggType[i] == 'n') {
        values = keyValue.getDoubleValue(i);
        if (this.isMeasureUpdateResuired && !values.equals(uniqueValue[i])) {
          measures[i] = values;
        }
      } else {
        measures[i] = keyValue.getByteArrayValue(i);
      }
    }
    return measures;
  }

  @Override public boolean hasNext() {
    return hasNext;
  }

  @Override public void fetchNextData() {
    CarbonSurrogateTupleHolder tuple = new CarbonSurrogateTupleHolder();
    tuple.setSurrogateKey(keyValue.getKeyArray());
    tuple.setMeasures(getMeasure());
    if (keyValue.hasNext()) {
      this.currentTuple = tuple;
    } else if (!blockletIterator.hasNext()) {
      hasNext = false;
    } else {
      initialise();
    }
    this.currentTuple = tuple;
  }

  @Override public CarbonSurrogateTupleHolder getNextData() {
    return this.currentTuple;
  }
}
