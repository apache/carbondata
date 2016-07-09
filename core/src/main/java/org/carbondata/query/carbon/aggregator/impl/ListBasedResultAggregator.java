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
import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.query.carbon.aggregator.DataAggregator;
import org.carbondata.query.carbon.aggregator.ScannedResultAggregator;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.impl.ListBasedResult;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * It is not a aggregator it is just a scanned result holder.
 *
 * @TODO change it to some other name
 */
public class ListBasedResultAggregator implements ScannedResultAggregator {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ListBasedResultAggregator.class.getName());

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
   * restructuring info
   */
  private KeyStructureInfo restructureInfos;

  /**
   * table block execution infos
   */
  private BlockExecutionInfo tableBlockExecutionInfos;

  private int[] measuresOrdinal;

  /**
   * to check whether measure exists in current table block or not this to
   * handle restructuring scenario
   */
  private boolean[] isMeasureExistsInCurrentBlock;

  /**
   * default value of the measures in case of restructuring some measure wont
   * be present in the table so in that default value will be used to
   * aggregate the data for that measure columns
   */
  private Object[] measureDefaultValue;

  /**
   * measure datatypes.
   */
  private DataType[] measureDatatypes;

  public ListBasedResultAggregator(BlockExecutionInfo blockExecutionInfos,
      DataAggregator aggregator) {
    limit = blockExecutionInfos.getLimit();
    this.tableBlockExecutionInfos = blockExecutionInfos;
    restructureInfos = blockExecutionInfos.getKeyStructureInfo();
    measuresOrdinal = tableBlockExecutionInfos.getAggregatorInfo().getMeasureOrdinals();
    isMeasureExistsInCurrentBlock = tableBlockExecutionInfos.getAggregatorInfo().getMeasureExists();
    measureDefaultValue = tableBlockExecutionInfos.getAggregatorInfo().getDefaultValues();
    this.measureDatatypes = tableBlockExecutionInfos.getAggregatorInfo().getMeasureDataTypes();
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   * @param scanned result
   *
   */
  @Override
  public int aggregateData(AbstractScannedResult scannedResult) {
    this.listBasedResult =
        new ArrayList<>(limit == -1 ? scannedResult.numberOfOutputRows() : limit);
    boolean isMsrsPresent = measureDatatypes.length > 0;
    ByteArrayWrapper wrapper = null;
    // scan the record and add to list
    ListBasedResultWrapper resultWrapper;
    while (scannedResult.hasNext() && (limit == -1 || rowCounter < limit)) {
      resultWrapper = new ListBasedResultWrapper();
      if (tableBlockExecutionInfos.isDimensionsExistInQuery()) {
        wrapper = new ByteArrayWrapper();
        wrapper.setDictionaryKey(scannedResult.getDictionaryKeyArray());
        wrapper.setNoDictionaryKeys(scannedResult.getNoDictionaryKeyArray());
        wrapper.setComplexTypesKeys(scannedResult.getComplexTypeKeyArray());
        resultWrapper.setKey(wrapper);
      } else {
        scannedResult.incrementCounter();
      }
      if (isMsrsPresent) {
        Object[] msrValues = new Object[measureDatatypes.length];
        fillMeasureData(msrValues, scannedResult);
        resultWrapper.setValue(msrValues);
      }
      listBasedResult.add(resultWrapper);
      rowCounter++;
    }
    return rowCounter;
  }

  private void fillMeasureData(Object[] msrValues, AbstractScannedResult scannedResult) {
    for (short i = 0; i < measuresOrdinal.length; i++) {
      // if measure exists is block then pass measure column
      // data chunk to the aggregator
      if (isMeasureExistsInCurrentBlock[i]) {
        msrValues[i] = getMeasureData(scannedResult.getMeasureChunk(measuresOrdinal[i]),
            scannedResult.getCurrenrRowId(), measureDatatypes[i]);
      } else {
        // if not then get the default value and use that value in aggregation
        msrValues[i] = measureDefaultValue[i];
      }
    }
  }

  private Object getMeasureData(MeasureColumnDataChunk dataChunk, int index, DataType dataType) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      Object msrVal;
      switch (dataType) {
        case LONG:
          msrVal = dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(index);
          break;
        case DECIMAL:
          msrVal = dataChunk.getMeasureDataHolder().getReadableBigDecimalValueByIndex(index);
          break;
        default:
          msrVal = dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index);
      }
      return DataTypeUtil.getMeasureDataBasedOnDataType(msrVal, dataType);
    }
    return null;
  }

  /**
   * Below method will used to get the result
   */
  @Override public Result<List<ListBasedResultWrapper>, Object> getAggregatedResult() {
    Result<List<ListBasedResultWrapper>, Object> result = new ListBasedResult();
    if (tableBlockExecutionInfos.isFixedKeyUpdateRequired() && tableBlockExecutionInfos
        .isDimensionsExistInQuery()) {
      updateKeyWithLatestBlockKeygenerator();
      result.addScannedResult(listBasedResult);
    } else {
      result.addScannedResult(listBasedResult);
    }
    return result;
  }

  /**
   * Below method will be used to update the fixed length key with the
   * latest block key generator
   *
   * @return updated block
   */
  private void updateKeyWithLatestBlockKeygenerator() {
    try {
      long[] data = null;
      ByteArrayWrapper key = null;
      for (int i = 0; i < listBasedResult.size(); i++) {
        // get the key
        key = listBasedResult.get(i).getKey();
        // unpack the key with table block key generator
        data = tableBlockExecutionInfos.getBlockKeyGenerator()
            .getKeyArray(key.getDictionaryKey(), tableBlockExecutionInfos.getMaskedByteForBlock());
        // packed the key with latest block key generator
        // and generate the masked key for that key
        key.setDictionaryKey(QueryUtil
            .getMaskedKey(restructureInfos.getKeyGenerator().generateKey(data),
                restructureInfos.getMaxKey(), restructureInfos.getMaskByteRanges(),
                restructureInfos.getMaskByteRanges().length));
        listBasedResult.get(i).setKey(key);
      }
    } catch (KeyGenException e) {
      LOGGER.error(e);
    }
  }

}
