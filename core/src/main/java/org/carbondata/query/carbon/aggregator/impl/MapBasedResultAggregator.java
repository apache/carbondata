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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.DataAggregator;
import org.carbondata.query.carbon.aggregator.ScannedResultAggregator;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.impl.MapBasedResult;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * Scanned result aggregator for aggregated query This will use hash map to
 * aggregate the query result
 */
public class MapBasedResultAggregator implements ScannedResultAggregator {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MapBasedResultAggregator.class.getName());

  /**
   * will be used to aggregate the scaned result
   */
  private Map<ByteArrayWrapper, MeasureAggregator[]> aggData;

  /**
   * interface for data aggregation
   */
  private DataAggregator dataAggregator;

  /**
   * key which will be used for aggregation
   */
  private ByteArrayWrapper wrapper;

  /**
   * aggregate which will be used to get the new aggregator
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

  public MapBasedResultAggregator(BlockExecutionInfo tableBlockExecutionInfos,
      DataAggregator dataAggregator) {
    // creating a map of bigger value to avoid rehasing
    // problem with this is if enough space is not present the memory
    // this object creation will take more time because of gc
    aggData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(100000, 1.0f);
    this.dataAggregator = dataAggregator;
    this.wrapper = new ByteArrayWrapper();
    this.tableBlockExecutionInfos = tableBlockExecutionInfos;
    blockAggregator = tableBlockExecutionInfos.getAggregatorInfo().getMeasuresAggreagators();
    restructureInfos = tableBlockExecutionInfos.getKeyStructureInfo();
  }

  /**
   * Below method will be used to aggregate the scanned result
   *
   * @param scannedResult scanned result
   * @return how many records was aggregated
   */
  @Override
  public int aggregateData(AbstractScannedResult scannedResult) {
    while (scannedResult.hasNext()) {
      // fill the keys
      wrapper.setDictionaryKey(scannedResult.getDictionaryKeyArray());
      wrapper.setNoDictionaryKeys(scannedResult.getNoDictionaryKeyArray());
      wrapper.setComplexTypesKeys(scannedResult.getComplexTypeKeyArray());
      MeasureAggregator[] measureAggregators = aggData.get(wrapper);
      // if null then row was not present in the map
      // so we need to create a new measure aggregator and
      // add it to map
      if (null == measureAggregators) {
        measureAggregators = getNewAggregator();
        ByteArrayWrapper byteArrayWrapper = wrapper;
        wrapper = new ByteArrayWrapper();
        aggData.put(byteArrayWrapper, measureAggregators);
      }
      // aggregate the measure value
      dataAggregator.aggregateData(scannedResult, measureAggregators);
    }
    return 0;
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
   */
  @Override
  public Result<Map<ByteArrayWrapper, MeasureAggregator[]>, MeasureAggregator>
        getAggregatedResult() {
    Result<Map<ByteArrayWrapper, MeasureAggregator[]>, MeasureAggregator> result =
        new MapBasedResult();
    updateAggregatedResult();
    result.addScannedResult(aggData);
    return result;
  }

  /**
   * Below method will be used to update the fixed length key with the latest
   * block key generator
   *
   * @return updated block
   */
  private void updateAggregatedResult() {
    if (!tableBlockExecutionInfos.isFixedKeyUpdateRequired() || !tableBlockExecutionInfos
        .isDimensionsExistInQuery()) {
      return;
    }
    try {
      long[] data = null;
      ByteArrayWrapper key = null;
      for (Entry<ByteArrayWrapper, MeasureAggregator[]> e : aggData.entrySet()) {
        // get the key
        key = e.getKey();
        // unpack the key with table block key generator
        data = tableBlockExecutionInfos.getBlockKeyGenerator()
            .getKeyArray(key.getDictionaryKey(), tableBlockExecutionInfos.getMaskedByteForBlock());
        // packed the key with latest block key generator
        // and generate the masked key for that key
        key.setDictionaryKey(QueryUtil
            .getMaskedKey(restructureInfos.getKeyGenerator().generateKey(data),
                restructureInfos.getMaxKey(), restructureInfos.getMaskByteRanges(),
                restructureInfos.getMaskByteRanges().length));
        aggData.put(key, e.getValue());
      }
    } catch (KeyGenException e) {
      LOGGER.error(e);
    }
  }

}
