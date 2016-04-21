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

package org.carbondata.query.processor.row;

import org.carbondata.core.util.ByteUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.processor.DataProcessorExt;
import org.carbondata.query.processor.exception.DataProcessorException;

public class AggreagtedRowProcessor extends RowProcessor {
  /**
   * prevMsrs
   */
  private MeasureAggregator[] prevMsrs;
  /**
   * prevKey
   */
  private byte[] prevKey;

  public AggreagtedRowProcessor(DataProcessorExt dataProcessor) {
    super(dataProcessor);
  }

  @Override public void processRow(final byte[] key, final MeasureAggregator[] value)
      throws DataProcessorException {
    if (prevKey != null) {
      if (ByteUtil.compare(key, prevKey) == 0) {
        aggregateData(prevMsrs, value);
      } else {
        dataProcessor.processRow(key, value);
      }
    }
    prevKey = key.clone();
    prevMsrs = value;
  }

  private void aggregateData(final MeasureAggregator[] src, final MeasureAggregator[] dest) {
    for (int i = 0; i < dest.length; i++) {
      dest[i].merge(src[i]);
    }
  }

  public void finish() throws DataProcessorException {
    dataProcessor.processRow(prevKey, prevMsrs);
    super.finish();
  }
}
