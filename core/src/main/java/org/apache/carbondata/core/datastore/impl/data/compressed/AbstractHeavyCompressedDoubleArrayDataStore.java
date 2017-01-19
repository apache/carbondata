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

package org.apache.carbondata.core.datastore.impl.data.compressed;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.NodeMeasureDataStore;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.datastore.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;

public abstract class AbstractHeavyCompressedDoubleArrayDataStore
    implements NodeMeasureDataStore //NodeMeasureDataStore<double[]>
{

  private LogService LOGGER =
      LogServiceFactory.getLogService(AbstractHeavyCompressedDoubleArrayDataStore.class.getName());

  /**
   * values.
   */
  protected ValueCompressionHolder[] values;

  /**
   * compressionModel.
   */
  protected WriterCompressModel compressionModel;

  /**
   * type
   */
  private char[] type;

  /**
   * AbstractHeavyCompressedDoubleArrayDataStore constructor.
   *
   * @param compressionModel
   */
  public AbstractHeavyCompressedDoubleArrayDataStore(WriterCompressModel compressionModel) {
    this.compressionModel = compressionModel;
    if (null != compressionModel) {
      this.type = compressionModel.getType();
      values =
          new ValueCompressionHolder[compressionModel.getValueCompressionHolder().length];
    }
  }

  // this method first invokes encoding routine to encode the data chunk,
  // followed by invoking compression routine for preparing the data chunk for writing.
  @Override public byte[][] getWritableMeasureDataArray(CarbonWriteDataHolder[] dataHolder) {
    byte[][] returnValue = new byte[values.length][];
    for (int i = 0; i < compressionModel.getValueCompressionHolder().length; i++) {
      values[i] = compressionModel.getValueCompressionHolder()[i];
      if (type[i] != CarbonCommonConstants.BYTE_VALUE_MEASURE
          && type[i] != CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        // first perform encoding of the data chunk
        values[i].setValue(
            ValueCompressionUtil.getValueCompressor(compressionModel.getCompressionFinders()[i])
                .getCompressedValues(compressionModel.getCompressionFinders()[i], dataHolder[i],
                    compressionModel.getMaxValue()[i],
                    compressionModel.getMantissa()[i]));
      } else {
        values[i].setValue(dataHolder[i].getWritableByteArrayValues());
      }
      values[i].compress();
      returnValue[i] = values[i].getCompressedData();
    }

    return returnValue;
  }

}
