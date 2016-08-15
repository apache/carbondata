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

package org.apache.carbondata.core.datastorage.store.impl.data.uncompressed;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;

public abstract class AbstractDoubleArrayDataStore implements NodeMeasureDataStore {

  protected ValueCompressonHolder.UnCompressValue[] values;

  protected ValueCompressionModel compressionModel;

  private char[] type;

  public AbstractDoubleArrayDataStore(ValueCompressionModel compressionModel) {
    this.compressionModel = compressionModel;
    if (null != compressionModel) {
      values =
          new ValueCompressonHolder.UnCompressValue[compressionModel.getUnCompressValues().length];
      type = compressionModel.getType();
    }
  }

  @Override public byte[][] getWritableMeasureDataArray(CarbonWriteDataHolder[] dataHolder) {
    values =
        new ValueCompressonHolder.UnCompressValue[compressionModel.getUnCompressValues().length];
    for (int i = 0; i < compressionModel.getUnCompressValues().length; i++) {
      values[i] = compressionModel.getUnCompressValues()[i].getNew();
      if (type[i] != CarbonCommonConstants.BYTE_VALUE_MEASURE
          && type[i] != CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        if (type[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
          values[i].setValue(ValueCompressionUtil
              .getCompressedValues(compressionModel.getCompType()[i],
                  dataHolder[i].getWritableLongValues(), compressionModel.getChangedDataType()[i],
                  (long) compressionModel.getMaxValue()[i], compressionModel.getDecimal()[i]));
        } else {
          values[i].setValue(ValueCompressionUtil
              .getCompressedValues(compressionModel.getCompType()[i],
                  dataHolder[i].getWritableDoubleValues(), compressionModel.getChangedDataType()[i],
                  (double) compressionModel.getMaxValue()[i], compressionModel.getDecimal()[i]));
        }
      } else {
        values[i].setValue(dataHolder[i].getWritableByteArrayValues());
      }
    }

    byte[][] resturnValue = new byte[values.length][];

    for (int i = 0; i < values.length; i++) {
      resturnValue[i] = values[i].getBackArrayData();
    }
    return resturnValue;
  }

  @Override public short getLength() {
    return values != null ? (short) values.length : 0;
  }

}

