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

package com.huawei.unibi.molap.datastorage.store.compression.type;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

import java.nio.ByteBuffer;

public class UnCompressNoneShort implements UnCompressValue<short[]> {
  /**
   * Attribute for Molap LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressNoneShort.class.getName());

  /**
   * shortCompressor.
   */
  private static Compressor<short[]> shortCompressor =
      SnappyCompression.SnappyShortCompression.INSTANCE;

  /**
   * value.
   */
  private short[] shortValue;

  @Override public void setValue(short[] shortValue) {
    this.shortValue = shortValue;

  }

  @Override public UnCompressValue getNew() {
    try {
      return (UnCompressValue) clone();
    } catch (CloneNotSupportedException cns1) {
      LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, cns1, cns1.getMessage());
    }
    return null;
  }

  @Override public UnCompressValue compress() {

    UnCompressNoneByte byte1 = new UnCompressNoneByte();
    byte1.setValue(shortCompressor.compress(shortValue));

    return byte1;

  }

  @Override public UnCompressValue uncompress(DataType dataType) {
    return null;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(shortValue);
  }

  @Override public void setValueInBytes(byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    shortValue = ValueCompressionUtil.convertToShortArray(buffer, value.length);
  }

  /**
   * @see com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue#getCompressorObject()
   */
  @Override public UnCompressValue getCompressorObject() {
    return new UnCompressNoneByte();
  }

  @Override public MolapReadDataHolder getValues(int decimal, double maxValue) {
    MolapReadDataHolder dataHolder = new MolapReadDataHolder();
    double[] vals = new double[shortValue.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = shortValue[i];
    }
    dataHolder.setReadableDoubleValues(vals);
    return dataHolder;
  }

}
