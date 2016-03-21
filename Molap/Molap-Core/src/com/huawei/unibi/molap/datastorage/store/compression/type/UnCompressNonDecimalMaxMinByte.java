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
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

public class UnCompressNonDecimalMaxMinByte implements UnCompressValue<byte[]> {
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(UnCompressNonDecimalMaxMinByte.class.getName());
    /**
     * byteCompressor.
     */
    private static Compressor<byte[]> byteCompressor =
            SnappyCompression.SnappyByteCompression.INSTANCE;
    /**
     * value.
     */
    private byte[] value;

    @Override public UnCompressValue getNew() {
        try {
            return (UnCompressValue) clone();
        } catch (CloneNotSupportedException cloneNotSupportedException) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, cloneNotSupportedException,
                    cloneNotSupportedException.getMessage());
        }
        return null;
    }

    @Override public UnCompressValue compress() {
        UnCompressNonDecimalMaxMinByte byte1 = new UnCompressNonDecimalMaxMinByte();
        byte1.setValue(byteCompressor.compress(value));
        return byte1;
    }

    @Override public UnCompressValue uncompress(DataType dataType) {
        UnCompressValue byte1 = ValueCompressionUtil.unCompressNonDecimalMaxMin(dataType, dataType);
        ValueCompressonHolder.unCompress(dataType, byte1, value);
        return byte1;
    }

    @Override public byte[] getBackArrayData() {
        return value;
    }

    /**
     * @see com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue#getCompressorObject()
     */
    @Override public UnCompressValue getCompressorObject() {
        return new UnCompressNonDecimalMaxMinByte();
    }

    @Override public void setValueInBytes(byte[] value) {
        this.value = value;
    }

    @Override public MolapReadDataHolder getValues(int decimalVal, double maxValue) {
        double[] vals = new double[value.length];
        MolapReadDataHolder dataHolder = new MolapReadDataHolder();
        for (int i = 0; i < vals.length; i++) {
            vals[i] = value[i] / Math.pow(10, decimalVal);

            if (value[i] == 0) {
                vals[i] = maxValue;
            } else {
                vals[i] = (maxValue - value[i]) / Math.pow(10, decimalVal);
            }

        }
        dataHolder.setReadableDoubleValues(vals);
        return dataHolder;
    }

    @Override public void setValue(byte[] value) {
        this.value = value;
    }

}
