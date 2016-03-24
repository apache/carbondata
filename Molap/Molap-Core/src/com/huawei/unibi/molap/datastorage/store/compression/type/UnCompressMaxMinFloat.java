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

import java.nio.ByteBuffer;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

public class UnCompressMaxMinFloat implements UnCompressValue<float[]> {

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(UnCompressMaxMinFloat.class.getName());
    /**
     * floatCompressor
     */
    private static Compressor<float[]> floatCompressor =
            SnappyCompression.SnappyFloatCompression.INSTANCE;
    /**
     * value.
     */
    private float[] value;

    @Override public void setValue(float[] value) {
        this.value = (float[]) value;

    }

    @Override public UnCompressValue getNew() {
        try {
            return (UnCompressValue) clone();
        } catch (CloneNotSupportedException ex4) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, ex4, ex4.getMessage());
        }
        return null;
    }

    @Override public UnCompressValue compress() {

        UnCompressMaxMinByte byte1 = new UnCompressMaxMinByte();
        byte1.setValue(floatCompressor.compress(value));
        return byte1;
    }

    @Override public UnCompressValue uncompress(DataType dTypeVal) {
        return null;
    }

    @Override public byte[] getBackArrayData() {
        return ValueCompressionUtil.convertToBytes(value);
    }

    @Override public void setValueInBytes(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        this.value = ValueCompressionUtil.convertToFloatArray(buffer, value.length);
    }

    /**
     * @see com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue#getCompressorObject()
     */
    @Override public UnCompressValue getCompressorObject() {
        return new UnCompressMaxMinByte();
    }

    @Override public MolapReadDataHolder getValues(int decimal, Object maxValueObject) {
        double maxValue = (double) maxValueObject;
        double[] vals = new double[value.length];
        MolapReadDataHolder dataHolderVal = new MolapReadDataHolder();
        for (int i = 0; i < vals.length; i++) {
            if (value[i] == 0) {
                vals[i] = maxValue;
            } else {
                vals[i] = maxValue - value[i];
            }

        }
        dataHolderVal.setReadableDoubleValues(vals);
        return dataHolderVal;
    }

}
