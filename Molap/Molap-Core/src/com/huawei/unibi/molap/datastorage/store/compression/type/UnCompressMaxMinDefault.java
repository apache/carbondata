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

public class UnCompressMaxMinDefault implements UnCompressValue<double[]> {

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(UnCompressMaxMinDefault.class.getName());

    /**
     * doubleCompressor.
     */
    private static Compressor<double[]> doubleCompressor =
            SnappyCompression.SnappyDoubleCompression.INSTANCE;
    /**
     * value.
     */
    private double[] value;

    @Override public void setValue(double[] value) {
        this.value = (double[]) value;

    }

    @Override public UnCompressValue getNew() {
        try {
            return (UnCompressValue) clone();
        } catch (CloneNotSupportedException ex5) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, ex5, ex5.getMessage());
        }
        return null;
    }

    @Override public UnCompressValue compress() {
        UnCompressMaxMinByte byte1 = new UnCompressMaxMinByte();
        byte1.setValue(doubleCompressor.compress(value));
        return byte1;
    }

    @Override public UnCompressValue uncompress(DataType dataType) {
        return null;
    }

    @Override public byte[] getBackArrayData() {
        return ValueCompressionUtil.convertToBytes(value);
    }

    @Override public void setValueInBytes(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        this.value = ValueCompressionUtil.convertToDoubleArray(buffer, value.length);
    }

    /**
     * @see com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue#getCompressorObject()
     */
    @Override public UnCompressValue getCompressorObject() {
        return new UnCompressMaxMinByte();
    }

    //TODO SIMIAN
    @Override public MolapReadDataHolder getValues(int decimal, Object maxValueObject) {
        double maxValue = (double) maxValueObject;
        double[] vals = new double[value.length];
        MolapReadDataHolder dataHolderInfoObj = new MolapReadDataHolder();
        for (int i = 0; i < vals.length; i++) {
            if (value[i] == 0) {
                vals[i] = maxValue;
            } else {
                vals[i] = maxValue - value[i];
            }

        }
        dataHolderInfoObj.setReadableDoubleValues(vals);
        return dataHolderInfoObj;
    }

}
