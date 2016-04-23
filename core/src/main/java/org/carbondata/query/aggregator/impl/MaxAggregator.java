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

package org.carbondata.query.aggregator.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Class Description :
 * It will return max of values
 */
public class MaxAggregator extends AbstractMeasureAggregatorMaxMin {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -5850218739083899419L;

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MaxAggregator.class.getName());

    protected void internalAgg(Object value) {
        if (value instanceof Comparable) {
            @SuppressWarnings("unchecked")
            Comparable<Object> newValue = ((Comparable<Object>) value);
            aggVal = (aggVal == null || aggVal.compareTo(newValue) < 0) ? newValue : aggVal;
        }
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        byte[] objectBytesVal = new byte[0];
        if (firstTime) {
            return objectBytesVal;
        }
        ByteArrayOutputStream bos = null;
        ObjectOutput out = null;
        try {
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);

            out.writeObject(aggVal);
            objectBytesVal = bos.toByteArray();
        } catch (Exception e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "Problem while getting byte array in maxaggregator: " + e.getMessage());
        } finally {
            CarbonUtil.closeStreams(bos);
        }
        return objectBytesVal;
    }

    /**
     * Merge the value, it will update the max aggregate value if aggregator
     * passed as an argument will have value greater than aggVal
     *
     * @param aggregator MaxAggregator
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        MaxAggregator maxAggregator = (MaxAggregator) aggregator;
        agg(maxAggregator.aggVal);
    }

    @Override
    public void writeData(DataOutput dataOutput) throws IOException {
        ByteArrayOutputStream bos = null;
        ObjectOutput out = null;

        try {
            dataOutput.writeBoolean(firstTime);
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);
            out.writeObject(aggVal);
            byte[] objectBytes = bos.toByteArray();
            dataOutput.write(objectBytes.length);
            dataOutput.write(objectBytes, 0, objectBytes.length);
        } catch (Exception e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "Problem while getting byte array in maxaggregator: " + e.getMessage());

        } finally {
            CarbonUtil.closeStreams(bos);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readData(DataInput inPut) throws IOException {

        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        try {
            firstTime = inPut.readBoolean();
            int len = inPut.readInt();
            byte[] data = new byte[len];
            bis = new ByteArrayInputStream(data);
            in = new ObjectInputStream(bis);
            aggVal = (Comparable<Object>) in.readObject();
        } catch (Exception e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "Problem while getting byte array in maxaggregator: " + e.getMessage());
        } finally {
            CarbonUtil.closeStreams(bis);

        }
    }

    @Override
    public MeasureAggregator getCopy() {
        MaxAggregator aggregator = new MaxAggregator();
        aggregator.aggVal = aggVal;
        aggregator.firstTime = firstTime;
        return aggregator;
    }

    @Override
    public void merge(byte[] value) {
        if (0 == value.length) {
            return;
        }
        ByteArrayInputStream bytesInputStream = null;
        ObjectInput in = null;
        try {
            bytesInputStream = new ByteArrayInputStream(value);
            in = new ObjectInputStream(bytesInputStream);
            Object newVal = (Comparable<Object>) in.readObject();
            internalAgg(newVal);
            firstTime = false;
        } catch (Exception e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "Problem while merging byte array in maxaggregator: " + e.getMessage());
        } finally {
            CarbonUtil.closeStreams(bytesInputStream);
        }
    }

	@Override
	public MeasureAggregator getNew() {
		return new MaxAggregator();
	}
}
