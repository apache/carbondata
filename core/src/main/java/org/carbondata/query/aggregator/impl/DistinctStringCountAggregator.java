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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;

public class DistinctStringCountAggregator implements MeasureAggregator {
    private static final long serialVersionUID = 6313463368629960186L;

    private Set<String> valueSetForStr;

    public DistinctStringCountAggregator() {
        this.valueSetForStr = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    public void agg(double newVal) {
    }

    public void agg(String newVal) {
        this.valueSetForStr.add(newVal);
    }

    private void agg(Set<String> set2) {
        this.valueSetForStr.addAll(set2);
    }

    public void merge(MeasureAggregator aggregator) {
        DistinctStringCountAggregator distinctCountAggregator =
                (DistinctStringCountAggregator) aggregator;
        agg(distinctCountAggregator.valueSetForStr);
    }

    public Double getDoubleValue() {
        return (double) this.valueSetForStr.size();
    }

    public Long getLongValue() {
        return (long) this.valueSetForStr.size();
    }

    public BigDecimal getBigDecimalValue() {
        return new BigDecimal(this.valueSetForStr.size());
    }

    public Object getValueObject() {
        return Integer.valueOf(this.valueSetForStr.size());
    }

    public void setNewValue(Object newValue) {
    }

    public boolean isFirstTime() {
        return false;
    }

    public void writeData(DataOutput output) throws IOException {
        int length = this.valueSetForStr.size() * 8;
        ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4);
        byteBuffer.putInt(length);
        for (String val : this.valueSetForStr) {
            byte[] b = val.getBytes(Charset.defaultCharset());
            byteBuffer.putInt(b.length);
            byteBuffer.put(b);
        }
        byteBuffer.flip();
        output.write(byteBuffer.array());
    }

    public void readData(DataInput inPut) throws IOException {
        int length = inPut.readInt();
        length /= 8;
        this.valueSetForStr = new HashSet<String>(length + 1, 1.0F);
        for (int i = 0; i < length; i++) {
            byte[] b = new byte[inPut.readInt()];
            inPut.readFully(b);
            this.valueSetForStr.add(new String(b, Charset.defaultCharset()));
        }
    }

    public MeasureAggregator getCopy() {
        DistinctStringCountAggregator aggregator = new DistinctStringCountAggregator();
        aggregator.valueSetForStr = new HashSet<String>(this.valueSetForStr);
        return aggregator;
    }

    public int compareTo(MeasureAggregator o) {
        double val = getDoubleValue();
        double otherVal = o.getDoubleValue();
        if (val > otherVal) {
            return 1;
        }
        if (val < otherVal) {
            return -1;
        }
        return 0;
    }

    @Override
    public void agg(Object newVal) {
        this.valueSetForStr.add((String) newVal);
    }

    @Override
    public void agg(MeasureColumnDataChunk dataChunk, int index) {
    }

    @Override
    public byte[] getByteArray() {
        return null;
    }

    @Override
    public MeasureAggregator get() {
        return this;
    }

    public String toString() {
        return valueSetForStr.size() + "";
    }

    @Override
    public void merge(byte[] value) {
    }

	@Override
	public MeasureAggregator getNew() {
		// TODO Auto-generated method stub
		return new DistinctStringCountAggregator();
	}

}
