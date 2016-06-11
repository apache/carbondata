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

import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * AbstractMeasureAggregatorDummy
 * Used for custom Carbon Aggregator dummy
 */
public abstract class AbstractMeasureAggregatorDummy extends AbstractMeasureAggregatorBasic {
  private static final long serialVersionUID = 1L;

  @Override public int compareTo(MeasureAggregator o) {
    if (equals(o)) {
      return 0;
    }
    return -1;
  }

  @Override public boolean equals(Object arg0) {
    return super.equals(arg0);
  }

  @Override public int hashCode() {
    return super.hashCode();
  }

  @Override public byte[] getByteArray() {
    return null;
  }

  @Override public void merge(MeasureAggregator aggregator) {
  }

  @Override public MeasureAggregator getCopy() {
    return null;
  }

  @Override public void writeData(DataOutput output) throws IOException {
  }

  @Override public void readData(DataInput inPut) throws IOException {
  }

  @Override public void merge(byte[] value) {
  }
}
