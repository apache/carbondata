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

package org.carbondata.query.aggregator.impl.min;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.impl.AbstractMeasureAggregatorMaxMin;

public abstract class AbstractMinAggregator extends AbstractMeasureAggregatorMaxMin {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractMinAggregator.class.getName());

  protected void internalAgg(Object value) {
    if (value instanceof Comparable) {
      @SuppressWarnings("unchecked") Comparable<Object> newValue = ((Comparable<Object>) value);
      aggVal = (aggVal == null || aggVal.compareTo(newValue) > 0) ? newValue : aggVal;
    }
  }

  @Override public void merge(byte[] value) {
    if (0 == value.length) {
      return;
    }
    ByteArrayInputStream bis = null;
    ObjectInput objectInput = null;
    try {
      bis = new ByteArrayInputStream(value);
      objectInput = new ObjectInputStream(bis);
      Object newVal = (Comparable<Object>) objectInput.readObject();
      internalAgg(newVal);
      firstTime = false;
    } catch (Exception e) {
      LOGGER.error(e, "Problem while merging byte array in minAggregator: " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(bis);
    }
  }
}
