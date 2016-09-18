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
package org.apache.carbondata.core.datastorage.store.compression.type;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

public class UnCompressDefaultLong extends UnCompressNoneLong {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressDefaultLong.class.getName());

  public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException clnNotSupportedExc) {
      LOGGER.error(clnNotSupportedExc,
          clnNotSupportedExc.getMessage());
    }
    return null;
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    long[] vals = new long[value.length];
    System.arraycopy(value, 0, vals, 0, vals.length);
    dataHolder.setReadableLongValues(vals);
    return dataHolder;
  }

}
