/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.store;

import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.processing.store.writer.CarbonFactDataWriter;
import org.apache.carbondata.processing.store.writer.v3.CarbonFactDataWriterImplV3;

/**
 * Factory class to get the writer instance
 */
class CarbonDataWriterFactory {

  /**
   * static instance
   */
  private static final CarbonDataWriterFactory CARBON_DATA_WRITER_FACTORY =
      new CarbonDataWriterFactory();

  /**
   * private constructor
   */
  private CarbonDataWriterFactory() {
    // TODO Auto-generated constructor stub
  }

  /**
   * Below method will be used to get the instance of factory class
   *
   * @return fact class instance
   */
  public static CarbonDataWriterFactory getInstance() {
    return CARBON_DATA_WRITER_FACTORY;
  }

  /**
   * Below method will be used to get the writer instance based on version
   *
   * @param version writer version
   * @param model   CarbonFactDataHandlerModel object
   * @return writer instance
   */
  public CarbonFactDataWriter getFactDataWriter(final ColumnarFormatVersion version,
      final CarbonFactDataHandlerModel model) {
    switch (version) {
      case V3:
        return new CarbonFactDataWriterImplV3(model);
      default:
        throw new UnsupportedOperationException("V1 and V2 CarbonData Writer is not supported");
    }
  }

}
