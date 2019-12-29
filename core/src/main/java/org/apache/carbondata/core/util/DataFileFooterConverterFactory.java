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

package org.apache.carbondata.core.util;

import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

/**
 * Factory class to get the thrift reader object based on version
 */
public class DataFileFooterConverterFactory {

  /**
   * static instance
   */
  private static final DataFileFooterConverterFactory FOOTER_CONVERTER_FACTORY =
      new DataFileFooterConverterFactory();

  /**
   * private constructor
   */
  private DataFileFooterConverterFactory() {

  }

  /**
   * Below method will be used to get the instance of this class
   *
   * @return DataFileFooterConverterFactory instance
   */
  public static DataFileFooterConverterFactory getInstance() {
    return FOOTER_CONVERTER_FACTORY;
  }

  /**
   * Method will be used to get the file footer converter instance based on version
   *
   * @param version
   * @return footer reader instance
   */
  public AbstractDataFileFooterConverter getDataFileFooterConverter(
      final ColumnarFormatVersion version) {
    switch (version) {
      case V3:
        return new DataFileFooterConverterV3();
      default:
        throw new UnsupportedOperationException("Unsupported file version: " + version);
    }
  }

}
