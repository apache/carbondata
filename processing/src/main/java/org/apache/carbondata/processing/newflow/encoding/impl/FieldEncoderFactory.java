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
package org.apache.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;

public class FieldEncoderFactory {

  private static FieldEncoderFactory instance;

  private FieldEncoderFactory() {

  }

  public static FieldEncoderFactory getInstance() {
    if (instance == null) {
      instance = new FieldEncoderFactory();
    }
    return instance;
  }

  public FieldEncoder createFieldEncoder(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, int index) {
    if (dataField.hasDictionaryEncoding()) {
      return new DictionaryFieldEncoderImpl(dataField, cache, carbonTableIdentifier, index);
    } else if (dataField.getColumn().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      return new DirectDictionaryFieldEncoderImpl(dataField, index);
    } else if (dataField.getColumn().isComplex()) {
      return new ComplexFieldEncoderImpl();
    } else if ((dataField.getColumn().hasEncoding(Encoding.DICTIONARY) || dataField.getColumn()
        .hasEncoding(Encoding.DIRECT_DICTIONARY))) {
      return new NonDictionaryFieldEncoderImpl(dataField, index);
    } else if (!dataField.getColumn().isDimesion()) {
      return new MeasureFieldEncoderImpl(dataField, index);
    }
    return new NonDictionaryFieldEncoderImpl(dataField, index);
  }
}
