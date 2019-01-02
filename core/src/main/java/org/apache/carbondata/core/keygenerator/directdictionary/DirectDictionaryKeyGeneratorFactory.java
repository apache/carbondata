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
package org.apache.carbondata.core.keygenerator.directdictionary;

import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Factory for DirectDictionary Key generator
 */
public final class DirectDictionaryKeyGeneratorFactory {
  /**
   * private constructor
   */
  private DirectDictionaryKeyGeneratorFactory() {
  }

  /**
   * The method returns the DirectDictionaryGenerator based for direct dictionary
   * column based on dataType
   *
   * @param dataType DataType
   * @return the generator instance
   */
  public static DirectDictionaryGenerator getDirectDictionaryGenerator(DataType dataType,
      String dateFormat) {
    DirectDictionaryGenerator directDictionaryGenerator = null;
    if (dataType == DataTypes.DATE) {
      directDictionaryGenerator = new DateDirectDictionaryGenerator(dateFormat);
    } else if (dataType == DataTypes.TIMESTAMP) {
      directDictionaryGenerator = new TimeStampDirectDictionaryGenerator(dateFormat);
    }
    return directDictionaryGenerator;
  }

  public static DirectDictionaryGenerator getDirectDictionaryGenerator(DataType dataType) {
    return getDirectDictionaryGenerator(dataType, CarbonUtil.getFormatFromProperty(dataType));
  }
}
