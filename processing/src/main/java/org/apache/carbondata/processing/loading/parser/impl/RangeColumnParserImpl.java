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
package org.apache.carbondata.processing.loading.parser.impl;

import java.util.ArrayList;

import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.parser.CarbonParserFactory;
import org.apache.carbondata.processing.loading.parser.GenericParser;
import org.apache.carbondata.processing.loading.parser.RowParser;

public class RangeColumnParserImpl implements RowParser {

  private GenericParser genericParser;

  public RangeColumnParserImpl(DataField rangeField, CarbonDataLoadConfiguration configuration) {
    String[] complexDelimiters =
        (String[]) configuration.getDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS);
    ArrayList<String> complexDelimiterList = new ArrayList<>(complexDelimiters.length);
    for (int index = 0; index < complexDelimiters.length; index++) {
      complexDelimiterList.add(complexDelimiters[index]);
    }
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    genericParser =
        CarbonParserFactory.createParser(rangeField.getColumn(), complexDelimiterList, nullFormat);
  }

  @Override
  public Object[] parseRow(Object[] row) {
    if (row == null || row.length < 1) {
      return new String[1];
    }
    Object[] result = new Object[1];
    result[0] = genericParser.parse(row[0]);
    return result;
  }

}
