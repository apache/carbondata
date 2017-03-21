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
package org.apache.carbondata.processing.newflow.parser.impl;

import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.parser.CarbonParserFactory;
import org.apache.carbondata.processing.newflow.parser.GenericParser;
import org.apache.carbondata.processing.newflow.parser.RowParser;

public class RowParserImpl implements RowParser {

  private GenericParser[] genericParsers;

  private int[] outputMapping;

  private int[] inputMapping;

  private int numberOfColumns;

  public RowParserImpl(DataField[] output, CarbonDataLoadConfiguration configuration) {
    String[] complexDelimiters =
        (String[]) configuration.getDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS);
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    DataField[] input = getInput(configuration);
    genericParsers = new GenericParser[input.length];
    for (int i = 0; i < genericParsers.length; i++) {
      genericParsers[i] =
          CarbonParserFactory.createParser(input[i].getColumn(), complexDelimiters, nullFormat);
    }
    outputMapping = new int[output.length];
    for (int i = 0; i < input.length; i++) {
      for (int j = 0; j < output.length; j++) {
        if (input[i].getColumn().equals(output[j].getColumn())) {
          outputMapping[i] = j;
          break;
        }
      }
    }
  }

  public DataField[] getInput(CarbonDataLoadConfiguration configuration) {
    DataField[] fields = configuration.getDataFields();
    String[] header = configuration.getHeader();
    numberOfColumns = header.length;
    DataField[] input = new DataField[fields.length];
    inputMapping = new int[input.length];
    int k = 0;
    for (int i = 0; i < fields.length; i++) {
      for (int j = 0; j < numberOfColumns; j++) {
        if (header[j].equalsIgnoreCase(fields[i].getColumn().getColName())) {
          input[k] = fields[i];
          inputMapping[k] = j;
          k++;
          break;
        }
      }
    }
    return input;
  }

  @Override
  public Object[] parseRow(Object[] row) {
    // If number of columns are less in a row then create new array with same size of header.
    if (row.length < numberOfColumns) {
      String[] temp = new String[numberOfColumns];
      System.arraycopy(row, 0, temp, 0, row.length);
      row = temp;
    }
    Object[] out = new Object[genericParsers.length];
    for (int i = 0; i < genericParsers.length; i++) {
      Object obj = row[inputMapping[i]];
      out[outputMapping[i]] = genericParsers[i].parse(obj);
    }
    return out;
  }

}
