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
package org.apache.carbondata.processing.newflow.steps;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

/**
 * if the table doesn't have sort_columns, just convert row format.
 */
public class NoSortProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private int dimensionCount;

  private int dimensionWithComplexCount;

  private int noDictCount;

  private int measureCount;

  private boolean[] isNoDictionaryDimensionColumn;

  private char[] aggType;

  public NoSortProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
    this.dimensionWithComplexCount = configuration.getDimensionCount();
    this.noDictCount =
        configuration.getNoDictionaryCount() + configuration.getComplexDimensionCount();
    this.dimensionCount = configuration.getDimensionCount() - this.noDictCount;
    this.measureCount = configuration.getMeasureCount();
    this.isNoDictionaryDimensionColumn =
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
    this.aggType = CarbonDataProcessorUtil
        .getAggType(configuration.getMeasureCount(), configuration.getMeasureFields());
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void initialize() throws IOException {
    child.initialize();
  }

  /**
   * convert input CarbonRow to output CarbonRow
   * e.g. There is a table as following,
   * the number of dictionary dimensions is a,
   * the number of no-dictionary dimensions is b,
   * the number of complex dimensions is c,
   * the number of measures is d.
   * input CarbonRow format:  the length of Object[] data is a+b+c+d, the number of all columns.
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Part                     | Object item                    | describe                 |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Object[0 ~ a+b-1]        | Integer, byte[], Integer, ...  | dict + no dict dimensions|
   * ----------------------------------------------------------------------------------------
   * | Object[a+b ~ a+b+c-1]    | byte[], byte[], ...            | complex dimensions       |
   * ----------------------------------------------------------------------------------------
   * | Object[a+b+c ~ a+b+c+d-1]| int, byte[], ...               | measures                 |
   * ----------------------------------------------------------------------------------------
   * output CarbonRow format: the length of object[] data is 3.
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Part                     | Object item                    | describe                 |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Object[0]                | int[a]                         | dict dimension array     |
   * ----------------------------------------------------------------------------------------
   * | Object[1]                | byte[b+c][]                    | no dict + complex dim    |
   * ----------------------------------------------------------------------------------------
   * | Object[2]                | Object[d]                      | measures                 |
   * ----------------------------------------------------------------------------------------
   * @param row
   * @return
   */
  @Override protected CarbonRow processRow(CarbonRow row) {
    int dictIndex = 0;
    int nonDicIndex = 0;
    int[] dim = new int[this.dimensionCount];
    byte[][] nonDicArray = new byte[this.noDictCount][];
    Object[] measures = new Object[this.measureCount];
    // read dimension values
    int dimCount = 0;
    for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
      if (isNoDictionaryDimensionColumn[dimCount]) {
        nonDicArray[nonDicIndex++] = (byte[]) row.getObject(dimCount);
      } else {
        dim[dictIndex++] = (int) row.getObject(dimCount);
      }
    }

    for (; dimCount < this.dimensionWithComplexCount; dimCount++) {
      nonDicArray[nonDicIndex++] = (byte[]) row.getObject(dimCount);
    }

    // measure values
    for (int mesCount = 0; mesCount < this.measureCount; mesCount++) {
      Object value = row.getObject(mesCount + this.dimensionWithComplexCount);
      if (null != value) {
        if (aggType[mesCount] == CarbonCommonConstants.DOUBLE_MEASURE) {
          measures[mesCount] = value;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
          measures[mesCount] = value;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          BigDecimal val = (BigDecimal) value;
          measures[mesCount] = DataTypeUtil.bigDecimalToByte(val);
        }
      } else {
        measures[mesCount] = null;
      }
    }
    // create new row of size 3 (1 for dims , 1 for high card , 1 for measures)
    Object[] holder = new Object[3];
    NonDictionaryUtil.prepareOutObj(holder, dim, nonDicArray, measures);
    //return out row
    return new CarbonRow(holder);
  }

  @Override
  public void close() {
    if (!closed) {
      super.close();
    }
  }

  @Override protected String getStepName() {
    return "No Sort Processor";
  }
}
