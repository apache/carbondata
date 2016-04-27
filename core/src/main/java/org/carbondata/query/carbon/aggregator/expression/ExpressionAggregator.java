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
package org.carbondata.query.carbon.aggregator.expression;

import java.nio.ByteBuffer;
import java.util.List;

import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.CustomMeasureAggregator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbonfilterinterface.RowImpl;

/**
 * Below class will be used for aggregate expression for example if there are
 * two in aggregation f(column1)+f(column2).
 */
public class ExpressionAggregator {

  /**
   * aggregator index of the expression
   */
  private int expressionStartIndex;

  /**
   * buffer which will be used to convert the actual dictionary column data to
   * surrogate key which will be used to get the actual value from dictionary
   */
  private ByteBuffer buffer;

  /**
   * block execution info which has all the detail of expression related
   * operations
   */
  private BlockExecutionInfo blockExecutionInfo;

  public ExpressionAggregator(BlockExecutionInfo blockExecutionInfo) {
    buffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
    this.expressionStartIndex = blockExecutionInfo.getExpressionStartIndex();
    this.blockExecutionInfo = blockExecutionInfo;
  }

  /**
   * Below method will be used to aggregate the columns present in expression.
   * Expression can be on any column dimension and measure so for aggregation
   * we need to create a row based on and fill the value for all the column
   * participated in expression and then pass it to aggregator to aggregate
   * the values
   *
   * @param scannedResult     scanned result
   * @param measureAggregator measure aggregate to aggregate the data
   */
  public void aggregateExpression(AbstractScannedResult scannedResult,
      MeasureAggregator[] measureAggregator) {

    RowImpl rowImpl = null;
    for (int i = 0; i < this.blockExecutionInfo.getCustomAggregateExpressions().size(); i++) {
      List<CarbonColumn> referredColumns =
          this.blockExecutionInfo.getCustomAggregateExpressions().get(i).getReferredColumns();
      Object[] row = new Object[referredColumns.size()];
      for (int j = 0; j < referredColumns.size(); j++) {
        CarbonColumn carbonColumn = referredColumns.get(j);
        if (!carbonColumn.isDimesion()) {
          // for measure column first checking whether measure value
          // was null or not
          if (!scannedResult.isNullMeasureValue(carbonColumn.getOrdinal())) {
            // if no null then get the data based on actual data
            // type
            switch (carbonColumn.getDataType()) {
              case LONG:
                row[j] = scannedResult.getLongMeasureValue(carbonColumn.getOrdinal());
                break;
              case DECIMAL:
                row[j] = scannedResult.getBigDecimalMeasureValue(carbonColumn.getOrdinal());
                break;
              default:
                row[j] = scannedResult.getDoubleMeasureValue(carbonColumn.getOrdinal());
            }
          }
        } else if (!CarbonUtil.hasEncoding(carbonColumn.getEncoder(), Encoding.DICTIONARY)) {
          // for dictionary column get the data
          String noDictionaryColumnData =
              new String(scannedResult.getDimensionKey(carbonColumn.getOrdinal()));
          // if data is equal to default value then its null so no
          // need to do any thing
          if (!CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(noDictionaryColumnData)) {
            row[j] = DataTypeUtil
                .getDataBasedOnDataType(noDictionaryColumnData, carbonColumn.getDataType());
          }
        } else {

          // get the surrogate key of the dictionary column from data
          int surrogateKey = CarbonUtil
              .getSurrogateKey(scannedResult.getDimensionKey(carbonColumn.getOrdinal()), buffer);
          // if surrogate key is one then its null value
          // as we are writing the null value surrogate key as 1
          if (surrogateKey != 1) {
            row[j] = DataTypeUtil.getDataBasedOnDataType(
                blockExecutionInfo.getColumnIdToDcitionaryMapping().get(carbonColumn.getColumnId())
                    .getDictionaryValueForKey(surrogateKey), carbonColumn.getDataType());
          }
        }
        CustomMeasureAggregator agg =
            (CustomMeasureAggregator) measureAggregator[expressionStartIndex + i];
        rowImpl = new RowImpl();
        rowImpl.setValues(row);
        agg.agg(rowImpl);
      }
    }
  }
}