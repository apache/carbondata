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

package org.carbondata.query.carbon.result;

import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QuerySchemaInfo;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * Below class holds the query result of batches.
 */
public class BatchRawResult extends CarbonIterator<Object[]> {

  /**
   * list of keys
   */
  private Object[][] rows;

  private QuerySchemaInfo querySchemaInfo;
  /**
   * counter to check whether all the records are processed or not
   */
  private int counter;

  /**
   * size of the batches.
   */
  private int size;

  public BatchRawResult(Object[][] rows) {
    this.rows = rows;
    if (rows.length > 0) {
      this.size = rows[0].length;
    }
  }

  /**
   * This will return all the raw records.
   * @return
   */
  public Object[][] getAllRows() {
    return rows;
  }

  /**
   * This method will return one row at a time based on the counter given.
   * @param counter
   * @return
   */
  public Object[] getRawRow(int counter) {
    Object[] outputRow = new Object[rows.length];
    for(int col = 0 ; col < rows.length ; col++) {
      outputRow[col] = rows[col][counter];
    }
    return outputRow;
  }

  /**
   * Returns {@code true} if the iteration has more elements.
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    return counter < size;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public Object[] next() {
    return parseData();
  }

  private Object[] parseData() {
    ByteArrayWrapper key = (ByteArrayWrapper) rows[0][counter];
    int[] order = querySchemaInfo.getQueryReverseOrder();
    long[] surrogateResult = querySchemaInfo.getKeyGenerator()
        .getKeyArray(key.getDictionaryKey(), querySchemaInfo.getMaskedByteIndexes());
    QueryDimension[] queryDimensions = querySchemaInfo.getQueryDimensions();
    Object[] parsedData = new Object[queryDimensions.length + rows.length - 1];
    int noDictionaryColumnIndex = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      if (!queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
        parsedData[order[i]] = DataTypeUtil.getDataBasedOnDataType(
            new String(key.getNoDictionaryKeyByIndex(noDictionaryColumnIndex++)),
            queryDimensions[i].getDimension().getDataType());
      } else if (queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(queryDimensions[i].getDimension().getDataType());
        parsedData[order[i]] = directDictionaryGenerator.getValueFromSurrogate(
            (int) surrogateResult[queryDimensions[i].getDimension().getKeyOrdinal()]);
      } else {
        parsedData[order[i]] =
            (int) surrogateResult[queryDimensions[i].getDimension().getKeyOrdinal()];
      }
    }
    for (int i = 0; i < rows.length - 1; i++) {
      parsedData[order[i + queryDimensions.length]] = rows[i + 1][counter];
    }
    counter++;
    return parsedData;
  }

  public static Object[] parseData(ByteArrayWrapper key, Object[] aggData,
      QuerySchemaInfo querySchemaInfo, int[] aggOrder) {
    long[] surrogateResult = querySchemaInfo.getKeyGenerator()
        .getKeyArray(key.getDictionaryKey(), querySchemaInfo.getMaskedByteIndexes());
    QueryDimension[] queryDimensions = querySchemaInfo.getQueryDimensions();
    Object[] parsedData = new Object[queryDimensions.length + aggData.length];
    int noDictionaryColumnIndex = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      if (!CarbonUtil
          .hasEncoding(queryDimensions[i].getDimension().getEncoder(), Encoding.DICTIONARY)) {
        parsedData[i] = DataTypeUtil.getDataBasedOnDataType(
            new String(key.getNoDictionaryKeyByIndex(noDictionaryColumnIndex++)),
            queryDimensions[i].getDimension().getDataType());
      } else if (queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(queryDimensions[i].getDimension().getDataType());
        parsedData[i] = directDictionaryGenerator.getValueFromSurrogate(
            (int) surrogateResult[queryDimensions[i].getDimension().getKeyOrdinal()]);
      } else {
        parsedData[i] = (int) surrogateResult[queryDimensions[i].getDimension().getKeyOrdinal()];
      }
    }
    for (int i = 0; i < aggData.length; i++) {
      parsedData[i + queryDimensions.length] = aggData[i];
    }
    Object[] orderData = new Object[parsedData.length];
    for (int i = 0; i < parsedData.length; i++) {
      orderData[i] = parsedData[aggOrder[i]];
    }
    return orderData;
  }

  public QuerySchemaInfo getQuerySchemaInfo() {
    return querySchemaInfo;
  }

  public void setQuerySchemaInfo(QuerySchemaInfo querySchemaInfo) {
    this.querySchemaInfo = querySchemaInfo;
  }

  /**
   * For getting the total size.
   * @return
   */
  public int getSize() {
    return size;
  }
}
