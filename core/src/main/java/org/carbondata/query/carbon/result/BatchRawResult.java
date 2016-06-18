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
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QuerySchemaInfo;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * Below class holds the query result of batches.
 */
public class BatchRawResult extends BatchResult {

  private QuerySchemaInfo querySchemaInfo;

  /**
   * This method will return one row at a time based on the counter given.
   * @param counter
   * @return
   */
  public Object[] getRawRow(int counter) {
    return rows[counter];
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
    int[] order = querySchemaInfo.getQueryReverseOrder();
    Object[] row = rows[counter];
    ByteArrayWrapper key = (ByteArrayWrapper) row[0];
    QueryDimension[] queryDimensions = querySchemaInfo.getQueryDimensions();
    Object[] parsedData = new Object[queryDimensions.length + row.length - 1];
    if(key != null) {
      long[] surrogateResult = querySchemaInfo.getKeyGenerator()
          .getKeyArray(key.getDictionaryKey(), querySchemaInfo.getMaskedByteIndexes());
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
    }
    for (int i = 0; i < row.length - 1; i++) {
      parsedData[order[i + queryDimensions.length]] = row[i + 1];
    }
    counter++;
    return parsedData;
  }

  public void setQuerySchemaInfo(QuerySchemaInfo querySchemaInfo) {
    this.querySchemaInfo = querySchemaInfo;
  }

  /**
   * For getting the total size.
   * @return
   */
  public int getSize() {
    return rows.length;
  }
}
