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
package org.apache.carbondata.core.localdictionary;

import java.io.IOException;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressCodec;
import org.apache.carbondata.core.datastore.page.statistics.DummyStatsCollector;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.format.LocalDictionaryChunk;

/**
 * Class to maintain page level dictionary. It will store all unique dictionary values
 * used in a page. This is required while writing blocklet level dictionary in carbondata
 * file
 */
public class PageLevelDictionary {

  /**
   * dictionary generator to generate dictionary values for page data
   */
  private LocalDictionaryGenerator localDictionaryGenerator;

  /**
   * set of dictionary surrogate key in this page
   */
  private BitSet usedDictionaryValues;

  private String columnName;

  private DataType dataType;

  public PageLevelDictionary(LocalDictionaryGenerator localDictionaryGenerator, String columnName,
      DataType dataType) {
    this.localDictionaryGenerator = localDictionaryGenerator;
    this.usedDictionaryValues = new BitSet();
    this.columnName = columnName;
    this.dataType = dataType;
  }

  /**
   * Below method will be used to get the dictionary value
   *
   * @param data column data
   * @return dictionary value
   * @throws DictionaryThresholdReachedException when threshold crossed for column
   */
  public int getDictionaryValue(byte[] data) throws DictionaryThresholdReachedException {
    int dictionaryValue = localDictionaryGenerator.generateDictionary(data);
    this.usedDictionaryValues.set(dictionaryValue);
    return dictionaryValue;
  }

  /**
   * Method to merge the dictionary value across pages
   *
   * @param pageLevelDictionary other page level dictionary
   */
  public void mergerDictionaryValues(PageLevelDictionary pageLevelDictionary) {
    usedDictionaryValues.and(pageLevelDictionary.usedDictionaryValues);
  }

  /**
   * Below method will be used to get the local dictionary chunk for writing
   * @TODO Support for numeric data type dictionary exclude columns
   * @return encoded local dictionary chunk
   * @throws MemoryException
   * in case of problem in encoding
   * @throws IOException
   * in case of problem in encoding
   */
  public LocalDictionaryChunk getLocalDictionaryChunkForBlocklet()
      throws MemoryException, IOException {
    // TODO support for actual data type dictionary ColumnSPEC
    ColumnType columnType = ColumnType.PLAIN_VALUE;
    if (DataTypes.VARCHAR == dataType) {
      columnType = ColumnType.PLAIN_LONG_VALUE;
    }
    TableSpec.ColumnSpec spec =
        TableSpec.ColumnSpec.newInstance(columnName, DataTypes.BYTE_ARRAY, columnType);
    ColumnPage dictionaryColumnPage =
        ColumnPage.newPage(spec, DataTypes.BYTE_ARRAY, usedDictionaryValues.cardinality());
    // TODO support data type specific stats collector for numeric data types
    dictionaryColumnPage.setStatsCollector(new DummyStatsCollector());
    int rowId = 0;
    for (int i = usedDictionaryValues.nextSetBit(0);
         i >= 0; i = usedDictionaryValues.nextSetBit(i + 1)) {
      dictionaryColumnPage
          .putData(rowId++, localDictionaryGenerator.getDictionaryKeyBasedOnValue(i));
    }
    // creating a encoder
    ColumnPageEncoder encoder = new DirectCompressCodec(DataTypes.BYTE_ARRAY).createEncoder(null);
    // get encoded dictionary values
    LocalDictionaryChunk localDictionaryChunk = encoder.encodeDictionary(dictionaryColumnPage);
    // set compressed dictionary values
    localDictionaryChunk.setDictionary_values(CompressorFactory.getInstance().getCompressor()
        .compressByte(usedDictionaryValues.toByteArray()));
    // free the dictionary page memory
    dictionaryColumnPage.freeMemory();
    return localDictionaryChunk;
  }
}
