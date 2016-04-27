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

package org.carbondata.query.columnar.scanner.impl;

import org.carbondata.query.columnar.aggregator.impl.DataAggregator;
import org.carbondata.query.columnar.aggregator.impl.ListBasedResultAggregatorImpl;
import org.carbondata.query.columnar.scanner.AbstractColumnarStorageScanner;
import org.carbondata.query.schema.metadata.ColumnarStorageScannerInfo;

public class ColumnarStorageScannerImpl extends AbstractColumnarStorageScanner {
  private long counter;
  private int[] noDictionaryColIndexes;
  private int limit;

  public ColumnarStorageScannerImpl(ColumnarStorageScannerInfo columnarStorageScannerInfo) {
    super(columnarStorageScannerInfo);

    this.columnarAggaregator =
        new ListBasedResultAggregatorImpl(columnarStorageScannerInfo.getColumnarAggregatorInfo(),
            new DataAggregator(columnarStorageScannerInfo.isAutoAggregateTableRequest(),
                columnarStorageScannerInfo.getColumnarAggregatorInfo()));
    limit = columnarStorageScannerInfo.getColumnarAggregatorInfo().getLimit();
  }

  public ColumnarStorageScannerImpl(ColumnarStorageScannerInfo columnarStorageScannerInfo,
      int[] noDictionaryColIndexes) {
    this(columnarStorageScannerInfo);
    this.noDictionaryColIndexes = noDictionaryColIndexes;
  }

  @Override public void scanStore() {
    while (leafIterator.hasNext()) {
      blockDataHolder.setLeafDataBlock(leafIterator.next());
      blockDataHolder.reset();
      counter += this.columnarAggaregator
          .aggregateData(blockProcessor.getScannedData(blockDataHolder, noDictionaryColIndexes));
      finish();
      if (limit != -1 && counter >= limit) {
        break;
      }
    }
  }
}
