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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.Pruner;
import org.apache.carbondata.store.devapi.ScanUnit;

public class LocalPruner implements Pruner {

  private StoreConf storeConf;

  LocalPruner(StoreConf storeConf) {
    this.storeConf = storeConf;
  }

  @Override
  public List<ScanUnit> prune(TableIdentifier identifier, Expression filterExpression)
      throws CarbonException {
    try {
      TableInfo table = MetaOperation.getTable(identifier, storeConf);
      List<CarbonInputSplit> splits = IndexOperation.pruneBlock(table, filterExpression);
      return splits.stream().map(
          (Function<CarbonInputSplit, ScanUnit>) inputSplit ->
              // LocalScanner will scan in local JVM, it is not sending RPC to
              // schedulable (Worker), so it can be null
              new BlockScanUnit(inputSplit, null)
      ).collect(Collectors.toList());
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }
}
