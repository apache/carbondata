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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.sdk.store.ResultBatch;
import org.apache.carbondata.sdk.store.ScanUnit;
import org.apache.carbondata.sdk.store.Scanner;
import org.apache.carbondata.sdk.store.SelectOption;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;

public class ScannerImpl implements Scanner {

  private DistributedCarbonStore store;

  ScannerImpl(DistributedCarbonStore store) {
    this.store = store;
  }

  // TODO: implement using RPC
  @Override
  public List<ScanUnit> prune(TableIdentifier table, Expression filterExpression)
      throws CarbonException {
    Objects.requireNonNull(table);
    try {
      List<Distributable> l = CarbonStoreBase.pruneBlock(store.getTable(table), filterExpression);
      return l.stream()
          .map((Function<Distributable, ScanUnit>) BlockScanUnit::new)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  @Override
  public Iterator<ResultBatch<CarbonRow>> scan(ScanUnit input, ScanDescriptor select,
      SelectOption option) throws CarbonException {
    Objects.requireNonNull(input);
    Objects.requireNonNull(select);

    List<Distributable> toBeScan = new ArrayList<>();
    if (input instanceof BlockScanUnit) {
      toBeScan.add(((BlockScanUnit) input).getDistributable());
    } else {
      throw new CarbonException(input.getClass().getName() + " is not supported");
    }
    CarbonTable carbonTable = store.getTable(select.getTableIdentifier());
    try {
      List<CarbonRow> rows = store.doScan(
          carbonTable, select.getProjection(), select.getFilter(), select.getLimit(), toBeScan);
      return rows.stream()
          .map((Function<CarbonRow, ResultBatch<CarbonRow>>) carbonRow ->
              new RowMajorResultBatch(rows))
          .collect(Collectors.toList())
          .iterator();
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }
}
