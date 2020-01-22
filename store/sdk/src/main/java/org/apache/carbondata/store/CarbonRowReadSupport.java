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

package org.apache.carbondata.store;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

/**
 * ReadSupport that convert row object to CarbonRow
 */
@InterfaceAudience.Internal
public class CarbonRowReadSupport implements CarbonReadSupport<CarbonRow> {
  private CarbonReadSupport<Object[]> delegate;

  public CarbonRowReadSupport() {
    this.delegate = new CarbonReadSupport<Object[]>() {
      @Override
      public Object[] readRow(Object[] data) {
        return data;
      }
    };
  }

  @Override
  public void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable)
      throws IOException {
    delegate.initialize(carbonColumns, carbonTable);
  }

  @Override
  public CarbonRow readRow(Object[] data) {
    Object[] converted = delegate.readRow(data);
    return new CarbonRow(converted);
  }

  @Override
  public void close() {
    delegate.close();
  }
}