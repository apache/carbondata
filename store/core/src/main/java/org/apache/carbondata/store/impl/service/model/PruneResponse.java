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

package org.apache.carbondata.store.impl.service.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.impl.BlockScanUnit;

import org.apache.hadoop.io.Writable;

@InterfaceAudience.Internal
public class PruneResponse implements Serializable, Writable {

  private List<ScanUnit> scanUnits;

  public PruneResponse() {
  }

  public PruneResponse(List<ScanUnit> scanUnits) {
    this.scanUnits = scanUnits;
  }

  public List<ScanUnit> getScanUnits() {
    return scanUnits;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(scanUnits.size());
    for (ScanUnit scanUnit : scanUnits) {
      scanUnit.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    scanUnits = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      BlockScanUnit scanUnit = new BlockScanUnit();
      scanUnit.readFields(in);
      scanUnits.add(scanUnit);
    }
  }
}
