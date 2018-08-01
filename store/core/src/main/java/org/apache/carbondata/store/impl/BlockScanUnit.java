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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.sdk.store.ScanUnit;

public class BlockScanUnit implements ScanUnit {
  private CarbonInputSplit inputSplit;

  public BlockScanUnit() {
  }

  public BlockScanUnit(CarbonInputSplit inputSplit) {
    this.inputSplit = inputSplit;
  }

  public CarbonInputSplit getInputSplit() {
    return inputSplit;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    inputSplit.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    inputSplit = new CarbonInputSplit();
    inputSplit.readFields(in);
  }
}
