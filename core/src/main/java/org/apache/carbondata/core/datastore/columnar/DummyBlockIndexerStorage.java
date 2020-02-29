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

package org.apache.carbondata.core.datastore.columnar;

/**
 * This is for all dimension except DATE
 * TODO: refactor to use 'encode by meta' without going through BlockIndexerStorage
 */
public class DummyBlockIndexerStorage extends BlockIndexerStorage<byte[][]> {

  @Override
  public short[] getRowIdPage() {
    return new short[0];
  }

  @Override
  public int getRowIdPageLengthInBytes() {
    return 0;
  }

  @Override
  public short[] getRowIdRlePage() {
    return new short[0];
  }

  @Override
  public int getRowIdRlePageLengthInBytes() {
    return 0;
  }

  @Override
  public byte[][] getDataPage() {
    return new byte[0][];
  }

  @Override
  public short[] getDataRlePage() {
    return new short[0];
  }

  @Override
  public int getDataRlePageLengthInBytes() {
    return 0;
  }
}
