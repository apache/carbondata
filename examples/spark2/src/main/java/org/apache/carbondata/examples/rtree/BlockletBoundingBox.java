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

package org.apache.carbondata.examples.rtree;

public class BlockletBoundingBox {
  String directoryPath;
  Integer blockletId;
  double lowx, lowy, highx, highy;

  public BlockletBoundingBox(double[] low, double[] high, int blockletId, String directoryPath) {
    assert (low.length == 2 && low.length == high.length);
    this.directoryPath = directoryPath;
    this.blockletId = blockletId;
    this.lowx = low[0];
    this.lowy = low[1];
    this.highx = high[0];
    this.highy = high[1];
  }

  public Integer getBlockletId() {
    return blockletId;
  }
}
