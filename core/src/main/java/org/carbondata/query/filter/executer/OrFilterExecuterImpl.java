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
package org.carbondata.query.filter.executer;

import java.util.BitSet;

import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.expression.exception.FilterUnsupportedException;

public class OrFilterExecuterImpl implements FilterExecuter {

  private FilterExecuter leftExecuter;
  private FilterExecuter rightExecuter;

  public OrFilterExecuterImpl(FilterExecuter leftExecuter, FilterExecuter rightExecuter) {
    this.leftExecuter = leftExecuter;
    this.rightExecuter = rightExecuter;
  }

  @Override public BitSet applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException {
    BitSet leftFilters = leftExecuter.applyFilter(blockChunkHolder);
    BitSet rightFilters = rightExecuter.applyFilter(blockChunkHolder);
    leftFilters.or(rightFilters);

    return leftFilters;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet leftFilters = leftExecuter.isScanRequired(blockMaxValue, blockMinValue);
    BitSet rightFilters = rightExecuter.isScanRequired(blockMaxValue, blockMinValue);
    leftFilters.or(rightFilters);
    return leftFilters;
  }

}
