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

public interface FilterExecuter {

  /**
   * API will apply filter based on resolver instance
   *
   * @return
   * @throws FilterUnsupportedException
   */
  BitSet applyFilter(BlocksChunkHolder blocksChunkHolder) throws FilterUnsupportedException;

  /**
   * API will verify whether the block can be shortlisted based on block
   * max and min key.
   *
   * @param blockMaxValue, maximum value of the
   * @param blockMinValue
   * @return BitSet
   */
  BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue);
}
