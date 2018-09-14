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
package org.apache.carbondata.core.scan.filter.executer;

import java.util.BitSet;

/**
 * Implementation of this interface will involve block
 * and blocklet pruning based on block/blocklet id where
 * the filter values are present.
 */
public interface ImplicitColumnFilterExecutor {

  /**
   * This method will validate the block or blocklet id with the implicit
   * column filter value list and decide whether the required block or
   * blocklet has to be scanned for the data or not
   *
   * @param uniqueBlockPath
   * @return
   */
  BitSet isFilterValuesPresentInBlockOrBlocklet(byte[][] maxValue, byte[][] minValue,
      String uniqueBlockPath, boolean[] isMinMaxSet);

  /**
   * This method will validate the abstract index
   * and decide whether the index is valid for scanning or not.
   * Implicit index is always considered valid as it can be decided at block level.
   *
   * @return
   */
  Boolean isFilterValuesPresentInAbstractIndex(byte[][] maxValue, byte[][] minValue,
      boolean[] isMinMaxSet);
}
