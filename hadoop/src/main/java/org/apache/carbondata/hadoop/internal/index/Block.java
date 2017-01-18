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

package org.apache.carbondata.hadoop.internal.index;

import java.util.List;

/**
 * Represent one HDFS Block (one CarbonData file).
 */
public interface Block {

  /**
   * @return the path of this block
   */
  String getBlockPath();

  /**
   * return all matched blocklets for scanning
   * @return list of blocklet offset in the block
   */
  List<Long> getMatchedBlocklets();

  /**
   * @return true if need to do full scan of this block
   */
  boolean fullScan();
}
