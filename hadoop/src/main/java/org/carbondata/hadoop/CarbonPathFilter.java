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
package org.carbondata.hadoop;

import org.carbondata.core.carbon.path.CarbonTablePath;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Filters to accept valid carbon data Files only
 */
public class CarbonPathFilter implements PathFilter {

  // update extension which should be picked
  private final String validUpdateTimestamp;

  /**
   * @param validUpdateTimestamp update extension which should be picked
   */
  public CarbonPathFilter(String validUpdateTimestamp) {
    this.validUpdateTimestamp = validUpdateTimestamp;
  }

  @Override public boolean accept(Path path) {
    return CarbonTablePath.isCarbonDataFile(path.getName());
  }
}
