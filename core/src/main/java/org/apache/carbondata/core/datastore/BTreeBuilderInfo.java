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
package org.apache.carbondata.core.datastore;

import java.util.List;

import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;

/**
 * below class holds the meta data requires to build the blocks
 */
public class BTreeBuilderInfo {

  /**
   * holds all the information about data
   * file meta data
   */
  private List<DataFileFooter> footerList;

  /**
   * size of the each column value size
   * this will be useful for reading
   */
  private int[] dimensionColumnValueSize;

  public BTreeBuilderInfo(List<DataFileFooter> footerList,
      int[] dimensionColumnValueSize) {
    this.dimensionColumnValueSize = dimensionColumnValueSize;
    this.footerList = footerList;
  }

  public int[] getDimensionColumnValueSize() {
    return dimensionColumnValueSize;
  }

  public List<DataFileFooter> getFooterList() {
    return footerList;
  }
}
