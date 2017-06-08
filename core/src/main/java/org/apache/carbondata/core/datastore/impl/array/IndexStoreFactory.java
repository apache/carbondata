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
package org.apache.carbondata.core.datastore.impl.array;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.BtreeBuilder;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeBuilder;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Factory for index builder and finder
 */
public class IndexStoreFactory {

  public static BtreeBuilder getDriverIndexBuilder() {
    String storeType = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INDEX_DRIVER_STORE_TYPE,
            CarbonCommonConstants.CARBON_INDEX_DRIVER_STORE_TYPE_DEFAULT);
    switch (storeType.toLowerCase()) {
      case "unsafe.array":
        return new BlockArrayIndexBuilder();
      case "btree":
        return new BlockBTreeBuilder();
      default:
        return new BlockBTreeBuilder();
    }

  }

  public static DataRefNodeFinder getNodeFinder(int[] eachColumnValueSize, int numberOfSortColumns,
      int numberOfNoDictSortColumns) {
    String storeType = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INDEX_DRIVER_STORE_TYPE,
            CarbonCommonConstants.CARBON_INDEX_DRIVER_STORE_TYPE_DEFAULT);
    DataRefNodeFinder dataRefNodeFinder;
    switch (storeType.toLowerCase()) {
      case "unsafe.array":
        dataRefNodeFinder = new ArrayDataRefNodeFinder(eachColumnValueSize, numberOfSortColumns,
            numberOfNoDictSortColumns);
        break;
      default:
        dataRefNodeFinder = new BTreeDataRefNodeFinder(eachColumnValueSize, numberOfSortColumns,
            numberOfNoDictSortColumns);
    }
    return dataRefNodeFinder;
  }

}
