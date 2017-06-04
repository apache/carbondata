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
package org.apache.carbondata.core.indexstore;

import java.util.List;

import org.apache.carbondata.core.events.EventListener;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * DataMap at the table level, user can add any number of datamaps for one table. Depends
 * on the filter condition it can prune the blocklets.
 */
public interface TableDataMap extends EventListener {

  /**
   * It is called to initialize and load the required table datamap metadata.
   */
  void init(AbsoluteTableIdentifier identifier, String dataMapName);

  /**
   * Gives the writer to write the metadata information of this datamap at table level.
   *
   * @return
   */
  DataMapWriter getWriter();

  /**
   * Create the datamap using the segmentid  and name.
   *
   * @param identifier
   * @param segmentId
   * @return
   */
  DataMap createDataMap(AbsoluteTableIdentifier identifier, String segmentId);

  /**
   * Pass the valid segments and prune the datamap using filter expression
   *
   * @param segmentIds
   * @param filterExp
   * @return
   */
  List<Blocklet> prune(List<String> segmentIds, FilterResolverIntf filterExp);

  /**
   * This is used for making the datamap distributable.
   * It takes the valid segments and returns all the datamaps as distributable objects so that
   * it can be distributed across machines.
   *
   * @return
   */
  List<DataMapDistributable> toDistributable(List<String> segmentIds);

  /**
   * This method is used from any machine after it is distributed. It takes the distributable object
   * to prune the filters.
   *
   * @param distributable
   * @param filterExp
   * @return
   */
  List<Blocklet> prune(DataMapDistributable distributable, FilterResolverIntf filterExp);

  /**
   * This method checks whether the columns and the type of filters supported
   * for this datamap or not
   *
   * @param filterExp
   * @return
   */
  boolean isFiltersSupported(FilterResolverIntf filterExp);

  /**
   * Clears table level datamap
   */
  void clear();

}
