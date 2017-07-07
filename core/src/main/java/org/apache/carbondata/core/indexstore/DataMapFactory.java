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

import org.apache.carbondata.core.events.ChangeEvent;
import org.apache.carbondata.core.indexstore.schema.FilterType;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * Interface for datamap factory, it is responsible for creating the datamap.
 */
public interface DataMapFactory {

  /**
   * Initialization of Datamap factory
   * @param identifier
   * @param dataMapName
   */
  void init(AbsoluteTableIdentifier identifier, String dataMapName);
  /**
   * Get the datamap writer for each segmentid.
   *
   * @param identifier
   * @param segmentId
   * @return
   */
  DataMapWriter getDataMapWriter(AbsoluteTableIdentifier identifier,
      String segmentId);

  /**
   * Get the datamap for segmentid
   *
   * @param segmentId
   * @return
   */
  List<DataMap> getDataMaps(String segmentId);

  /**
   * Get datamap for distributable object.
   *
   * @param distributable
   * @return
   */
  DataMap getDataMap(DataMapDistributable distributable);

  /**
   * This method checks whether the columns and the type of filters supported
   * for this datamap or not
   *
   * @param filterType
   * @return
   */
  boolean isFiltersSupported(FilterType filterType);

  /**
   *
   * @param event
   */
  void fireEvent(ChangeEvent event);

  /**
   * Clears datamap of the segment
   */
  void clear(String segmentId);

  /**
   * Clear all datamaps from memory
   */
  void clear();

  /**
   * Manages the the data of one datamap
   */
  interface DataMap {

    /**
     * Give the writer to write the data.
     *
     * @return
     */
    DataMapWriter getWriter();

    /**
     * It is called to load the data map to memory or to initialize it.
     */
    void init(String path);

    /**
     * Prune the datamap with filter expression. It returns the list of
     * blocklets where these filters can exist.
     *
     * @param filterExp
     * @return
     */
    List<Blocklet> prune(FilterResolverIntf filterExp);

    /**
     * Convert datamap to distributable object
     * @return
     */
    DataMapDistributable toDistributable();

    /**
     * Clear complete index table and release memory.
     */
    void clear();

  }

}
