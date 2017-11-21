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
package org.apache.carbondata.core.datamap.dev;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.DataMapType;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.events.Event;

/**
 * Interface for datamap factory, it is responsible for creating the datamap.
 */
public interface DataMapFactory<T extends DataMap> {

  /**
   * Initialization of Datamap factory with the identifier and datamap name
   */
  void init(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema);

  /**
   * Return a new write for this datamap
   */
  AbstractDataMapWriter createWriter(String segmentId, String writeDirectoryPath);

  /**
   * Get the datamap for segmentid
   */
  List<T> getDataMaps(String segmentId) throws IOException;

  /**
   * Get datamaps for distributable object.
   */
  List<T> getDataMaps(DataMapDistributable distributable) throws IOException;

  /**
   * Get all distributable objects of a segmentid
   * @return
   */
  List<DataMapDistributable> toDistributable(String segmentId);

  /**
   *
   * @param event
   */
  void fireEvent(Event event);

  /**
   * Clears datamap of the segment
   */
  void clear(String segmentId);

  /**
   * Clear all datamaps from memory
   */
  void clear();

  /**
   * Return metadata of this datamap
   */
  DataMapMeta getMeta();

  /**
   *  Type of datamap whether it is FG or CG
   */
  DataMapType getDataMapType();
}
