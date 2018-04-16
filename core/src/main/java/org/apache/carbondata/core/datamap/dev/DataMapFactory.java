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

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.events.Event;

/**
 * Interface for datamap factory, it is responsible for creating the datamap.
 */
public interface DataMapFactory<T extends DataMap> {

  /**
   * Initialization of Datamap factory with the carbonTable and datamap name
   */
  void init(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws IOException, MalformedDataMapCommandException;

  /**
   * Return a new write for this datamap
   */
  DataMapWriter createWriter(Segment segment, String writeDirectoryPath);

  /**
   * Get the datamap for segmentid
   */
  List<T> getDataMaps(Segment segment, ReadCommittedScope readCommittedScope) throws IOException;

  /**
   * Get datamaps for distributable object.
   */
  List<T> getDataMaps(DataMapDistributable distributable, ReadCommittedScope readCommittedScope)
      throws IOException;

  /**
   * Get all distributable objects of a segmentid
   * @return
   */
  List<DataMapDistributable> toDistributable(Segment segment);

  /**
   *
   * @param event
   */
  void fireEvent(Event event);

  /**
   * Clears datamap of the segment
   */
  void clear(Segment segment);

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
  DataMapLevel getDataMapType();
}
