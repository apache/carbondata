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

import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * Datamap is an entity which can store and retrieve index data.
 */
public interface DataMap {

  /**
   * It is called to load the data map to memory or to initialize it.
   */
  void init(DataMapModel dataMapModel) throws MemoryException, IOException;

  /**
   * Prune the datamap with filter expression. It returns the list of
   * blocklets where these filters can exist.
   *
   * @param filterExp
   * @return
   */
  List<Blocklet> prune(FilterResolverIntf filterExp);

  // TODO Move this method to Abstract class
  /**
   * Prune the datamap with filter expression and partition information. It returns the list of
   * blocklets where these filters can exist.
   *
   * @param filterExp
   * @return
   */
  List<Blocklet> prune(FilterResolverIntf filterExp, List<String> partitions);

  // TODO Move this method to Abstract class
  /**
   * Validate whether the current segment needs to be fetching the required data
   *
   * @param filterExp
   * @return
   */
  boolean isScanRequired(FilterResolverIntf filterExp);

  /**
   * Clear complete index table and release memory.
   */
  void clear();

}
