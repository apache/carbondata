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

package org.apache.carbondata.core.index;

import org.apache.carbondata.core.metadata.schema.table.IndexSchema;

/**
 * This is the interface for inmemory catalog registry for MV.
 * @since 1.4.0
 */
public interface MVCatalog<T> {

  /**
   * Register schema to the catalog.
   * @param indexSchema
   */
  void registerSchema(IndexSchema indexSchema);

  /**
   * Unregister schema from catalog.
   * @param dataMapName
   */
  void unregisterSchema(String dataMapName);

  /**
   * List all registered valid schema catalogs
   * @return
   */
  T[] listAllValidSchema();

  /**
   * It reloads/removes all registered schema catalogs
   */
  void refresh();

  /**
   * This checks whether the datamapSchema is already registered
   */
  Boolean isMVExists(String mvName);

}
