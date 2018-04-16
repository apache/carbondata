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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

/**
 *It is used to save/retreive/drop datamap schema from storage medium like disk or DB.
 * Here dataMapName must be unique across whole store.
 *
 * @since 1.4.0
 */
@InterfaceAudience.Internal
public interface DataMapSchemaStorageProvider {

  /**
   * Save the schema to storage medium.
   * @param dataMapSchema
   */
  void saveSchema(DataMapSchema dataMapSchema) throws IOException;

  /**
   * Retrieve the schema by using dataMapName.
   * @param dataMapName
   */
  DataMapSchema retrieveSchema(String dataMapName) throws IOException;

  /**
   * Retrieve schemas by using the list of datamap names
   * @param dataMapNames
   * @return
   * @throws IOException
   */
  List<DataMapSchema> retrieveSchemas(List<String> dataMapNames) throws IOException;

  /**
   * Retrieve all schemas
   * @return
   * @throws IOException
   */
  List<DataMapSchema> retrieveAllSchemas() throws IOException;

  /**
   * Drop the schema from the storage by using dataMapName.
   * @param dataMapName
   */
  void dropSchema(String dataMapName,String tableName) throws IOException;

}
