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

package org.apache.carbondata.sdk.store;

import java.util.Iterator;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.sdk.store.exception.CarbonException;

/**
 * A Mutator is used to perform insert, update, delete operation on the table
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface Mutator extends TransactionalOperation {

  /**
   * Insert a batch of rows if key is not exist, otherwise update the row
   * @param row rows to be upsert
   * @param schema schema of the input row (fields without the primary key)
   * @throws CarbonException if any error occurs
   */
  void upsert(Iterator<KeyedRow> row, StructType schema) throws CarbonException;

  /**
   * Delete a batch of rows
   * @param keys keys to be deleted
   * @throws CarbonException if any error occurs
   */
  void delete(Iterator<PrimaryKey> keys) throws CarbonException;
}
