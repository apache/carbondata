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

package org.apache.carbondata.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;

/**
 * User can use {@link CarbonStore} to query data
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface CarbonStore extends Closeable {

  /**
   * Scan query on the data in the table path
   * @param path table path
   * @param projectColumns column names to read
   * @return rows
   * @throws IOException if unable to read files in table path
   */
  Iterator<CarbonRow> scan(
      AbsoluteTableIdentifier tableIdentifier,
      String[] projectColumns) throws IOException;

  /**
   * Scan query with filter, on the data in the table path
   * @param path table path
   * @param projectColumns column names to read
   * @param filter filter condition, can be null
   * @return rows that satisfy filter condition
   * @throws IOException if unable to read files in table path
   */
  Iterator<CarbonRow> scan(
      AbsoluteTableIdentifier tableIdentifier,
      String[] projectColumns,
      Expression filter) throws IOException;

  /**
   * SQL query, table should be created before calling this function
   * @param sqlString SQL statement
   * @return rows
   */
  Iterator<CarbonRow> sql(String sqlString);

}
