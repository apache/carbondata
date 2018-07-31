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
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.sdk.store.descriptor.SelectDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;

/**
 * A Scanner is used to scan the table
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface Scanner {

  /**
   * Scan a Table and return matched rows, using default select option
   * see {@link #scan(SelectDescriptor, SelectOption)} for more information
   *
   * @param select descriptor for select operation
   * @return matched rows
   * @throws CarbonException if any error occurs
   */
  List<CarbonRow> scan(SelectDescriptor select) throws CarbonException;

  /**
   * Scan a Table and return matched rows
   * @param select descriptor for select operation, including required column, filter, etc
   * @return matched rows
   * @throws CarbonException if any error occurs
   */
  List<CarbonRow> scan(SelectDescriptor select, SelectOption option) throws CarbonException;

  /**
   * Return an array of ScanUnit which will be the input in
   * {@link #scan(ScanUnit, SelectDescriptor, SelectOption)}
   *
   * Implementation will leverage index to prune using specified filter expression
   *
   * @param table table identifier
   * @param filterExpression expression of filter predicate given by user
   * @return unit of scan
   * @throws CarbonException if any error occurs
   */
  ScanUnit[] prune(TableIdentifier table, Expression filterExpression) throws CarbonException;

  /**
   * Perform a scan in a distributed compute framework like Spark, Presto, etc.
   * Filter/Projection/Limit operation is pushed down to the scan.
   *
   * This should be used with {@link #prune(TableIdentifier, Expression)} in a distributed
   * compute environment. It enables the framework to do a parallel scan by creating
   * multiple {@link ScanUnit} and perform parallel scan in worker, such as Spark executor
   *
   * The return result is in batch so that the caller can start next level of computation
   * before getting all results, such as implementing a `prefetch` execution model.
   *
   * @param input one scan unit
   * @param select parameter for scanning
   * @return scan result, the result is returned in batch
   * @throws CarbonException if any error occurs
   */
  <T> Iterator<ResultBatch<T>> scan(ScanUnit input, SelectDescriptor select,
      SelectOption option) throws CarbonException;

}
