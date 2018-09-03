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

package org.apache.carbondata.store.devapi;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.sdk.store.exception.CarbonException;

@InterfaceAudience.Developer("Integration")
@InterfaceStability.Unstable
public interface DataScanner<T> extends Serializable {

  /**
   * Perform a scan in a distributed compute framework like Spark, Presto, etc.
   * Filter/Projection/Limit operation is pushed down to the scan.
   *
   * This should be used with {@link Pruner#prune(TableIdentifier, Expression)}
   * in a distributed compute environment. It enables the framework to
   * do a parallel scan by creating multiple {@link ScanUnit} and perform
   * parallel scan in worker, such as Spark executor
   *
   * The return result is in batch so that the caller can start next
   * level of computation before getting all results, such as
   * implementing a `prefetch` execution model.
   *
   * @param input one scan unit
   * @return scan result, the result is returned in batch
   * @throws CarbonException if any error occurs
   */
  Iterator<? extends ResultBatch<T>> scan(ScanUnit input) throws CarbonException;
}
