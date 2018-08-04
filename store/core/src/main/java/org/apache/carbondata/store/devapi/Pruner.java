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

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;

@InterfaceAudience.Developer("Integration")
@InterfaceStability.Unstable
public interface Pruner {

  /**
   * Return an array of ScanUnit which will be the input in
   * {@link Scanner#scan(ScanUnit)}
   *
   * Implementation will leverage index to prune using specified
   * filter expression
   *
   * @param table table identifier
   * @param filterExpression expression of filter predicate given by user
   * @return list of ScanUnit which should be passed to
   *         {@link Scanner#scan(ScanUnit)}
   * @throws CarbonException if any error occurs
   */
  List<ScanUnit> prune(TableIdentifier table, Expression filterExpression) throws CarbonException;
}
