/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.filter.resolver.resolverinfo.visitable;

import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.visitor.ResolvedFilterInfoVisitorIntf;

public interface ResolvedFilterInfoVisitable {
  /**
   * This visitable method will visit through the visitor classes which is passed as parameter
   * and based on different visitor the visitable filter instance will be resolved.
   *
   * @param visitor
   * @param metadata
   * @throws QueryExecutionException
   * @throws FilterUnsupportedException
   */
  void populateFilterInfoBasedOnColumnType(ResolvedFilterInfoVisitorIntf visitor,
      FilterResolverMetadata metadata) throws FilterUnsupportedException;

}
