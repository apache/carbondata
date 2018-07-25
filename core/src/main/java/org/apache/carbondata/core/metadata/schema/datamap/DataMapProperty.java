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

package org.apache.carbondata.core.metadata.schema.datamap;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * Property that can be specified when creating DataMap
 */
@InterfaceAudience.Internal
public class DataMapProperty {

  /**
   * Used to specify the store location of the datamap
   */
  public static final String PARTITIONING = "partitioning";
  public static final String PATH = "path";

  /**
   * For datamap created with 'WITH DEFERRED REBUILD' syntax, we will add this
   * property internally
   */
  public static final String DEFERRED_REBUILD = "_internal.deferred.rebuild";
  /**
   * for internal property 'CHILD_SELECT_QUERY'
   */
  public static final String CHILD_SELECT_QUERY = "CHILD_SELECT QUERY";
  /**
   * for internal property 'QUERYTYPE'
   */
  public static final String QUERY_TYPE = "QUERYTYPE";
}
