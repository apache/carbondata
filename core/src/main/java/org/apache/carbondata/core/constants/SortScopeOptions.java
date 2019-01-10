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

package org.apache.carbondata.core.constants;

/**
 * Sort scope options
 */
public class SortScopeOptions {

  public static SortScope getSortScope(String sortScope) {
    if (sortScope == null) {
      sortScope = CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT;
    }
    switch (sortScope.toUpperCase()) {
      case "BATCH_SORT":
        return SortScope.BATCH_SORT;
      case "LOCAL_SORT":
        return SortScope.LOCAL_SORT;
      case "GLOBAL_SORT":
        return SortScope.GLOBAL_SORT;
      case "NO_SORT":
        return SortScope.NO_SORT;
      default:
        return getSortScope(CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT);
    }
  }

  public enum SortScope {
    NO_SORT, BATCH_SORT, LOCAL_SORT, GLOBAL_SORT
  }
}

