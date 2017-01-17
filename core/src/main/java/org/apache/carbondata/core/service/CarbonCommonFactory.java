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
package org.apache.carbondata.core.service;

import org.apache.carbondata.core.service.impl.ColumnUniqueIdGenerator;
import org.apache.carbondata.core.service.impl.DictionaryFactory;
import org.apache.carbondata.core.service.impl.PathFactory;

/**
 * Interface to get services
 */
public class CarbonCommonFactory {

  /**
   * @return dictionary service
   */
  public static DictionaryService getDictionaryService() {
    return DictionaryFactory.getInstance();
  }

  /**
   * @return path service
   */
  public static PathService getPathService() {
    return PathFactory.getInstance();
  }

  /**
   * @return unique id generator
   */
  public static ColumnUniqueIdService getColumnUniqueIdGenerator() {
    return ColumnUniqueIdGenerator.getInstance();
  }

}
