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

package org.apache.carbondata.core.datastore.page.encoding;

/**
 * Factory to create Encoding Strategy.
 * Now only a default strategy is supported which will choose encoding based on data type
 * and column data stats.
 */
public class EncodingStrategyFactory {

  private static EncodingFactory defaultStrategy = new DefaultEncodingFactory();

  public static EncodingFactory getStrategy() {
    // TODO: make it configurable after added new strategy
    return defaultStrategy;
  }
}
