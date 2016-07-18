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

package org.carbondata.common.logging;

/**
 * for Log Services
 */
public interface LogService {

  void debug(String message);

  void info(String message);

  void warn(String message);

  void error(String message);

  void error(Throwable throwable);

  void error(Throwable throwable, String message);

  void audit(String message);

  /**
   * Below method will be used to log the statistic information
   *
   * @param message statistic message
   */
  void statistic(String message);
}
