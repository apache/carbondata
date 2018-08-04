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

import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

@InterfaceAudience.User
@InterfaceStability.Unstable
public class ScanOption {

  /** batch size in number of rows in one {@link ResultBatch} */
  public static final String BATCH_SIZE = "batchSize";

  /**
   * set to true if return in row major object in {@link ResultBatch},
   * otherwise columnar object is returned
   */
  public static final String ROW_MAJOR = "rowMajor";

  /**
   * set to true if enable remote prune by RPC call,
   * otherwise prune executes in caller's JVM
   */
  public static final String REMOTE_PRUNE = "remotePrune";

  /**
   * set to true if enable operator pushdown like scan and load
   * otherwise operation executes in caller's JVM
   */
  public static final String OP_PUSHDOWN = "operatorPushDown";

  /**
   * Return true if REMOTE_PRUNE is set, default is false
   */
  public static boolean isRemotePrune(Map<String, String> options) {
    if (options == null) {
      return false;
    }
    return Boolean.valueOf(options.getOrDefault(REMOTE_PRUNE, "false"));
  }

  /**
   * Return true if REMOTE_PRUNE is set, default is false
   */
  public static boolean isOperatorPushdown(Map<String, String> options) {
    if (options == null) {
      return false;
    }
    return Boolean.valueOf(options.getOrDefault(OP_PUSHDOWN, "false"));
  }
}