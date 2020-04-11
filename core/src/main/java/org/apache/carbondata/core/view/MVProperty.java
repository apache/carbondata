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

package org.apache.carbondata.core.view;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * Property that can be specified when creating MV
 */
@InterfaceAudience.Internal
public class MVProperty {

  /**
   * internal property, choices: FULL, INCREMENTAL
   */
  public static final String REFRESH_MODE = "refresh_mode";

  /**
   * MV refresh model: FULL
   */
  public static final String REFRESH_MODE_FULL = "full";

  /**
   * MV refresh model: INCREMENTAL
   */
  public static final String REFRESH_MODE_INCREMENTAL = "incremental";

  /**
   * internal property, choices: ON_COMMIT, ON_MANUAL
   */
  public static final String REFRESH_TRIGGER_MODE = "refresh_trigger_mode";

  /**
   * MV refresh trigger model: ON_COMMIT
   */
  public static final String REFRESH_TRIGGER_MODE_ON_COMMIT = "on_commit";

  /**
   * MV refresh trigger model: ON_MANUAL
   */
  public static final String REFRESH_TRIGGER_MODE_ON_MANUAL = "on_manual";

}
