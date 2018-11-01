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

package org.apache.carbondata.core.util.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Carbon property that can be dynamic configure
 * it can be used set command to configure in beeline
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CarbonProperty {
  /**
   * default value is false, it means this property isn't dynamic configurable.
   * if set the value as true, it means this property can be dynamic configurable,
   * but still need support it when validate key and value
   *
   * @return
   */
  boolean dynamicConfigurable() default false;
}
