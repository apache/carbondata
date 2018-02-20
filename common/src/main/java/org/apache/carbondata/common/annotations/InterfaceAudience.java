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

package org.apache.carbondata.common.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation is ported and modified from Apache Hadoop project.
 *
 * Annotation to inform users of a package, class or method's intended audience.
 * Currently the audience can be {@link User}, {@link Developer}, {@link Internal}
 *
 * Public classes that are not marked with this annotation must be
 * considered by default as {@link Internal}.</li>
 *
 * External applications must only use classes that are marked {@link User}.
 *
 * Methods may have a different annotation that it is more restrictive
 * compared to the audience classification of the class. Example: A class
 * might be {@link User}, but a method may be {@link Developer}
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class InterfaceAudience {
  /**
   * Intended for use by any project or application.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface User { }

  /**
   * Intended for developers to develop extension for Apache CarbonData project
   * For example, "Index DataMap" to add a new index implementation, etc
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Developer {
    String[] value();
  }

  /**
   * Intended only for internal usage within Apache CarbonData project.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Internal { }

  private InterfaceAudience() { } // Audience can't exist on its own
}
