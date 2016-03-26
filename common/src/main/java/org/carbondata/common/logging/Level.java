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
 * Specifies the logging level.
 */
public enum Level {

    NONE(0),
    DEBUG(1),
    INFO(2),
    ERROR(3),
    AUDIT(4),
    WARN(5);

    /**
     * Constructor.
     *
     * @param level
     */
    Level(final int level) {

    }

    /**
     * Returns an Instance for the specified type.
     *
     * @param name instance type needed.
     * @return {@link Level}
     */
    public static Level getInstance(String name) {
        for (Level logLevel : Level.values()) {
            if (logLevel.name().equalsIgnoreCase(name)) {
                return logLevel;
            }
        }
        return null;
    }
}