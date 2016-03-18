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

/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2011
 * =====================================
 *
 */

package com.huawei.iweb.platform.logging;

/**
 * Specifies the logging level.
 * 
 * @author R72411
 * @version 1.0
 * @created 08-Oct-2008 10:37:40
 * @modified by a00902236
 */
public enum Level {
	/**
	 * NONE.
	 */
	NONE(0),
	/**
	 * DEBUG.
	 */
	DEBUG(1),
	/**
	 * INFO.
	 */
	INFO(2),
	/**
	 * ERROR.
	 */
	ERROR(3),
	/**
	 * SECURE.
	 */
	SECURE(4),
	/**
	 * AUDIT.
	 */
	AUDIT(5),
	/**
	 * WARN.
	 */
	WARN(6);

	/**
	 *Constructor.
	 * 
	 * @param level
	 */
	Level(final int level) {

	}

	/**
	 * Returns an Instance for the specified type.
	 * 
	 * @param name
	 *            instance type needed.
	 * @return {@link Level}
	 */
	public static Level getInstance(String name) {
		for (Level logLevel : Level.values()) {
			if (logLevel.name().equalsIgnoreCase(name))
			{
				return logLevel;
			}
		}
		return null;
	}
}