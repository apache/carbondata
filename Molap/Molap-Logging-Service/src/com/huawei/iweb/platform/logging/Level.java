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