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

package com.huawei.iweb.platform.logging;

/**
 * for Log Services
 */
public interface LogService
{

    /**
     * Tells if the debug mode of the logger is enabled
     * 
     * @return boolean
     * 
     */
    boolean isDebugEnabled();

    /**
     * Tells if the INFO mode of the logger is enabled
     * 
     * @return boolean
     * 
     */
    boolean isInfoEnabled();

    /**
     * Tells if the WARN mode of the logger is enabled
     * 
     * @return boolean
     * 
     */
    boolean isWarnEnabled();

    /**
     * This method has been deprecated. use method <code>info</code> instead of
     * audit
     * 
     * @param event events
     * @param inserts for inserts
     */
    void debug(LogEvent event , Object... inserts);

    /**
     * for debugs
     * 
     * @param event events
     * @param throwable throwable
     * @param inserts for inserts
     */
    void debug(LogEvent event , Throwable throwable , Object... inserts);

    /**
     * for Errors.
     * 
     * @param event events
     * @param inserts inserts
     */
    void error(LogEvent event , Object... inserts);

    /**
     * for errors and exception.
     * 
     * @param event events
     * @param throwable throwable.
     * @param inserts inserts.
     */
    void error(LogEvent event , Throwable throwable , Object... inserts);

    /**
     * for Info
     * 
     * @param event events.
     * @param inserts inserts.
     */
    void info(LogEvent event , Object... inserts);

    /**
     * for Info
     * 
     * @param event events
     * @param throwable throwable
     * @param inserts inserts
     */
    void info(LogEvent event , Throwable throwable , Object... inserts);

    /**
     * for audit
     * 
     * @param event events
     * @param inserts inserts
     */
    void audit(LogEvent event , Object... inserts);

    /**
     * for audit
     * 
     * @param msg Message
     */
    void audit(String msg);

    /**
     * for warn
     * 
     * @param event events
     * @param inserts inserts
     * 
     */
    void warn(LogEvent event , Object... inserts);

}
