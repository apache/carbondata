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

package org.apache.carbondata.core.datastore.exception;

import java.io.IOException;
import java.util.Locale;

/**
 * Exception class for block builder
 */
public class IndexBuilderException extends IOException {
  /**
   * default serial version ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The Error message.
   */
  private String msg = "";

  /**
   * Constructor
   *
   * @param msg       The error message for this exception.
   */
  public IndexBuilderException(String msg) {
    super(msg);
    this.msg = msg;
  }

  /**
   * Constructor
   *
   * @param msg       exception message
   * @param throwable detail exception
   */
  public IndexBuilderException(String msg, Throwable throwable) {
    super(msg, throwable);
    this.msg = msg;
  }

  /**
   * Constructor
   *
   * @param throwable exception
   */
  public IndexBuilderException(Throwable throwable) {
    super(throwable);
  }

  /**
   * This method is used to get the localized message.
   *
   * @param locale - A Locale object represents a specific geographical,
   *               political, or cultural region.
   * @return - Localized error message.
   */
  public String getLocalizedMessage(Locale locale) {
    return "";
  }

  /**
   * getMessage
   */
  public String getMessage() {
    return this.msg;
  }
}

