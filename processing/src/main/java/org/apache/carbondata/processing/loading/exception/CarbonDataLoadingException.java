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

package org.apache.carbondata.processing.loading.exception;

public class CarbonDataLoadingException extends RuntimeException {
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
   * @param msg The error message for this exception.
   */
  public CarbonDataLoadingException(String msg) {
    super(msg);
    this.msg = msg;
  }

  /**
   * Constructor
   *
   * @param msg The error message for this exception.
   */
  public CarbonDataLoadingException(String msg, Throwable t) {
    super(msg, t);
    this.msg = msg;
  }

  /**
   * Constructor
   *
   * @param t
   */
  public CarbonDataLoadingException(Throwable t) {
    super(t);
  }

  /**
   * getLocalizedMessage
   */
  @Override
  public String getLocalizedMessage() {
    return super.getLocalizedMessage();
  }

  /**
   * getMessage
   */
  @Override
  public String getMessage() {
    return this.msg;
  }
}
