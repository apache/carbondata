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

package org.apache.carbondata.core.scan.expression.exception;


/**
 * FilterIllegalMemberException class representing exception which can cause while evaluating
 * filter members needs to be gracefully handled without propagating to outer layer so that
 * the execution should not get interrupted.
 */
public class FilterIllegalMemberException extends Exception {

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
  public FilterIllegalMemberException(String msg) {
    super(msg);
    this.msg = msg;
  }

  /**
   * Constructor
   *
   */
  public FilterIllegalMemberException(Throwable t) {
    super(t);
  }

  /**
   * getMessage
   */
  public String getMessage() {
    return this.msg;
  }

}
