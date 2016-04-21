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

package org.carbondata.query.filters.likefilters;

import java.io.Serializable;

public class LikeExpression implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 2647557717408470809L;
  private String expressionName;
  private boolean notExpression;

  public boolean isNotExpression() {
    return notExpression;
  }

  public void setNotExpression(boolean notExpression) {
    this.notExpression = notExpression;
  }

  public String getExpressionName() {
    return expressionName;
  }

  public void setExpressionName(String expressionName) {
    this.expressionName = expressionName;
  }

}
