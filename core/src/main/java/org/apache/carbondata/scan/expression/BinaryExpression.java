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

package org.apache.carbondata.scan.expression;

public abstract class BinaryExpression extends Expression {

  private static final long serialVersionUID = 1L;
  protected Expression left;
  protected Expression right;

  public BinaryExpression(Expression left, Expression right) {
    this.left = left;
    this.right = right;
    children.add(left);
    children.add(right);
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

}
