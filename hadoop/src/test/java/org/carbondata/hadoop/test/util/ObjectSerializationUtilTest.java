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

package org.carbondata.hadoop.test.util;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.hadoop.util.ObjectSerializationUtil;
import org.carbondata.scan.expression.ColumnExpression;
import org.carbondata.scan.expression.Expression;
import org.carbondata.scan.expression.LiteralExpression;
import org.carbondata.scan.expression.conditional.EqualToExpression;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ObjectSerializationUtilTest extends TestCase {

  @Before public void setUp() throws Exception {

  }

  @Test public void testConvertObjectToString() throws Exception {
    Expression expression = new EqualToExpression(new ColumnExpression("c1", DataType.STRING),
        new LiteralExpression("a", DataType.STRING));
    String string = ObjectSerializationUtil.convertObjectToString(expression);
    Assert.assertTrue(string != null);
  }

  @Test public void testConvertStringToObject() throws Exception {
    Expression expression = new EqualToExpression(new ColumnExpression("c1", DataType.STRING),
        new LiteralExpression("a", DataType.STRING));
    String string = ObjectSerializationUtil.convertObjectToString(expression);
    Assert.assertTrue(string != null);
    Object object = ObjectSerializationUtil.convertStringToObject(string);
    Assert.assertTrue(object != null);
    Assert.assertTrue(object instanceof Expression);
  }
}
