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

package org.apache.carbondata.core.util;

import java.io.InvalidClassException;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import com.attacker.EvilDeserializationPayload;
import org.junit.Assert;
import org.junit.Test;

public class ObjectSerializationUtilSecurityTest {

  @Test
  public void testFilteredDeserializationAllowsFilterResolver() throws Exception {
    FilterResolverIntf resolver = new ConditionalFilterResolverImpl(
        new EqualToExpression(
            new LiteralExpression("a", DataTypes.STRING),
            new LiteralExpression("a", DataTypes.STRING)),
        true, true, false);

    String serialized = ObjectSerializationUtil.convertObjectToString(resolver);
    FilterResolverIntf deserialized = ObjectSerializationUtil.convertStringToObject(
        serialized, FilterResolverIntf.class, "org.apache.carbondata.");

    Assert.assertTrue(deserialized instanceof ConditionalFilterResolverImpl);
  }

  @Test
  public void testFilteredDeserializationRejectsClassBeforeReadObject() throws Exception {
    EvilDeserializationPayload.executed = false;
    String serialized = ObjectSerializationUtil.convertObjectToString(
        new EvilDeserializationPayload());

    try {
      ObjectSerializationUtil.convertStringToObject(
          serialized, FilterResolverIntf.class, "org.apache.carbondata.");
      Assert.fail("Expected the attacker class to be rejected");
    } catch (InvalidClassException expected) {
      Assert.assertFalse(EvilDeserializationPayload.executed);
    }
  }
}
