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
package org.carbondata.query.datastorage;

import org.carbondata.core.carbon.CarbonDef;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for MemberStore
 */
public class MemberStoreTest {

    @Test public void getMemberId() throws Exception {
        {
            CarbonDef.Level level = new CarbonDef.Level();
            level.name = "City";
            level.type = DataType.STRING.toString();

            MemberStore memberStore = new MemberStore(level, "");
            Member[][] member = new Member[1][4];
            member[0][0] = new Member("DM".getBytes());
            member[0][1] = new Member("DL".getBytes());
            member[0][2] = new Member("NY".getBytes());
            member[0][3] = new Member("CA".getBytes());
            memberStore.addAll(member, 1, 4, null);
            int surrogateKey = memberStore.getMemberId("DL", false);
            Assert.assertEquals("Expected and actual Surrogate Should be same", 2, surrogateKey);
            surrogateKey = memberStore.getMemberId("NY", false);
            Assert.assertEquals("Expected and actual Surrogate Should be same", 3, surrogateKey);
            surrogateKey = memberStore.getMemberId("LA", false);
            Assert.assertEquals("Expected and actual Surrogate Should be same", 0, surrogateKey);
            surrogateKey = memberStore.getMemberId("DM", false);
            Assert.assertEquals("Expected and actual Surrogate Should be same", 1, surrogateKey);
        }
    }
}
