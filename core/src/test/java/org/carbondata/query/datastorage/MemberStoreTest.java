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
import org.carbondata.core.constants.CarbonCommonConstants;
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

    /**
     * Test case to test more 10K data
     * @throws Exception
     */
    @Test public void getMemberIdForMoreThan10KValues() throws Exception {
        {
            CarbonDef.Level level = new CarbonDef.Level();
            level.name = "City";
            level.type = DataType.STRING.toString();

            MemberStore memberStore = new MemberStore(level, "");
            int size = 10100;
            int div = size / CarbonCommonConstants.LEVEL_ARRAY_SIZE;
            int rem = size % CarbonCommonConstants.LEVEL_ARRAY_SIZE;
            if (rem > 0) {
                div++;
            }
            Member[][] member = new Member[div][];
            for (int j = 0; j < div; j++) {
                member[j] = new Member[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
            }
            if (rem > 0) {
                member[member.length - 1] = new Member[rem];
            } else {
                member[member.length - 1] = new Member[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
            }
            int prvArrayIndex = 0;
            int current = 0;
            int index = 0;
            while (current < size) {

                String memberStr = "LA" + current;
                member[current / CarbonCommonConstants.LEVEL_ARRAY_SIZE][index] =
                        new Member(memberStr.getBytes());
                current++;
                if (current / CarbonCommonConstants.LEVEL_ARRAY_SIZE > prvArrayIndex) {
                    prvArrayIndex++;
                    index = 0;
                } else {
                    index++;
                }
            }
            //get the surrogate of member which does not exist in the member cache
            int min = 1;
            memberStore.addAll(member, min, size + min, null);
            int surrogateKey = memberStore.getMemberId("AB", false);
            Assert.assertEquals("Expected and actual Surrogate Should be same", 0, surrogateKey);
            //test with members which exit in the members cache
            current = 0;
            while (current < size) {
                surrogateKey = memberStore.getMemberId("LA" + current, false);
                Assert.assertEquals("Expected and actual Surrogate Should be same", current+min,
                        surrogateKey);
                current++;
            }
        }
    }

    /**
     * Test case to test incremental load scenario means when the min value is equal to prev
     * load Max key more 10K data
     * @throws Exception
     */
    @Test public void getMemberIdForIncrementalLoadWith10KValues() throws Exception {
        {
            CarbonDef.Level level = new CarbonDef.Level();
            level.name = "City";
            level.type = DataType.STRING.toString();

            MemberStore memberStore = new MemberStore(level, "");
            int size = 10100;
            int div = size / CarbonCommonConstants.LEVEL_ARRAY_SIZE;
            int rem = size % CarbonCommonConstants.LEVEL_ARRAY_SIZE;
            if (rem > 0) {
                div++;
            }
            Member[][] member = new Member[div][];
            for (int j = 0; j < div; j++) {
                member[j] = new Member[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
            }
            if (rem > 0) {
                member[member.length - 1] = new Member[rem];
            } else {
                member[member.length - 1] = new Member[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
            }
            int prvArrayIndex = 0;
            int current = 0;
            int index = 0;
            while (current < size) {

                String memberStr = "LA" + current;
                member[current / CarbonCommonConstants.LEVEL_ARRAY_SIZE][index] =
                        new Member(memberStr.getBytes());
                current++;
                if (current / CarbonCommonConstants.LEVEL_ARRAY_SIZE > prvArrayIndex) {
                    prvArrayIndex++;
                    index = 0;
                } else {
                    index++;
                }
            }
            //get the surrogate of member which does not exist in the member cache
            int min = 1000;
            memberStore.addAll(member, min, size + min, null);
            int surrogateKey = memberStore.getMemberId("ZB100", false);
            Assert.assertEquals("Expected and actual Surrogate Should be same", 0, surrogateKey);
            //test with members which exit in the members cache
            current = 0;
            while (current < size) {
                surrogateKey = memberStore.getMemberId("LA" + current, false);
                Assert.assertEquals("Expected and actual Surrogate Should be same", current+min,
                        surrogateKey);
                current++;
            }
        }
    }
}
