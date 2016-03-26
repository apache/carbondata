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

package org.carbondata.common.logging.impl;

import java.util.Locale;

import junit.framework.TestCase;
import mockit.Mock;
import mockit.MockUp;
import org.carbondata.common.logging.LogEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ResourceBundleMessageFinderTest_UT extends TestCase {

    /**
     * @throws Exception
     */
    @Before public void setUp() throws Exception {
        new MockUp<ResourceBundleMessageFinder>() {
            @SuppressWarnings("unused") @Mock
            public String findLogEventMessage(Locale locale, LogEvent event) {
                return "CPU Alarm";
            }
        };
    }

    /**
     * @throws Exception
     * @Author k00742797
     * @Description : tearDown
     */
    @After public void tearDown() throws Exception {
    }

    @Test public void testResourceBundleMessageFinder() {
        assertNotNull(new ResourceBundleMessageFinder());
    }

    @Test public void testFindLogEventMessageLogEvent() {
        LogEvent logEvent = new LogEvent() {
            @Override public String getModuleName() {
                return "TEST";
            }

            @Override public String getEventCode() {
                return "TEST";
            }
        };

        String eventMessage = new ResourceBundleMessageFinder().findLogEventMessage(logEvent);
        Assert.assertEquals("CPU Alarm", eventMessage);
    }

    @Test public void testFindLogEventMessageLocaleLogEvent() {
        LogEvent logEvent = new LogEvent() {
            @Override public String getModuleName() {
                return "TEST";
            }

            @Override public String getEventCode() {
                return "TEST";
            }
        };

        String eventMessage = new ResourceBundleMessageFinder()
                .findLogEventMessage(Locale.getDefault(), logEvent);
        Assert.assertEquals("CPU Alarm", eventMessage);
    }

}
